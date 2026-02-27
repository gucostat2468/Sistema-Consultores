from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
import re
import secrets
import tempfile
import unicodedata
import uuid

import pandas as pd
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from src.db import (
    AuthenticatedUser,
    authenticate_user,
    ensure_admin_user,
    finalize_ingestion_batch,
    fetch_customer_import_hints,
    fetch_document_consultant_hints,
    fetch_credit_limits_for_user,
    fetch_receivables_for_user,
    import_credit_limits,
    import_receivables,
    init_db,
    list_ingestion_batches,
    list_consultants,
    register_ingestion_file,
    save_import_format_profile,
    stage_credit_limit_records,
    stage_receivables_records,
    start_ingestion_batch,
)
from src.credit_excel_parser import parse_credit_excel
from src.metrics import (
    build_client_health,
    build_enriched_receivables,
    build_guidance,
    build_portfolio_metrics,
    classify_financial_status,
    safe_ratio,
)
from src.pdf_parser import parse_pdf
from src.receivables_excel_parser import parse_receivables_excel
from src.update_validation import (
    EXPECTED_CONSULTANTS,
    validate_credit_parse_report,
    validate_excel_receivables_parse_report,
    validate_parse_report,
)


DEFAULT_CONSULTANT_PASSWORD = "Consultor@123"
DEFAULT_ADMIN_PASSWORD = "Admin@123"
DEFAULT_REPORT_CONSULTANT = "CARTEIRA GERAL (SEM CONSULTOR)"
TOKEN_STORE: dict[str, AuthenticatedUser] = {}

CUSTOMER_NAME_CORRECTIONS = {
    "AGRODRONE SOLU ES AGRICOLAS LTDA": "AGRODRONE SOLUCOES AGRICOLAS LTDA",
}

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_DIST_CANDIDATES = [
    BASE_DIR / "frontend" / "dist" / "frontend" / "browser",
    BASE_DIR / "frontend" / "dist" / "frontend",
]
FRONTEND_DIST_DIR = next(
    (path for path in FRONTEND_DIST_CANDIDATES if path.exists()),
    FRONTEND_DIST_CANDIDATES[0],
)
FRONTEND_INDEX_FILE = FRONTEND_DIST_DIR / "index.html"


def to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "on", "yes", "sim"}


def cents_to_brl(value: int | float) -> float:
    return round(float(value) / 100.0, 2)


def strip_status(status: str) -> str:
    return (
        status.replace("á", "a")
        .replace("ã", "a")
        .replace("ç", "c")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ê", "e")
    )


def normalize_customer_key(value: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_name.upper().replace("_", " ").split())


def normalize_loose_customer_name_key(value: str | None) -> str:
    base = normalize_customer_key(value)
    reduced = re.sub(r"[^A-Z0-9]+", " ", base)
    return " ".join(reduced.split())


def sanitize_company_name(value: str | None) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""

    cleaned = text.replace("\ufffd", "")
    cleaned = re.sub(r"\bSOLU\?{1,2}ES\b", "SOLUCOES", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\?\s*", " ", cleaned)
    cleaned = " ".join(cleaned.split())

    key = normalize_loose_customer_name_key(cleaned)
    corrected = CUSTOMER_NAME_CORRECTIONS.get(key)
    if corrected:
        return corrected

    return cleaned


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def classify_credit_alert(exposure_cents: int, available_cents: int) -> str:
    if exposure_cents <= 0 and available_cents > 0:
        return "Sem exposicao"
    if exposure_cents > 0 and available_cents <= 0:
        return "Sem limite livre"
    if available_cents <= 0:
        return "Sem dados"

    ratio = safe_ratio(exposure_cents, available_cents)
    if ratio > 1:
        return "Acima do limite"
    if ratio >= 0.8:
        return "Atencao"
    return "Controlado"


def credit_alert_priority(alert: str) -> int:
    order = {
        "Acima do limite": 5,
        "Sem limite livre": 4,
        "Atencao": 3,
        "Controlado": 2,
        "Sem exposicao": 1,
        "Sem dados": 0,
    }
    return order.get(alert, 0)


def build_auth_session(user: AuthenticatedUser) -> dict:
    token = secrets.token_urlsafe(32)
    TOKEN_STORE[token] = user
    return {
        "accessToken": token,
        "user": {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "role": "admin" if user.is_admin else "consultor",
        },
    }


def get_current_user_from_header(authorization: str | None) -> AuthenticatedUser:
    if not authorization:
        raise HTTPException(status_code=401, detail="Token ausente.")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Formato de token invalido.")
    token = parts[1].strip()
    user = TOKEN_STORE.get(token)
    if not user:
        raise HTTPException(status_code=401, detail="Token invalido ou expirado.")
    return user


def rows_to_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def map_financial_status(status: str) -> str:
    if status.startswith("Cr"):
        return "Critico"
    if status.startswith("At"):
        return "Atencao"
    return "Saudavel"


def build_client_health_for_api(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    enriched = build_enriched_receivables(df, today=date.today())
    if "customer_name" in enriched.columns:
        enriched = enriched.copy()
        enriched["customer_name"] = enriched["customer_name"].map(sanitize_company_name)
    records: list[dict] = []

    for keys, group in enriched.groupby(
        ["consultant_id", "consultant_name", "customer_name", "customer_code"], as_index=False
    ):
        consultant_id, consultant_name, customer_name, customer_code = keys
        balances = pd.to_numeric(group["balance_cents"], errors="coerce").fillna(0).astype(float)
        days = pd.to_numeric(group["days_to_due"], errors="coerce").fillna(9_999).astype(float)

        total_balance_cents = int(round(float(balances.sum())))
        titles_count = int(len(group))

        overdue_mask = days < 0
        due_7_mask = (days >= 0) & (days <= 7)
        due_30_mask = (days >= 0) & (days <= 30)
        due_15_mask = (days >= 0) & (days <= 15)

        overdue_1_7_mask = (days >= -7) & (days <= -1)
        overdue_8_30_mask = (days >= -30) & (days <= -8)
        overdue_31_plus_mask = days <= -31

        due_8_15_mask = (days >= 8) & (days <= 15)
        due_16_30_mask = (days >= 16) & (days <= 30)

        overdue_cents = int(round(float(balances[overdue_mask].sum())))
        due_7_cents = int(round(float(balances[due_7_mask].sum())))
        due_30_cents = int(round(float(balances[due_30_mask].sum())))
        due_15_cents = int(round(float(balances[due_15_mask].sum())))

        overdue_1_7_cents = float(balances[overdue_1_7_mask].sum())
        overdue_8_30_cents = float(balances[overdue_8_30_mask].sum())
        overdue_31_plus_cents = float(balances[overdue_31_plus_mask].sum())
        due_8_15_cents = float(balances[due_8_15_mask].sum())
        due_16_30_cents = float(balances[due_16_30_mask].sum())

        overdue_ratio = safe_ratio(overdue_cents, total_balance_cents)
        due_15_ratio = safe_ratio(due_15_cents, total_balance_cents)
        severe_overdue_ratio = safe_ratio(
            int(round(overdue_8_30_cents + overdue_31_plus_cents)),
            total_balance_cents,
        )

        overdue_titles_count = int(overdue_mask.sum())
        due_15_titles_count = int(due_15_mask.sum())
        max_title_cents = float(balances.max()) if not balances.empty else 0.0
        max_title_share = max_title_cents / total_balance_cents if total_balance_cents > 0 else 0.0
        weighted_mean_days = (
            float((balances * days.clip(lower=-45, upper=60)).sum() / total_balance_cents)
            if total_balance_cents > 0
            else 60.0
        )

        overdue_severity = (
            max(
                0.0,
                min(
                    1.0,
                    (
                        overdue_1_7_cents
                        + overdue_8_30_cents * 1.4
                        + overdue_31_plus_cents * 1.9
                    )
                    / total_balance_cents,
                ),
            )
            if total_balance_cents > 0
            else 0.0
        )
        near_pressure = (
            max(
                0.0,
                min(
                    1.0,
                    (
                        due_7_cents
                        + due_8_15_cents * 0.65
                        + due_16_30_cents * 0.35
                    )
                    / total_balance_cents,
                ),
            )
            if total_balance_cents > 0
            else 0.0
        )
        title_stress = (
            max(
                0.0,
                min(
                    1.0,
                    (overdue_titles_count / titles_count) * 0.75
                    + (due_15_titles_count / titles_count) * 0.25,
                ),
            )
            if titles_count > 0
            else 0.0
        )
        concentration_risk = max(0.0, min(1.0, (max_title_share - 0.35) / 0.65))
        trajectory_risk = max(0.0, min(1.0, (12.0 - weighted_mean_days) / 57.0))

        base_penalty = (
            overdue_severity * 60.0
            + near_pressure * 18.0
            + title_stress * 8.0
            + concentration_risk * 8.0
            + trajectory_risk * 6.0
        )
        escalation_penalty = (
            max(0.0, overdue_ratio - 0.35) * 35.0
            + max(0.0, severe_overdue_ratio - 0.2) * 20.0
        )
        risk_score = int(round(max(0.0, min(100.0, 100.0 - base_penalty - escalation_penalty))))

        financial_status = classify_financial_status(
            risk_score=risk_score,
            overdue_ratio=overdue_ratio,
            due_15_ratio=due_15_ratio,
            severe_overdue_ratio=severe_overdue_ratio,
        )
        mapped_status = map_financial_status(financial_status)
        action = {
            "Critico": "Contato imediato e plano de renegociacao antes de novas propostas.",
            "Atencao": "Contato preventivo para vencimentos proximos e revisao de limite.",
            "Saudavel": "Cliente apto para novas propostas com monitoramento padrao.",
        }[mapped_status]

        records.append(
            {
                "consultantId": int(consultant_id),
                "consultantName": consultant_name,
                "customerName": sanitize_company_name(customer_name),
                "customerCode": str(customer_code),
                "totalBalance": cents_to_brl(total_balance_cents),
                "overdue": cents_to_brl(overdue_cents),
                "due7": cents_to_brl(due_7_cents),
                "due30": cents_to_brl(due_30_cents),
                "titles": titles_count,
                "score": risk_score,
                "status": mapped_status,
                "action": action,
            }
        )

    records.sort(key=lambda item: item["totalBalance"], reverse=True)
    return records


def build_summary_for_api(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "totalBalance": 0,
            "due7": 0,
            "due30": 0,
            "overdue": 0,
            "clients": 0,
            "titles": 0,
            "overdueRatio": 0,
            "due7Ratio": 0,
            "guidance": "Sem carteira carregada. Importe PDF/Excel para iniciar.",
        }

    enriched = build_enriched_receivables(df, today=date.today())
    metrics = build_portfolio_metrics(enriched)
    health = build_client_health(enriched)
    guidance = build_guidance(metrics, health)
    overdue_ratio = safe_ratio(metrics["overdue_cents"], metrics["total_balance_cents"])
    due7_ratio = safe_ratio(metrics["due_7_cents"], metrics["total_balance_cents"])

    return {
        "totalBalance": cents_to_brl(metrics["total_balance_cents"]),
        "due7": cents_to_brl(metrics["due_7_cents"]),
        "due30": cents_to_brl(metrics["due_30_cents"]),
        "overdue": cents_to_brl(metrics["overdue_cents"]),
        "clients": int(metrics["clients_count"]),
        "titles": int(metrics["titles_count"]),
        "overdueRatio": float(round(overdue_ratio, 4)),
        "due7Ratio": float(round(due7_ratio, 4)),
        "guidance": strip_status(guidance),
    }


def build_receivables_for_api(rows: list[dict]) -> list[dict]:
    payload: list[dict] = []
    for row in rows:
        customer_name = sanitize_company_name(str(row["customer_name"] or ""))
        payload.append(
            {
                "id": int(row["id"]),
                "consultantId": int(row["consultant_id"]),
                "consultantName": row["consultant_name"],
                "customerName": customer_name,
                "customerCode": str(row["customer_code"] or ""),
                "status": row["status"],
                "documentId": row["document_id"],
                "documentRef": row["document_ref"],
                "installment": row["installment"],
                "issueDate": row["issue_date"],
                "dueDate": row["due_date"],
                "balance": cents_to_brl(row["balance_cents"]),
                "installmentValue": cents_to_brl(row["installment_value_cents"]),
            }
        )
    return payload


def build_credit_limits_for_api(*, credit_rows: list[dict], receivable_rows: list[dict]) -> dict:
    today = date.today()
    exposure_by_customer: dict[tuple[int, str], int] = {}
    overdue_by_customer: dict[tuple[int, str], int] = {}
    due7_by_customer: dict[tuple[int, str], int] = {}
    customer_code_by_customer: dict[tuple[int, str], str] = {}

    for row in receivable_rows:
        consultant_id = int(row["consultant_id"])
        customer_name = sanitize_company_name(str(row["customer_name"] or ""))
        customer_key = normalize_customer_key(customer_name)
        key = (consultant_id, customer_key)

        balance_cents = int(row.get("balance_cents") or 0)
        exposure_by_customer[key] = exposure_by_customer.get(key, 0) + balance_cents
        customer_code = str(row.get("customer_code") or "").strip()
        if customer_code and key not in customer_code_by_customer:
            customer_code_by_customer[key] = customer_code

        due_date = parse_iso_date(row.get("due_date"))
        days_to_due = (due_date - today).days if due_date else None
        is_overdue = "vencido" in str(row.get("status") or "").lower() or (
            days_to_due is not None and days_to_due < 0
        )
        if is_overdue:
            overdue_by_customer[key] = overdue_by_customer.get(key, 0) + balance_cents
        if days_to_due is not None and 0 <= days_to_due <= 7:
            due7_by_customer[key] = due7_by_customer.get(key, 0) + balance_cents

    items: list[dict] = []
    mapped_keys: set[tuple[int, str]] = set()
    total_exposure_cents = 0
    total_overdue_cents = 0
    total_due7_cents = 0

    for row in credit_rows:
        consultant_id = int(row["consultant_id"])
        customer_name = sanitize_company_name(str(row["customer_name"] or ""))
        key = (consultant_id, normalize_customer_key(customer_name))
        mapped_keys.add(key)

        credit_limit_cents = int(row.get("credit_limit_cents") or 0)
        credit_used_cents = int(row.get("credit_used_cents") or 0)
        credit_available_cents = int(row.get("credit_available_cents") or 0)
        exposure_cents = int(exposure_by_customer.get(key, 0))
        overdue_cents = int(overdue_by_customer.get(key, 0))
        due7_cents = int(due7_by_customer.get(key, 0))
        total_exposure_cents += exposure_cents
        total_overdue_cents += overdue_cents
        total_due7_cents += due7_cents

        usage_ratio = safe_ratio(credit_used_cents, credit_limit_cents)
        exposure_to_available_ratio = safe_ratio(exposure_cents, credit_available_cents)
        alert = classify_credit_alert(exposure_cents, credit_available_cents)

        items.append(
            {
                "consultantId": consultant_id,
                "consultantName": str(row["consultant_name"] or ""),
                "customerName": customer_name,
                "customerCode": customer_code_by_customer.get(key),
                "cnpj": row.get("cnpj"),
                "creditLimit": cents_to_brl(credit_limit_cents),
                "creditUsed": cents_to_brl(credit_used_cents),
                "creditAvailable": cents_to_brl(credit_available_cents),
                "exposure": cents_to_brl(exposure_cents),
                "overdue": cents_to_brl(overdue_cents),
                "due7": cents_to_brl(due7_cents),
                "usageRatio": float(round(usage_ratio, 4)),
                "exposureToAvailableRatio": float(round(exposure_to_available_ratio, 4)),
                "alert": alert,
                "updatedAt": row.get("updated_at"),
                "_sortPriority": credit_alert_priority(alert),
                "_sortExposure": exposure_cents,
            }
        )

    items.sort(
        key=lambda item: (
            int(item["_sortPriority"]),
            float(item["exposureToAvailableRatio"]),
            int(item["_sortExposure"]),
        ),
        reverse=True,
    )
    for item in items:
        item.pop("_sortPriority", None)
        item.pop("_sortExposure", None)

    total_limit_cents = sum(int(row.get("credit_limit_cents") or 0) for row in credit_rows)
    total_used_cents = sum(int(row.get("credit_used_cents") or 0) for row in credit_rows)
    total_available_cents = sum(int(row.get("credit_available_cents") or 0) for row in credit_rows)

    all_exposure_cents = sum(exposure_by_customer.values())
    uncovered_exposure_cents = sum(
        exposure
        for key, exposure in exposure_by_customer.items()
        if key not in mapped_keys
    )
    uncovered_customers = len([key for key in exposure_by_customer if key not in mapped_keys])

    status_counts = {
        "acimaLimite": 0,
        "semLimiteLivre": 0,
        "atencao": 0,
        "controlado": 0,
        "semExposicao": 0,
    }
    for item in items:
        alert = item["alert"]
        if alert == "Acima do limite":
            status_counts["acimaLimite"] += 1
        elif alert == "Sem limite livre":
            status_counts["semLimiteLivre"] += 1
        elif alert == "Atencao":
            status_counts["atencao"] += 1
        elif alert == "Controlado":
            status_counts["controlado"] += 1
        elif alert == "Sem exposicao":
            status_counts["semExposicao"] += 1

    summary = {
        "customersWithLimit": len(items),
        "customersWithoutLimit": uncovered_customers,
        "totalLimit": cents_to_brl(total_limit_cents),
        "totalUsed": cents_to_brl(total_used_cents),
        "totalAvailable": cents_to_brl(total_available_cents),
        "totalExposure": cents_to_brl(total_exposure_cents),
        "totalOverdue": cents_to_brl(total_overdue_cents),
        "totalDue7": cents_to_brl(total_due7_cents),
        "uncoveredExposure": cents_to_brl(uncovered_exposure_cents),
        "portfolioExposure": cents_to_brl(all_exposure_cents),
        "usageRatio": float(round(safe_ratio(total_used_cents, total_limit_cents), 4)),
        "exposureToAvailableRatio": float(round(safe_ratio(total_exposure_cents, total_available_cents), 4)),
        "coverageRatio": float(round(safe_ratio(total_available_cents, total_exposure_cents), 4)),
        "statusCounts": status_counts,
    }

    return {
        "summary": summary,
        "items": items,
    }


def parse_upload_temp(upload: UploadFile, suffix: str) -> Path:
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = upload.file.read()
        tmp.write(content)
        return Path(tmp.name)


def resolve_frontend_file(path: str) -> Path | None:
    if not FRONTEND_DIST_DIR.exists():
        return None

    requested = (FRONTEND_DIST_DIR / path).resolve()
    dist_root = FRONTEND_DIST_DIR.resolve()
    try:
        requested.relative_to(dist_root)
    except ValueError:
        return None

    if not requested.is_file():
        return None
    return requested


app = FastAPI(title="DronePro API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "http://127.0.0.1:4200",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def ensure_bootstrap_admin() -> None:
    admins = [item for item in list_consultants() if item["is_admin"]]
    if admins:
        return
    ensure_admin_user(password=DEFAULT_ADMIN_PASSWORD, username="adm")


init_db()
ensure_bootstrap_admin()


@app.get("/api/health")
def health() -> dict:
    return {"ok": True, "service": "dronepro-api"}


@app.post("/api/auth/login")
def login(payload: dict) -> dict:
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", "")).strip()
    user = authenticate_user(username=username, password=password)
    if user is None:
        raise HTTPException(status_code=401, detail="Usuario ou senha invalidos.")
    if user.is_admin and username.lower() == "adm":
        user = AuthenticatedUser(
            id=user.id,
            name=user.name,
            username="adm",
            is_admin=True,
        )
    return build_auth_session(user)


@app.get("/api/consultants")
def consultants(authorization: str | None = Header(default=None)) -> list[dict]:
    user = get_current_user_from_header(authorization)
    available = list_consultants()
    if user.is_admin:
        return [
            {
                "id": int(item["id"]),
                "name": item["name"],
                "username": item["username"],
                "role": "admin" if item["is_admin"] else "consultor",
            }
            for item in available
            if not item["is_admin"]
        ]

    return [
        {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "role": "consultor",
        }
    ]


@app.get("/api/dashboard/receivables")
def dashboard_receivables(
    consultantId: int | None = None,
    authorization: str | None = Header(default=None),
) -> list[dict]:
    user = get_current_user_from_header(authorization)
    rows = fetch_receivables_for_user(user=user, selected_consultant_id=consultantId)
    return build_receivables_for_api(rows)


@app.get("/api/dashboard/summary")
def dashboard_summary(
    consultantId: int | None = None,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    rows = fetch_receivables_for_user(user=user, selected_consultant_id=consultantId)
    df = rows_to_dataframe(rows)
    return build_summary_for_api(df)


@app.get("/api/dashboard/client-health")
def dashboard_client_health(
    consultantId: int | None = None,
    authorization: str | None = Header(default=None),
) -> list[dict]:
    user = get_current_user_from_header(authorization)
    rows = fetch_receivables_for_user(user=user, selected_consultant_id=consultantId)
    df = rows_to_dataframe(rows)
    return build_client_health_for_api(df)


@app.get("/api/dashboard/credit-limits")
def dashboard_credit_limits(
    consultantId: int | None = None,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    credit_rows = fetch_credit_limits_for_user(user=user, selected_consultant_id=consultantId)
    receivable_rows = fetch_receivables_for_user(user=user, selected_consultant_id=consultantId)
    return build_credit_limits_for_api(credit_rows=credit_rows, receivable_rows=receivable_rows)


@app.get("/api/admin/ingestion/history")
def admin_ingestion_history(
    limit: int = 30,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    if not user.is_admin or user.username.lower() != "adm":
        raise HTTPException(status_code=403, detail="Somente o usuario adm pode consultar o historico.")
    return {"items": list_ingestion_batches(limit=limit)}


@app.post("/api/admin/import")
def admin_import(
    mode: str = Form(default="update"),
    strictVendors: str = Form(default="false"),
    appendMode: str = Form(default="false"),
    allowSkippedLines: int = Form(default=0),
    allowSkippedCreditRows: int = Form(default=10),
    inputProfile: str = Form(default="auto"),
    actorUsername: str = Form(default="adm"),
    pdf: UploadFile | None = File(default=None),
    excel: UploadFile | None = File(default=None),
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    if not user.is_admin or user.username.lower() != "adm":
        raise HTTPException(status_code=403, detail="Somente o usuario adm pode importar.")
    if not pdf and not excel:
        raise HTTPException(status_code=400, detail="Selecione ao menos um arquivo.")

    strict = to_bool(strictVendors)
    append_mode = to_bool(appendMode)
    op_mode = "validate" if str(mode).strip().lower() == "validate" else "update"
    input_profile = str(inputProfile or "auto").strip().lower()
    if input_profile not in {"auto", "report_v1"}:
        input_profile = "auto"
    operation_id = f"IMP-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
    processed_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    update_policy_message = ""
    if op_mode == "update" and append_mode:
        update_policy_message = (
            "Regra aplicada: novos clientes/titulos sao adicionados e registros existentes "
            "sao atualizados, sem limpeza da base."
        )

    files: list[dict] = []
    warnings: list[str] = []
    has_errors = False
    validation_errors_count = 0
    audit = {
        "newRecords": 0,
        "updatedRecords": 0,
        "ignoredDuplicates": 0,
        "errors": 0,
        "stagedRecords": 0,
        "stagedReceivables": 0,
        "stagedCreditLimits": 0,
        "newCustomers": 0,
        "newTitles": 0,
        "updatedTitles": 0,
        "newCreditLimits": 0,
        "updatedCreditLimits": 0,
        "onlyUpdates": False,
    }
    batch_id = start_ingestion_batch(
        operation_id=operation_id,
        mode=op_mode,
        actor_username=user.username,
        strict_vendors=strict,
        append_mode=append_mode,
    )
    audit_log: list[str] = [
        f"Protocolo: {operation_id}",
        f"Lote interno: {batch_id}",
        f"Processado em: {processed_at}",
        f"Modo: {op_mode}",
        f"Perfil de entrada: {input_profile}",
        f"Append mode: {'sim' if append_mode else 'nao'}",
        f"Estrito consultores: {'sim' if strict else 'nao'}",
    ]
    if update_policy_message:
        audit_log.append(f"[POLITICA] {update_policy_message}")

    def finalize_response(*, success: bool, mode_value: str, message: str) -> dict:
        if mode_value == "validate":
            status = "validated_ok" if success else "validated_error"
        else:
            status = "completed" if success else "failed"

        finalize_ingestion_batch(
            batch_id=batch_id,
            status=status,
            message=message,
            audit=audit,
            warnings=warnings,
        )
        return {
            "success": success,
            "mode": mode_value,
            "message": message,
            "warnings": warnings,
            "files": files,
            "audit": audit,
            "auditLog": audit_log,
            "operationId": operation_id,
            "processedAt": processed_at,
            "ingestionBatchId": batch_id,
        }

    pdf_report = None
    credit_report = None
    excel_receivables_report = None

    if actorUsername.strip().lower() != "adm":
        warnings.append("Actor informado diferente de adm; autenticacao em token foi usada.")
    audit_log.append(
        f"Arquivos recebidos: PDF={'sim' if pdf else 'nao'} | Excel={'sim' if excel else 'nao'}"
    )

    if pdf:
        temp_pdf = parse_upload_temp(pdf, ".pdf")
        try:
            pdf_report = parse_pdf(temp_pdf)
            pdf_validation = validate_parse_report(
                pdf_report,
                expected_vendor_names=EXPECTED_CONSULTANTS if strict else None,
                strict_vendor_match=strict,
                max_skipped_lines=int(allowSkippedLines),
            )
            if pdf_validation.errors:
                has_errors = True
            validation_errors_count += len(pdf_validation.errors)
            warnings.extend([f"[PDF:{item.code}] {item.message}" for item in pdf_validation.warnings])

            stats = pdf_validation.stats
            pdf_file_id = register_ingestion_file(
                batch_id=batch_id,
                file_name=pdf.filename or "upload.pdf",
                file_type="pdf",
                source_kind="pdf_receivables",
                record_count=len(pdf_report.records),
                meta={
                    "pages": int(stats["pages_count"]),
                    "candidateLines": int(stats["candidate_lines_count"]),
                    "parsedLines": int(stats["records_count"]),
                },
            )
            staged_rows = stage_receivables_records(
                batch_id=batch_id,
                source_kind="pdf_receivables",
                source_file=pdf.filename or "upload.pdf",
                records=pdf_report.records,
                file_id=pdf_file_id,
            )
            audit["stagedRecords"] += staged_rows
            audit["stagedReceivables"] += staged_rows
            files.append(
                {
                    "fileName": pdf.filename,
                    "fileType": "pdf",
                    "status": "processed",
                    "details": (
                        f"Linhas interpretadas {stats['records_count']}/{stats['candidate_lines_count']} "
                        f"| nao interpretadas {stats['skipped_lines_count']} | consultores {stats['vendors_count']}"
                    ),
                    "summaryLines": [
                        f"Paginas: {stats['pages_count']}",
                        f"Parse ratio: {stats['parse_success_ratio'] * 100:.1f}%",
                    ],
                }
            )
            audit_log.extend(
                [
                    f"[PDF] arquivo: {pdf.filename}",
                    f"[PDF] staging interno: {staged_rows} titulos padronizados",
                    (
                        f"[PDF] lidas {stats['records_count']}/{stats['candidate_lines_count']} "
                        f"| ignoradas {stats['skipped_lines_count']} | consultores {stats['vendors_count']}"
                    ),
                    f"[PDF] erros validacao: {len(pdf_validation.errors)} | avisos: {len(pdf_validation.warnings)}",
                ]
            )
            warnings.extend([f"[PDF:{item.code}] {item.message}" for item in pdf_validation.errors])
            save_import_format_profile(
                source_file=pdf.filename or "upload.pdf",
                source_kind="pdf_receivables",
                sheet_name=None,
                header_signature="pdf_row_regex_v1",
                detected_columns=[
                    "vendor_name",
                    "customer_name",
                    "customer_code",
                    "status",
                    "document_id",
                    "document_ref",
                    "installment",
                    "issue_date",
                    "due_date",
                    "balance",
                ],
                records_detected=len(pdf_report.records),
            )
        finally:
            temp_pdf.unlink(missing_ok=True)

    if excel:
        suffix = Path(excel.filename or "").suffix.lower() or ".xlsx"
        if suffix not in {".xlsx", ".xlsm", ".xls", ".csv"}:
            suffix = ".xlsx"
        temp_excel = parse_upload_temp(excel, suffix)
        try:
            customer_hints = fetch_customer_import_hints()
            document_hints = fetch_document_consultant_hints()
            credit_report = parse_credit_excel(temp_excel, customer_hints=customer_hints)
            augmented_hints = {
                key: {
                    "consultantName": value.get("consultantName"),
                    "customerCode": value.get("customerCode"),
                }
                for key, value in customer_hints.items()
            }
            for record in credit_report.records:
                key = normalize_customer_key(record.customer_name)
                hint = augmented_hints.get(key, {})
                consultant_name = record.consultant_name
                customer_code = hint.get("customerCode")
                augmented_hints[key] = {
                    "consultantName": consultant_name,
                    "customerCode": customer_code,
                }
            excel_receivables_report = parse_receivables_excel(
                temp_excel,
                customer_hints=augmented_hints,
                document_hints=document_hints,
                default_consultant_name=(
                    DEFAULT_REPORT_CONSULTANT if input_profile == "report_v1" else None
                ),
            )
            if input_profile == "report_v1":
                expected_columns = {"dueDate", "documentRef", "customerName", "installmentValue"}
                profile_ok = any(
                    (
                        summary.receivable_section_detected
                        and expected_columns.issubset(set(summary.detected_columns))
                    )
                    for summary in excel_receivables_report.sheet_summaries
                )
                if not profile_ok:
                    has_errors = True
                    validation_errors_count += 1
                    warnings.append(
                        "[EXCEL:REPORT_PROFILE_MISMATCH] "
                        "Formato invalido para perfil report_v1. Esperadas colunas: "
                        "vencimento, NF, Cliente e valor parcela."
                    )
            credit_validation = validate_credit_parse_report(
                credit_report,
                expected_vendor_names=EXPECTED_CONSULTANTS if strict else None,
                strict_vendor_match=strict,
                max_skipped_rows=int(allowSkippedCreditRows),
            )
            receivable_validation = validate_excel_receivables_parse_report(
                excel_receivables_report,
                expected_vendor_names=EXPECTED_CONSULTANTS if strict else None,
                strict_vendor_match=strict,
                max_skipped_rows=int(allowSkippedLines),
            )

            credit_records_count = len(credit_report.records)
            receivable_records_count = len(excel_receivables_report.records)
            has_credit_format = len(credit_report.processed_sheets) > 0
            has_receivable_format = len(excel_receivables_report.processed_sheets) > 0

            should_validate_credit = credit_records_count > 0 or (has_credit_format and not has_receivable_format)
            should_validate_receivables = (
                receivable_records_count > 0 or (has_receivable_format and not has_credit_format)
            )

            if should_validate_credit:
                non_fatal_credit_codes = {
                    "CREDIT_SKIPPED_ROWS",
                    "LOW_CREDIT_PARSE_RATIO",
                    "MISSING_CREDIT_VENDORS",
                    "EXTRA_CREDIT_VENDORS",
                }
                if strict:
                    non_fatal_credit_codes.discard("MISSING_CREDIT_VENDORS")
                fatal_credit_errors = [
                    item for item in credit_validation.errors if item.code not in non_fatal_credit_codes
                ]
                downgraded_credit_errors = [
                    item for item in credit_validation.errors if item.code in non_fatal_credit_codes
                ]
                if fatal_credit_errors:
                    has_errors = True
                validation_errors_count += len(fatal_credit_errors)
                warnings.extend(
                    [f"[EXCEL:CREDIT:{item.code}] {item.message}" for item in credit_validation.warnings]
                )
                warnings.extend(
                    [f"[EXCEL:CREDIT:{item.code}] {item.message}" for item in downgraded_credit_errors]
                )
                warnings.extend(
                    [f"[EXCEL:CREDIT:{item.code}] {item.message}" for item in fatal_credit_errors]
                )

            if should_validate_receivables:
                non_fatal_receivable_codes = {
                    "EXCEL_RECEIVABLE_SKIPPED_ROWS",
                    "LOW_EXCEL_RECEIVABLE_PARSE_RATIO",
                    "NO_EXCEL_RECEIVABLE_VENDORS",
                    "MISSING_EXCEL_RECEIVABLE_VENDORS",
                    "EXTRA_EXCEL_RECEIVABLE_VENDORS",
                }
                if strict:
                    non_fatal_receivable_codes.discard("MISSING_EXCEL_RECEIVABLE_VENDORS")
                fatal_receivable_errors = [
                    item
                    for item in receivable_validation.errors
                    if item.code not in non_fatal_receivable_codes
                ]
                downgraded_receivable_errors = [
                    item
                    for item in receivable_validation.errors
                    if item.code in non_fatal_receivable_codes
                ]
                if fatal_receivable_errors:
                    has_errors = True
                validation_errors_count += len(fatal_receivable_errors)
                warnings.extend(
                    [f"[EXCEL:RECEIVABLE:{item.code}] {item.message}" for item in receivable_validation.warnings]
                )
                warnings.extend(
                    [f"[EXCEL:RECEIVABLE:{item.code}] {item.message}" for item in downgraded_receivable_errors]
                )
                warnings.extend(
                    [f"[EXCEL:RECEIVABLE:{item.code}] {item.message}" for item in fatal_receivable_errors]
                )

            if not credit_records_count and not receivable_records_count:
                has_errors = True
                validation_errors_count += 1
                warnings.append(
                    "[EXCEL:UNRECOGNIZED_FORMAT] Nenhum formato válido de limite de crédito ou "
                    "movimentações em aberto foi reconhecido."
                )

            if has_receivable_format and not receivable_records_count:
                warnings.append(
                    "[EXCEL:RECEIVABLE:NO_RECORDS] Estrutura de movimentações encontrada, mas sem títulos válidos."
                )
            if has_credit_format and not credit_records_count:
                warnings.append(
                    "[EXCEL:CREDIT:NO_RECORDS] Estrutura de limite encontrada, mas sem registros válidos."
                )

            credit_stats = credit_validation.stats
            receivable_stats = receivable_validation.stats
            excel_file_id = register_ingestion_file(
                batch_id=batch_id,
                file_name=excel.filename or "upload.xlsx",
                file_type="excel",
                source_kind="excel_mixed",
                record_count=credit_records_count + receivable_records_count,
                meta={
                    "creditRecords": int(credit_records_count),
                    "receivableRecords": int(receivable_records_count),
                    "scannedSheets": int(credit_stats["scanned_sheets_count"]),
                    "workbookSheets": int(credit_stats["workbook_sheets_count"]),
                },
            )
            staged_credit_rows = stage_credit_limit_records(
                batch_id=batch_id,
                source_kind="excel_credit",
                source_file=excel.filename or "upload.xlsx",
                records=credit_report.records,
                file_id=excel_file_id,
            )
            staged_receivable_rows = stage_receivables_records(
                batch_id=batch_id,
                source_kind="excel_receivables",
                source_file=excel.filename or "upload.xlsx",
                records=excel_receivables_report.records,
                file_id=excel_file_id,
            )
            audit["stagedRecords"] += staged_credit_rows + staged_receivable_rows
            audit["stagedCreditLimits"] += staged_credit_rows
            audit["stagedReceivables"] += staged_receivable_rows
            files.append(
                {
                    "fileName": excel.filename,
                    "fileType": "excel",
                    "status": "processed",
                    "details": (
                        f"Credito {credit_stats['records_count']} | Titulos {receivable_stats['records_count']} | "
                        f"abas lidas {credit_stats['scanned_sheets_count']}/{credit_stats['workbook_sheets_count']}"
                    ),
                    "summaryLines": [
                        f"Perfil de entrada: {input_profile}",
                        (
                            f"Fallback de consultor: {DEFAULT_REPORT_CONSULTANT}"
                            if input_profile == "report_v1"
                            else "Fallback de consultor: desativado"
                        ),
                        f"Aba referencia credito: {credit_stats['sheet_name']}",
                        f"Aba referencia titulos: {receivable_stats['sheet_name']}",
                        (
                            f"Credito -> linhas lidas {credit_stats['rows_scanned']} | "
                            f"candidatas {credit_stats['candidate_rows']} | "
                            f"abas com credito {credit_stats['processed_sheets_count']}"
                        ),
                        (
                            f"Titulos -> linhas lidas {receivable_stats['rows_scanned']} | "
                            f"candidatas {receivable_stats['candidate_rows']} | "
                            f"abas com titulos {receivable_stats['processed_sheets_count']}"
                        ),
                    ],
                }
            )
            audit_log.extend(
                [
                    f"[EXCEL] arquivo: {excel.filename}",
                    (
                        f"[EXCEL] staging interno: creditos={staged_credit_rows} "
                        f"| titulos={staged_receivable_rows}"
                    ),
                    (
                        f"[EXCEL] credito={credit_records_count} | titulos={receivable_records_count} | "
                        f"abas lidas {credit_stats['scanned_sheets_count']}/{credit_stats['workbook_sheets_count']}"
                    ),
                    (
                        f"[EXCEL] erros credito={len(credit_validation.errors)} "
                        f"| erros titulos={len(receivable_validation.errors)}"
                    ),
                ]
            )

            for summary in credit_report.sheet_summaries:
                save_import_format_profile(
                    source_file=excel.filename or "upload.xlsx",
                    source_kind="excel_credit",
                    sheet_name=summary.sheet_name,
                    header_signature="subdealer/cnpj" if summary.credit_section_detected else None,
                    detected_columns=["subdealer_cliente", "cnpj", "limite_credito"],
                    records_detected=summary.records_count,
                )
            for summary in excel_receivables_report.sheet_summaries:
                save_import_format_profile(
                    source_file=excel.filename or "upload.xlsx",
                    source_kind="excel_receivables",
                    sheet_name=summary.sheet_name,
                    header_signature=summary.header_signature,
                    detected_columns=summary.detected_columns,
                    records_detected=summary.records_count,
                )
            audit_log.append(
                "[EXCEL] perfis de formato registrados: "
                f"credito={len(credit_report.sheet_summaries)} | "
                f"titulos={len(excel_receivables_report.sheet_summaries)}"
            )
        finally:
            temp_excel.unlink(missing_ok=True)

    if op_mode == "validate":
        audit["errors"] = validation_errors_count
        audit["onlyUpdates"] = (
            audit["newRecords"] == 0
            and audit["updatedRecords"] > 0
        )
        audit_log.append(
            f"[AUDITORIA] novos={audit['newRecords']} | atualizados={audit['updatedRecords']} | "
            f"ignorados_duplicidade={audit['ignoredDuplicates']} | erros={audit['errors']}"
        )
        audit_log.append(
            "[AUDITORIA:DETALHE] "
            f"clientes_novos={audit['newCustomers']} | "
            f"titulos_novos={audit['newTitles']} | "
            f"titulos_atualizados={audit['updatedTitles']} | "
            f"limites_novos={audit['newCreditLimits']} | "
            f"limites_atualizados={audit['updatedCreditLimits']}"
        )
        return finalize_response(
            success=not has_errors,
            mode_value="validate",
            message="Validacao concluida com sucesso." if not has_errors else "Validacao concluida com erros.",
        )

    if has_errors:
        audit["errors"] = validation_errors_count
        audit["onlyUpdates"] = (
            audit["newRecords"] == 0
            and audit["updatedRecords"] > 0
        )
        audit_log.append(
            f"[AUDITORIA] novos={audit['newRecords']} | atualizados={audit['updatedRecords']} | "
            f"ignorados_duplicidade={audit['ignoredDuplicates']} | erros={audit['errors']}"
        )
        audit_log.append(
            "[AUDITORIA:DETALHE] "
            f"clientes_novos={audit['newCustomers']} | "
            f"titulos_novos={audit['newTitles']} | "
            f"titulos_atualizados={audit['updatedTitles']} | "
            f"limites_novos={audit['newCreditLimits']} | "
            f"limites_atualizados={audit['updatedCreditLimits']}"
        )
        return finalize_response(
            success=False,
            mode_value="update",
            message="Atualizacao cancelada: a validacao encontrou erros.",
        )

    receivables_imported_once = False

    if pdf_report and pdf_report.records:
        summary = import_receivables(
            pdf_report.records,
            source_file=pdf.filename or "upload.pdf",
            default_password=DEFAULT_CONSULTANT_PASSWORD,
            wipe_existing=not append_mode,
        )
        receivables_imported_once = True
        ignored_by_duplicate = summary.duplicated_rows + summary.deduplicated_rows + summary.skipped_rows
        audit["newRecords"] += summary.imported_rows
        audit["updatedRecords"] += summary.updated_rows
        audit["ignoredDuplicates"] += ignored_by_duplicate
        audit["newCustomers"] += summary.new_customers
        audit["newTitles"] += summary.imported_rows
        audit["updatedTitles"] += summary.updated_rows
        for item in files:
            if item["fileType"] == "pdf":
                item.setdefault("summaryLines", []).extend(
                    [
                        f"Clientes novos: {summary.new_customers}",
                        f"Inseridos: {summary.imported_rows}",
                        f"Atualizados: {summary.updated_rows}",
                        f"Sem mudanca: {summary.duplicated_rows}",
                        f"Deduplicados: {summary.deduplicated_rows}",
                        f"Ignorados por snapshot antigo: {summary.skipped_rows}",
                    ]
                )
                break
        audit_log.append(
            "[PDF] update -> "
            f"clientes_novos={summary.new_customers} | "
            f"inseridos={summary.imported_rows} | atualizados={summary.updated_rows} | "
            f"ignorados_duplicidade={ignored_by_duplicate}"
        )

    if excel_receivables_report and excel_receivables_report.records:
        summary = import_receivables(
            excel_receivables_report.records,
            source_file=excel.filename or "upload.xlsx",
            default_password=DEFAULT_CONSULTANT_PASSWORD,
            wipe_existing=not append_mode and not receivables_imported_once,
        )
        receivables_imported_once = True
        ignored_by_duplicate = summary.duplicated_rows + summary.deduplicated_rows + summary.skipped_rows
        audit["newRecords"] += summary.imported_rows
        audit["updatedRecords"] += summary.updated_rows
        audit["ignoredDuplicates"] += ignored_by_duplicate
        audit["newCustomers"] += summary.new_customers
        audit["newTitles"] += summary.imported_rows
        audit["updatedTitles"] += summary.updated_rows
        for item in files:
            if item["fileType"] == "excel":
                item.setdefault("summaryLines", []).extend(
                    [
                        f"Clientes novos em titulos: {summary.new_customers}",
                        f"Titulos inseridos: {summary.imported_rows}",
                        f"Titulos atualizados: {summary.updated_rows}",
                        f"Titulos sem mudanca: {summary.duplicated_rows}",
                        f"Titulos deduplicados: {summary.deduplicated_rows}",
                        f"Titulos ignorados por snapshot antigo: {summary.skipped_rows}",
                    ]
                )
                break
        audit_log.append(
            "[EXCEL:RECEIVABLE] update -> "
            f"clientes_novos={summary.new_customers} | "
            f"inseridos={summary.imported_rows} | atualizados={summary.updated_rows} | "
            f"ignorados_duplicidade={ignored_by_duplicate}"
        )

    if credit_report and credit_report.records:
        summary = import_credit_limits(
            credit_report.records,
            source_file=excel.filename or "upload.xlsx",
            wipe_existing=not append_mode,
        )
        audit["newRecords"] += summary.imported_rows
        audit["updatedRecords"] += summary.updated_rows
        audit["newCreditLimits"] += summary.imported_rows
        audit["updatedCreditLimits"] += summary.updated_rows
        for item in files:
            if item["fileType"] == "excel":
                item.setdefault("summaryLines", []).extend(
                    [
                        f"Inseridos: {summary.imported_rows}",
                        f"Atualizados: {summary.updated_rows}",
                        f"Ignorados: {summary.skipped_rows}",
                    ]
                )
                break
        audit_log.append(
            "[EXCEL] update -> "
            f"inseridos={summary.imported_rows} | atualizados={summary.updated_rows} | "
            f"ignorados={summary.skipped_rows}"
        )
        if summary.unresolved_consultants:
            unresolved = ", ".join(
                f"{name} ({count})" for name, count in sorted(summary.unresolved_consultants.items())
            )
            warnings.append(f"[EXCEL:UNRESOLVED_CONSULTANTS] {unresolved}")
            validation_errors_count += sum(summary.unresolved_consultants.values())
            audit_log.append(f"[EXCEL] consultores nao resolvidos: {unresolved}")

    audit["errors"] = validation_errors_count
    audit["onlyUpdates"] = (
        audit["newRecords"] == 0
        and audit["updatedRecords"] > 0
    )
    audit_log.append(
        f"[AUDITORIA] novos={audit['newRecords']} | atualizados={audit['updatedRecords']} | "
        f"ignorados_duplicidade={audit['ignoredDuplicates']} | erros={audit['errors']}"
    )
    audit_log.append(
        "[AUDITORIA:DETALHE] "
        f"clientes_novos={audit['newCustomers']} | "
        f"titulos_novos={audit['newTitles']} | "
        f"titulos_atualizados={audit['updatedTitles']} | "
        f"limites_novos={audit['newCreditLimits']} | "
        f"limites_atualizados={audit['updatedCreditLimits']}"
    )
    return finalize_response(
        success=True,
        mode_value="update",
        message=(
            "Atualizacao concluida com sucesso."
            + (f" {update_policy_message}" if update_policy_message else "")
        ),
    )


@app.post("/api/admin/import-report-v1")
def admin_import_report_v1(
    excel: UploadFile | None = File(default=None),
    actorUsername: str = Form(default="adm"),
    authorization: str | None = Header(default=None),
) -> dict:
    if not excel:
        raise HTTPException(status_code=400, detail="Selecione o arquivo report.")

    return admin_import(
        mode="update",
        strictVendors="false",
        appendMode="true",
        allowSkippedLines=0,
        allowSkippedCreditRows=0,
        inputProfile="report_v1",
        actorUsername=actorUsername,
        pdf=None,
        excel=excel,
        authorization=authorization,
    )


@app.get("/", include_in_schema=False, response_model=None)
def frontend_root():
    if FRONTEND_INDEX_FILE.is_file():
        return FileResponse(FRONTEND_INDEX_FILE)
    return JSONResponse({"ok": True, "service": "dronepro-api", "frontend": "not-built"})


@app.get("/{full_path:path}", include_in_schema=False, response_model=None)
def frontend_spa_or_asset(full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Rota nao encontrada.")

    static_file = resolve_frontend_file(full_path)
    if static_file is not None:
        return FileResponse(static_file)

    if FRONTEND_INDEX_FILE.is_file():
        return FileResponse(FRONTEND_INDEX_FILE)

    raise HTTPException(status_code=404, detail="Rota nao encontrada.")
