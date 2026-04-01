from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
import base64
import hashlib
import json
import os
import re
import sqlite3
import smtplib
import ssl
import tempfile
import time
import unicodedata
import uuid

import pandas as pd
import jwt
from pypdf import PdfReader, PdfWriter
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from src.db import (
    AuthenticatedUser,
    authenticate_user,
    ensure_admin_user,
    finalize_ingestion_batch,
    fetch_consultant_customers_for_user,
    fetch_customer_import_hints,
    fetch_document_consultant_hints,
    fetch_credit_limits_for_user,
    fetch_receivables_for_user,
    get_connection,
    get_consultant_by_id,
    import_credit_limits,
    import_receivables,
    init_db,
    list_active_admin_users,
    list_ingestion_batches,
    list_consultants,
    register_ingestion_file,
    reset_operational_data,
    save_import_format_profile,
    stage_credit_limit_records,
    stage_receivables_records,
    start_ingestion_batch,
    upsert_consultant_customer,
    update_consultant_email,
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
from src.auth import hash_password
from src.update_validation import (
    EXPECTED_CONSULTANTS,
    validate_credit_parse_report,
    validate_excel_receivables_parse_report,
    validate_parse_report,
    validate_report_v1_workbook_layout,
)


DEFAULT_CONSULTANT_PASSWORD = "Consultor@123"
DEFAULT_ADMIN_PASSWORD = "Admin@123"
DEFAULT_ORDER_ADMIN_PASSWORD = os.getenv("ORDER_ADMIN_PASSWORD", "drone2026")
DEFAULT_ISABEL_PASSWORD = os.getenv("ISABEL_PASSWORD", DEFAULT_ORDER_ADMIN_PASSWORD)
DEFAULT_MARCOS_PASSWORD = os.getenv("MARCOS_PASSWORD", DEFAULT_ORDER_ADMIN_PASSWORD)
VITOR_FINANCIAL_USERNAME = os.getenv("VITOR_FINANCIAL_USERNAME", "vitor_financeiro").strip().lower()
DEFAULT_VITOR_FINANCIAL_PASSWORD = os.getenv("VITOR_FINANCIAL_PASSWORD", "dronepro2026")
DEFAULT_REPORT_CONSULTANT = "CARTEIRA GERAL (SEM CONSULTOR)"
TOKEN_STORE: dict[str, AuthenticatedUser] = {}
AUTH_JWT_ALGORITHM = "HS256"
AUTH_JWT_SECRET = os.getenv("AUTH_JWT_SECRET", "trocar-token-em-producao")
AUTH_TOKEN_TTL_SECONDS = max(300, int(os.getenv("AUTH_TOKEN_TTL_SECONDS", str(12 * 60 * 60))))

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

ORDER_STORAGE_DIR = BASE_DIR / "data" / "order_approval"
ORDER_ORIGINAL_DIR = ORDER_STORAGE_DIR / "original"
ORDER_SIGNED_DIR = ORDER_STORAGE_DIR / "signed"
ORDER_SIGNATURE_DIR = ORDER_STORAGE_DIR / "signatures"
ORDER_ANALYSIS_DIR = ORDER_STORAGE_DIR / "analysis"
ORDER_PACKAGE_DIR = ORDER_STORAGE_DIR / "packages"
REPORT_BACKUP_DIR = BASE_DIR / "data" / "backups" / "report_updates"
for _path in [
    ORDER_STORAGE_DIR,
    ORDER_ORIGINAL_DIR,
    ORDER_SIGNED_DIR,
    ORDER_SIGNATURE_DIR,
    ORDER_ANALYSIS_DIR,
    ORDER_PACKAGE_DIR,
    REPORT_BACKUP_DIR,
]:
    _path.mkdir(parents=True, exist_ok=True)

ORDER_SIGNATURE_MODE = os.getenv("ORDER_SIGNATURE_MODE", "canvas").strip().lower()
if ORDER_SIGNATURE_MODE not in {"canvas", "hash"}:
    ORDER_SIGNATURE_MODE = "canvas"
ORDER_SIGNATURE_SECRET = os.getenv("ORDER_SIGNATURE_SECRET", "trocar-assinatura-em-producao")
ORDER_MAX_PDF_BYTES = int(os.getenv("ORDER_MAX_PDF_BYTES", str(10 * 1024 * 1024)))
ORDER_APPROVAL_PANEL_URL = os.getenv(
    "ORDER_APPROVAL_PANEL_URL",
    "http://localhost:4200/app/status",
)
ISABEL_USERNAME = os.getenv("ISABEL_USERNAME", "Isabel_Dronepro").strip().lower()
MARCOS_USERNAME = os.getenv("MARCOS_USERNAME", "Marcos_Dronepro").strip().lower()
DEFAULT_OPERATIONAL_USERNAMES = {
    "isabel",
    "isabel_dronepro",
    "marcos",
    "marcos_dronepro",
}
EXTRA_OPERATIONAL_USERNAMES = {
    str(item or "").strip().lower()
    for item in os.getenv("OPERATIONAL_USERNAMES", "").split(",")
    if str(item or "").strip()
}
OPERATIONAL_USERNAMES = {
    username
    for username in (
        DEFAULT_OPERATIONAL_USERNAMES
        | EXTRA_OPERATIONAL_USERNAMES
        | {ISABEL_USERNAME, MARCOS_USERNAME}
    )
    if username
}

DEFAULT_ISABEL_USERNAMES = {
    "isabel",
    "isabel_dronepro",
}
EXTRA_ISABEL_USERNAMES = {
    str(item or "").strip().lower()
    for item in os.getenv("ISABEL_USERNAMES", "").split(",")
    if str(item or "").strip()
}
ISABEL_USERNAMES = {
    username
    for username in (
        DEFAULT_ISABEL_USERNAMES
        | EXTRA_ISABEL_USERNAMES
        | {ISABEL_USERNAME}
    )
    if username
}

DEFAULT_COMMERCIAL_DIRECTOR_USERNAMES = {
    "marcos",
    "marcos_dronepro",
}
EXTRA_COMMERCIAL_DIRECTOR_USERNAMES = {
    str(item or "").strip().lower()
    for item in os.getenv("COMMERCIAL_DIRECTOR_USERNAMES", "").split(",")
    if str(item or "").strip()
}
COMMERCIAL_DIRECTOR_USERNAMES = {
    username
    for username in (
        DEFAULT_COMMERCIAL_DIRECTOR_USERNAMES
        | EXTRA_COMMERCIAL_DIRECTOR_USERNAMES
        | {MARCOS_USERNAME}
    )
    if username
}

DEFAULT_FINANCIAL_USERNAMES = {
    "vitor_financeiro",
}
EXTRA_FINANCIAL_USERNAMES = {
    str(item or "").strip().lower()
    for item in os.getenv("FINANCIAL_USERNAMES", "").split(",")
    if str(item or "").strip()
}
FINANCIAL_USERNAMES = {
    username
    for username in (
        DEFAULT_FINANCIAL_USERNAMES
        | EXTRA_FINANCIAL_USERNAMES
        | {VITOR_FINANCIAL_USERNAME}
    )
    if username
}

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip() or None
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip() or None
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on", "sim"}
SMTP_TLS_VERIFY = os.getenv("SMTP_TLS_VERIFY", "true").strip().lower() in {"1", "true", "yes", "on", "sim"}
SMTP_TLS_ALLOW_INSECURE_FALLBACK = os.getenv("SMTP_TLS_ALLOW_INSECURE_FALLBACK", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
    "sim",
}
SMTP_MAX_RETRIES = max(1, int(os.getenv("SMTP_MAX_RETRIES", "3")))
SMTP_RETRY_BACKOFF_SECONDS = max(0.0, float(os.getenv("SMTP_RETRY_BACKOFF_SECONDS", "1.2")))
SMTP_FALLBACK_HOSTS = [
    host.strip()
    for host in os.getenv("SMTP_FALLBACK_HOSTS", "").split(",")
    if host.strip()
]
SMTP_FROM = os.getenv("SMTP_FROM", "no-reply@dronepro.local").strip()

ORDER_STATUS_LEGACY_AWAITING_FINANCIAL_SIGNATURE = "AGUARDANDO_ASSINATURA_DIRETOR_FINANCEIRO"
ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE = "AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL"
ORDER_STATUS_AWAITING_SIGNATURE = "AGUARDANDO_ASSINATURA_ISABEL"
ORDER_STATUS_NEGATIVE = "NEGADO_SEM_LIMITE"
ORDER_STATUS_RETURNED = "DEVOLVIDO_REVISAO"
ORDER_STATUS_SIGNED_DISTRIBUTING = "ASSINADO_AGUARDANDO_DISTRIBUICAO"
ORDER_STATUS_DONE = "CONCLUIDO"
ORDER_STATUS_BILLED = "FATURADO"
ORDER_STATUS_DELETED = "EXCLUIDO"
ORDER_STATUS_ALL = {
    ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE,
    ORDER_STATUS_AWAITING_SIGNATURE,
    ORDER_STATUS_NEGATIVE,
    ORDER_STATUS_RETURNED,
    ORDER_STATUS_SIGNED_DISTRIBUTING,
    ORDER_STATUS_DONE,
    ORDER_STATUS_BILLED,
    ORDER_STATUS_DELETED,
}

EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "on", "yes", "sim"}


def cents_to_brl(value: int | float) -> float:
    return round(float(value) / 100.0, 2)


def format_brl_from_cents(value_cents: int | float) -> str:
    value = cents_to_brl(value_cents)
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


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


GENERIC_CUSTOMER_LOOKUP_KEYS = {
    "ENDERECO",
    "ENDERECO DO CLIENTE",
    "CLIENTE",
    "NOME",
    "NOME DO CLIENTE",
    "RAZAO SOCIAL",
    "RAZAO SOCIAL DO CLIENTE",
}


def is_generic_customer_lookup_name(value: str | None) -> bool:
    key = normalize_loose_customer_name_key(value)
    if not key:
        return True
    if key in GENERIC_CUSTOMER_LOOKUP_KEYS:
        return True
    # Captura variações comuns vindas de extração ruim de PDF.
    if key.startswith("ENDERECO ") or key.endswith(" ENDERECO"):
        return True
    return False


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def create_report_update_backup(*, operation_id: str, actor_username: str) -> dict:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_operation_id = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(operation_id)).strip("-") or "operation"
    backup_name = f"{stamp}-{safe_operation_id}.db"
    backup_path = REPORT_BACKUP_DIR / backup_name
    metadata_path = backup_path.with_suffix(".json")

    try:
        with get_connection() as source_conn:
            with sqlite3.connect(str(backup_path), timeout=30.0) as backup_conn:
                source_conn.backup(backup_conn)
        metadata = {
            "operationId": operation_id,
            "actorUsername": actor_username,
            "createdAt": utc_now_iso(),
            "backupFile": backup_path.name,
            "backupPath": backup_path.as_posix(),
        }
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001
        backup_path.unlink(missing_ok=True)
        metadata_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=500,
            detail=f"Falha ao criar backup pré-atualização. Atualização cancelada para preservar histórico. Detalhe: {exc}",
        ) from exc

    return {
        "fileName": backup_path.name,
        "filePath": backup_path.as_posix(),
        "metadataFile": metadata_path.name,
        "createdAt": utc_now_iso(),
    }


def brl_to_cents(value: float | Decimal | int) -> int:
    decimal_value = Decimal(str(value))
    return int((decimal_value * 100).quantize(Decimal("1")))


def normalize_email_value(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if not EMAIL_REGEX.match(text):
        raise HTTPException(status_code=400, detail=f"E-mail invalido: {text}")
    return text


def split_email_targets(value: str | None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    tokens = [item.strip().lower() for item in re.split(r"[;,]", raw) if item.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        normalized = normalize_email_value(token)
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def normalize_email_targets(value: str | None) -> str:
    return ", ".join(split_email_targets(value))


def build_order_external_id() -> str:
    return f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"


def parse_brl_number(value: str | None) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    clean = (
        text.replace("R$", "")
        .replace(" ", "")
        .replace("\u00a0", "")
        .replace(".", "")
        .replace(",", ".")
    )
    try:
        return float(clean)
    except ValueError:
        return None


def decode_canvas_signature(payload: str | None) -> bytes | None:
    if not payload:
        return None
    raw = payload.strip()
    if raw.startswith("data:"):
        parts = raw.split(",", 1)
        if len(parts) != 2:
            return None
        raw = parts[1]
    try:
        return base64.b64decode(raw, validate=True)
    except Exception:
        return None


def hash_order_signature(*, pdf_bytes: bytes, order_number: str, signed_by: str, signed_at_iso: str) -> str:
    digest = hashlib.sha256()
    digest.update(pdf_bytes)
    digest.update(order_number.encode("utf-8"))
    digest.update(signed_by.encode("utf-8"))
    digest.update(signed_at_iso.encode("utf-8"))
    digest.update(ORDER_SIGNATURE_SECRET.encode("utf-8"))
    return digest.hexdigest()


def classify_credit_alert(exposure_cents: int, available_cents: int, limit_cents: int) -> str:
    if exposure_cents <= 0:
        if available_cents > 0 or limit_cents > 0:
            return "Sem exposicao"
        return "Sem dados"

    if limit_cents <= 0:
        return "Sem limite livre"

    debt_to_limit_ratio = safe_ratio(exposure_cents, limit_cents)
    if debt_to_limit_ratio > 1.0:
        return "Acima do limite"
    if available_cents <= 0:
        return "Atencao"
    if debt_to_limit_ratio >= 0.85:
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
    issued_at = datetime.now(timezone.utc)
    payload = {
        "sub": str(user.id),
        "username": user.username,
        "name": user.name,
        "isAdmin": bool(user.is_admin),
        "email": user.email,
        "iat": int(issued_at.timestamp()),
        "exp": int((issued_at + timedelta(seconds=AUTH_TOKEN_TTL_SECONDS)).timestamp()),
    }
    token = jwt.encode(payload, AUTH_JWT_SECRET, algorithm=AUTH_JWT_ALGORITHM)
    TOKEN_STORE[token] = user
    return {
        "accessToken": token,
        "user": {
            "id": user.id,
            "name": user.name,
            "username": user.username,
            "role": "admin" if user.is_admin else "consultor",
            "email": user.email,
        },
    }


def get_current_user_from_header(authorization: str | None) -> AuthenticatedUser:
    if not authorization:
        raise HTTPException(status_code=401, detail="Token ausente.")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Formato de token invalido.")
    token = parts[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token ausente.")

    cached_user = TOKEN_STORE.get(token)
    if cached_user:
        return cached_user

    try:
        payload = jwt.decode(token, AUTH_JWT_SECRET, algorithms=[AUTH_JWT_ALGORITHM])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expirado. Faca login novamente.") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Token invalido ou expirado.") from exc

    username = str(payload.get("username") or "").strip()
    name = str(payload.get("name") or "").strip()
    user_id_raw = payload.get("sub")
    try:
        user_id = int(str(user_id_raw).strip())
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="Token invalido ou expirado.") from exc
    if user_id <= 0 or not username or not name:
        raise HTTPException(status_code=401, detail="Token invalido ou expirado.")

    restored_user = AuthenticatedUser(
        id=user_id,
        name=name,
        username=username,
        is_admin=bool(payload.get("isAdmin", False)),
        email=str(payload.get("email") or "").strip() or None,
    )
    TOKEN_STORE[token] = restored_user
    return restored_user


def ensure_admin(user: AuthenticatedUser) -> None:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Acesso exclusivo de administrador.")


def is_operational_username(username: str | None) -> bool:
    return str(username or "").strip().lower() in OPERATIONAL_USERNAMES


def is_financial_username(username: str | None) -> bool:
    return str(username or "").strip().lower() in FINANCIAL_USERNAMES


def is_commercial_director_username(username: str | None) -> bool:
    return str(username or "").strip().lower() in COMMERCIAL_DIRECTOR_USERNAMES


def is_isabel_username(username: str | None) -> bool:
    return str(username or "").strip().lower() in ISABEL_USERNAMES


def ensure_operational_user(user: AuthenticatedUser) -> None:
    if is_operational_username(user.username):
        return
    raise HTTPException(status_code=403, detail="Acesso restrito a Marcos e Isabel.")


def ensure_isabel(user: AuthenticatedUser) -> None:
    if not is_isabel_username(user.username):
        raise HTTPException(status_code=403, detail="Acesso exclusivo da Isabel.")


def ensure_status_access(user: AuthenticatedUser) -> None:
    ensure_operational_user(user)


def ensure_commercial_signature_user(user: AuthenticatedUser) -> None:
    if is_commercial_director_username(user.username):
        return
    raise HTTPException(status_code=403, detail="Assinatura desta etapa e exclusiva do diretor comercial.")


def ensure_financial_receipts_access(user: AuthenticatedUser) -> None:
    if is_financial_username(user.username) or is_operational_username(user.username):
        return
    raise HTTPException(status_code=403, detail="Acesso restrito ao time operacional autorizado.")


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


def build_credit_lookup_for_client_health(credit_rows: list[dict]) -> dict[tuple[int, str], dict]:
    lookup: dict[tuple[int, str], dict] = {}
    for row in credit_rows:
        consultant_id = int(row.get("consultant_id") or 0)
        if consultant_id <= 0:
            continue
        customer_name = sanitize_company_name(str(row.get("customer_name") or ""))
        key = (consultant_id, normalize_customer_key(customer_name))
        limit_cents = int(row.get("credit_limit_cents") or 0)
        used_cents = int(row.get("credit_used_cents") or 0)
        available_cents = int(row.get("credit_available_cents") or 0)
        current = lookup.get(key)
        if current is None:
            lookup[key] = {
                "creditLimitCents": limit_cents,
                "creditUsedCents": used_cents,
                "creditAvailableCents": available_cents,
            }
            continue
        # Duplicates can appear in imported sheets; keep the strongest registered values.
        current["creditLimitCents"] = max(int(current.get("creditLimitCents") or 0), limit_cents)
        current["creditUsedCents"] = max(int(current.get("creditUsedCents") or 0), used_cents)
        current["creditAvailableCents"] = max(int(current.get("creditAvailableCents") or 0), available_cents)
    return lookup


def evaluate_credit_penalty_for_client(
    *,
    total_balance_cents: int,
    overdue_ratio: float,
    due_15_ratio: float,
    credit_snapshot: dict | None,
) -> dict:
    limit_cents = int((credit_snapshot or {}).get("creditLimitCents") or 0)
    used_cents = int((credit_snapshot or {}).get("creditUsedCents") or 0)
    available_cents = int((credit_snapshot or {}).get("creditAvailableCents") or 0)
    has_limit = limit_cents > 0

    if total_balance_cents <= 0:
        return {
            "penalty": 0.0,
            "hasLimit": has_limit,
            "creditLimitCents": limit_cents,
            "creditUsedCents": used_cents,
            "creditAvailableCents": available_cents,
            "coverageRatio": 1.0,
            "debtToLimitRatio": 0.0,
            "maxScoreCap": 100,
            "flags": [],
        }

    if not has_limit:
        debt_scale = max(0.0, min(1.0, total_balance_cents / float(brl_to_cents(2_500_000))))
        penalty = 11.0 + debt_scale * 9.0 + overdue_ratio * 6.0 + due_15_ratio * 4.0
        cap = 70 if overdue_ratio < 0.08 else 62
        return {
            "penalty": penalty,
            "hasLimit": False,
            "creditLimitCents": 0,
            "creditUsedCents": 0,
            "creditAvailableCents": 0,
            "coverageRatio": 0.0,
            "debtToLimitRatio": 9.99,
            "maxScoreCap": cap,
            "flags": ["sem_limite_cadastrado"],
        }

    debt_to_limit_ratio = safe_ratio(total_balance_cents, limit_cents)
    inferred_available_cents = max(0, limit_cents - total_balance_cents)
    effective_available_cents = max(available_cents, inferred_available_cents)
    coverage_ratio = safe_ratio(effective_available_cents, total_balance_cents)
    used_ratio = safe_ratio(used_cents, limit_cents)

    limit_pressure = max(0.0, min(1.0, (debt_to_limit_ratio - 0.90) / 0.80))
    coverage_deficit = max(0.0, min(1.0, 1.0 - coverage_ratio))
    high_usage_pressure = max(0.0, min(1.0, (used_ratio - 0.85) / 0.15))
    over_limit_pressure = max(0.0, min(1.0, (debt_to_limit_ratio - 1.0) / 0.8))
    no_available = 1.0 if effective_available_cents <= 0 and debt_to_limit_ratio > 1.0 else 0.0

    # Disponível zerado não deve derrubar score de forma extrema quando a dívida
    # ainda está dentro do limite e sem atraso relevante.
    coverage_weight = 8.0
    if debt_to_limit_ratio <= 1.0 and overdue_ratio < 0.03:
        coverage_weight = 2.5
    if debt_to_limit_ratio <= 0.90 and overdue_ratio < 0.03:
        coverage_weight = 1.8

    penalty = (
        limit_pressure * 10.0
        + coverage_deficit * coverage_weight
        + high_usage_pressure * 4.0
        + over_limit_pressure * 8.0
        + no_available * 6.0
        + overdue_ratio * 2.0
    )
    if debt_to_limit_ratio <= 0.75 and overdue_ratio < 0.05:
        penalty = max(0.0, penalty - 3.0)

    flags: list[str] = []
    score_cap = 100
    if effective_available_cents <= 0 and debt_to_limit_ratio >= 1.35:
        flags.append("sem_limite_livre")
        score_cap = 60
    elif debt_to_limit_ratio > 1.05 and coverage_ratio < 0.35:
        flags.append("cobertura_baixa")
        score_cap = 68

    return {
        "penalty": penalty,
        "hasLimit": True,
        "creditLimitCents": limit_cents,
        "creditUsedCents": used_cents,
        "creditAvailableCents": effective_available_cents,
        "coverageRatio": coverage_ratio,
        "debtToLimitRatio": debt_to_limit_ratio,
        "maxScoreCap": score_cap,
        "flags": flags,
    }


def build_client_scope_key(
    consultant_id: int,
    customer_code: str | None,
    customer_name: str | None,
) -> tuple[int, str]:
    normalized_code = str(customer_code or "").strip()
    if normalized_code.lower() in {"nan", "none", "null", "<na>"}:
        normalized_code = ""
    if normalized_code:
        return int(consultant_id), f"code::{normalized_code.upper()}"
    return int(consultant_id), f"name::{normalize_customer_key(str(customer_name or ''))}"


def build_client_health_for_api(
    df: pd.DataFrame,
    *,
    credit_rows: list[dict] | None = None,
    customer_rows: list[dict] | None = None,
) -> list[dict]:
    credit_lookup = build_credit_lookup_for_client_health(credit_rows or [])
    records: list[dict] = []
    existing_keys: set[tuple[int, str]] = set()

    if not df.empty:
        enriched = build_enriched_receivables(df, today=date.today())
        if "customer_name" in enriched.columns:
            enriched = enriched.copy()
            enriched["customer_name"] = enriched["customer_name"].map(sanitize_company_name)

        for keys, group in enriched.groupby(
            ["consultant_id", "consultant_name", "customer_name", "customer_code"], as_index=False
        ):
            consultant_id, consultant_name, customer_name, customer_code = keys
            normalized_customer_code = str(customer_code or "").strip()
            if normalized_customer_code.lower() in {"nan", "none", "null", "<na>"}:
                normalized_customer_code = ""
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
                + near_pressure * 14.0
                + title_stress * 6.0
                + concentration_risk * 6.0
                + trajectory_risk * 6.0
            )
            escalation_penalty = (
                max(0.0, overdue_ratio - 0.35) * 35.0
                + max(0.0, severe_overdue_ratio - 0.2) * 20.0
            )

            credit_key = (int(consultant_id), normalize_customer_key(str(customer_name or "")))
            credit_eval = evaluate_credit_penalty_for_client(
                total_balance_cents=total_balance_cents,
                overdue_ratio=overdue_ratio,
                due_15_ratio=due_15_ratio,
                credit_snapshot=credit_lookup.get(credit_key),
            )
            has_limit = bool(credit_eval["hasLimit"])
            coverage_ratio = float(credit_eval["coverageRatio"])
            debt_to_limit_ratio = float(credit_eval["debtToLimitRatio"])
            credit_available_cents = int(credit_eval["creditAvailableCents"])
            raw_score = 100.0 - base_penalty - escalation_penalty - float(credit_eval["penalty"])
            positive_bonus = 0.0
            if has_limit and total_balance_cents > 0:
                if overdue_ratio <= 0.01:
                    positive_bonus += 3.5
                if severe_overdue_ratio <= 0.02:
                    positive_bonus += 1.5
                if debt_to_limit_ratio <= 0.75:
                    positive_bonus += 3.0
                elif debt_to_limit_ratio <= 0.90:
                    positive_bonus += 1.5
                if coverage_ratio >= 0.30:
                    positive_bonus += 1.5
                if due_15_ratio <= 0.35:
                    positive_bonus += 1.0
            elif not has_limit and overdue_ratio <= 0.01 and due_15_ratio <= 0.20:
                positive_bonus += 1.0

            risk_score = int(round(max(0.0, min(100.0, raw_score))))
            risk_score = int(round(max(0.0, min(100.0, risk_score + positive_bonus))))
            score_cap = int(credit_eval["maxScoreCap"])
            if score_cap < 100:
                risk_score = min(risk_score, score_cap)

            financial_status = classify_financial_status(
                risk_score=risk_score,
                overdue_ratio=overdue_ratio,
                due_15_ratio=due_15_ratio,
                severe_overdue_ratio=severe_overdue_ratio,
            )
            mapped_status = map_financial_status(financial_status)
            if not has_limit and total_balance_cents > 0:
                action = "Cliente sem limite cadastrado. Priorizar cadastro/revisao de limite antes de novas propostas."
            elif has_limit and debt_to_limit_ratio > 1.0 and total_balance_cents > 0:
                action = "Exposicao acima do limite de credito. Priorizar revisao de limite e tratativa comercial."
            elif has_limit and debt_to_limit_ratio <= 0.85 and overdue_ratio <= 0.03 and total_balance_cents > 0:
                action = "Cliente com limite e baixo risco de atraso. Cenario saudavel para continuidade comercial."
            elif has_limit and credit_available_cents <= 0 and overdue_ratio <= 0.01 and total_balance_cents > 0:
                action = "Limite totalmente utilizado no momento (sem folga), porem sem atraso relevante."
            elif has_limit and credit_available_cents <= 0 and total_balance_cents > 0:
                action = (
                    "Limite totalmente utilizado com pressao financeira. "
                    "Reforcar cobranca preventiva e revisao de limite."
                )
            elif has_limit and debt_to_limit_ratio >= 0.85 and total_balance_cents > 0:
                action = "Limite proximo do teto de uso. Acompanhar recebimentos e revisar novas concessoes."
            else:
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
                    "customerCode": normalized_customer_code,
                    "totalBalance": cents_to_brl(total_balance_cents),
                    "overdue": cents_to_brl(overdue_cents),
                    "due7": cents_to_brl(due_7_cents),
                    "due30": cents_to_brl(due_30_cents),
                    "titles": titles_count,
                    "score": risk_score,
                    "status": mapped_status,
                    "action": action,
                    "scoreModel": {
                        "basePenalty": float(round(base_penalty, 2)),
                        "escalationPenalty": float(round(escalation_penalty, 2)),
                        "creditPenalty": float(round(float(credit_eval["penalty"]), 2)),
                        "positiveBonus": float(round(positive_bonus, 2)),
                        "hasCreditLimit": has_limit,
                        "creditCoverageRatio": float(round(coverage_ratio, 4)),
                        "debtToLimitRatio": float(round(float(credit_eval["debtToLimitRatio"]), 4)),
                        "creditLimit": cents_to_brl(int(credit_eval["creditLimitCents"])),
                        "creditAvailable": cents_to_brl(int(credit_eval["creditAvailableCents"])),
                        "scoreCap": score_cap,
                        "flags": credit_eval["flags"],
                    },
                }
            )
            existing_keys.add(
                build_client_scope_key(
                    int(consultant_id),
                    normalized_customer_code,
                    str(customer_name),
                )
            )

    for row in customer_rows or []:
        consultant_id = int(row.get("consultant_id") or 0)
        if consultant_id <= 0:
            continue
        consultant_name = str(row.get("consultant_name") or "").strip()
        customer_name = sanitize_company_name(str(row.get("customer_name") or ""))
        customer_code = str(row.get("customer_code") or "").strip()
        if not customer_name:
            continue

        customer_key = build_client_scope_key(
            consultant_id,
            customer_code,
            customer_name,
        )
        if customer_key in existing_keys:
            continue

        credit_key = (consultant_id, normalize_customer_key(customer_name))
        credit_eval = evaluate_credit_penalty_for_client(
            total_balance_cents=0,
            overdue_ratio=0.0,
            due_15_ratio=0.0,
            credit_snapshot=credit_lookup.get(credit_key),
        )
        has_limit = bool(credit_eval["hasLimit"])
        records.append(
            {
                "consultantId": consultant_id,
                "consultantName": consultant_name,
                "customerName": customer_name,
                "customerCode": customer_code,
                "totalBalance": 0,
                "overdue": 0,
                "due7": 0,
                "due30": 0,
                "titles": 0,
                "score": 100,
                "status": "Saudavel",
                "action": (
                    "Cliente cadastrado manualmente sem titulos em aberto no momento."
                    if has_limit
                    else "Cliente sem limite cadastrado. Priorizar cadastro/revisao de limite antes de novas propostas."
                ),
                "scoreModel": {
                    "basePenalty": 0.0,
                    "escalationPenalty": 0.0,
                    "creditPenalty": 0.0,
                    "positiveBonus": 0.0,
                    "hasCreditLimit": has_limit,
                    "creditCoverageRatio": 1.0,
                    "debtToLimitRatio": 0.0,
                    "creditLimit": cents_to_brl(int(credit_eval["creditLimitCents"])),
                    "creditAvailable": cents_to_brl(int(credit_eval["creditAvailableCents"])),
                    "scoreCap": 100,
                    "flags": credit_eval["flags"],
                },
            }
        )
        existing_keys.add(customer_key)

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
        alert = classify_credit_alert(exposure_cents, credit_available_cents, credit_limit_cents)

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


def extract_order_data_from_pdf(pdf_path: Path) -> dict:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Falha ao ler PDF: {exc}") from exc

    text = "\n".join((page.extract_text() or "") for page in reader.pages[:3])
    normalized_text = re.sub(r"\s+", " ", text)

    order_number = None
    for pattern in [
        r"PEDIDO\s+DE\s+VENDA\s*Nro\.?\s*:\s*([0-9A-Za-z\-_/]+)",
        r"N[uú]mero\s+do\s+Pedido\s*:\s*([0-9A-Za-z\-_/]+)",
        r"\bPedido\s*:\s*([0-9A-Za-z\-_/]+)",
    ]:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
        if match:
            order_number = match.group(1).strip()
            break

    customer_id_doc = None
    for pattern in [
        r"\bID\s*:\s*([0-9A-Za-z\-./]+)",
        r"ID\s+Cliente\s*:\s*([0-9A-Za-z\-./]+)",
    ]:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
        if match:
            customer_id_doc = match.group(1).strip()
            break

    customer_name = None
    for pattern in [
        r"Cliente\s*:\s*([^:\n\r]+?)\s+ID\s*:",
        r"Cliente\s*:\s*([^:\n\r]+)",
    ]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            customer_name = " ".join(match.group(1).split()).strip()
            if customer_name:
                break

    order_value = None
    for pattern in [
        r"\bTotal\s*:\s*R?\$?\s*([0-9\.\,]+)",
        r"Valor\s+Total\s*:\s*R?\$?\s*([0-9\.\,]+)",
        r"Valor\s+do\s+Pedido\s*:\s*R?\$?\s*([0-9\.\,]+)",
    ]:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
        if match:
            order_value = parse_brl_number(match.group(1))
            if order_value is not None:
                break

    return {
        "orderNumber": order_number,
        "customerIdDoc": customer_id_doc,
        "customerName": customer_name,
        "orderValue": order_value,
    }


def ensure_order_email_config(conn) -> dict:
    row = conn.execute(
        """
        SELECT
            id,
            isabel_emails,
            vitor_emails,
            marcos_emails,
            updated_by_user_id,
            updated_at,
            created_at
        FROM order_email_config
        WHERE id = 1
        """
    ).fetchone()
    if row:
        return dict(row)

    default_isabel = normalize_email_targets(os.getenv("ISABEL_EMAIL", ""))
    default_vitor = normalize_email_targets(os.getenv("VITOR_EMAIL", ""))
    default_marcos = normalize_email_targets(os.getenv("MARCOS_EMAIL", ""))
    conn.execute(
        """
        INSERT INTO order_email_config (
            id,
            isabel_emails,
            vitor_emails,
            marcos_emails,
            updated_at
        )
        VALUES (1, ?, ?, ?, ?)
        """,
        (default_isabel, default_vitor, default_marcos, utc_now_iso()),
    )
    created = conn.execute(
        """
        SELECT
            id,
            isabel_emails,
            vitor_emails,
            marcos_emails,
            updated_by_user_id,
            updated_at,
            created_at
        FROM order_email_config
        WHERE id = 1
        """
    ).fetchone()
    return dict(created)


def list_active_admin_email_targets(conn) -> list[str]:
    admins = list_active_admin_users()
    emails: list[str] = []
    seen: set[str] = set()
    for admin in admins:
        for target in split_email_targets(admin.get("email")):
            if target not in seen:
                seen.add(target)
                emails.append(target)
    return emails


def get_requester_email(conn, requester_user_id: int) -> str | None:
    row = conn.execute(
        "SELECT email FROM consultants WHERE id = ?",
        (int(requester_user_id),),
    ).fetchone()
    if not row:
        return None
    emails = split_email_targets(row["email"])
    return emails[0] if emails else None


def record_order_event(
    conn,
    *,
    order_request_id: int,
    event_type: str,
    actor_user_id: int | None,
    actor_name: str | None,
    from_status: str | None = None,
    to_status: str | None = None,
    message: str | None = None,
    payload: dict | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO order_request_events (
            order_request_id,
            event_type,
            actor_user_id,
            actor_name,
            from_status,
            to_status,
            message,
            payload_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(order_request_id),
            event_type,
            actor_user_id,
            actor_name,
            from_status,
            to_status,
            message,
            json.dumps(payload or {}, ensure_ascii=False),
            utc_now_iso(),
        ),
    )


def record_order_email_log(
    conn,
    *,
    order_request_id: int,
    email_kind: str,
    recipient: str,
    subject: str,
    success: bool,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO order_email_logs (
            order_request_id,
            email_kind,
            recipient,
            subject,
            success,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(order_request_id),
            email_kind,
            recipient,
            subject,
            1 if success else 0,
            error_message,
        ),
    )


def send_email_with_optional_pdf(
    *,
    recipient: str,
    subject: str,
    body: str,
    pdf_path: Path | None,
) -> tuple[bool, str | None]:
    if not SMTP_HOST:
        return False, "SMTP_HOST nao configurado."

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_FROM
    message["To"] = recipient
    message.set_content(body)

    if pdf_path is not None:
        try:
            content = pdf_path.read_bytes()
        except FileNotFoundError:
            return False, "PDF de anexo nao encontrado."
        message.add_attachment(
            content,
            maintype="application",
            subtype="pdf",
            filename=pdf_path.name,
        )

    def _send_once(*, host: str, verify_tls: bool) -> None:
        if SMTP_USE_TLS:
            tls_context = ssl.create_default_context()
            if not verify_tls:
                tls_context.check_hostname = False
                tls_context.verify_mode = ssl.CERT_NONE
            with smtplib.SMTP(host, SMTP_PORT, timeout=30) as server:
                server.starttls(context=tls_context)
                if SMTP_USERNAME and SMTP_PASSWORD:
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(message)
        else:
            ssl_context = ssl.create_default_context()
            if not verify_tls:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            with smtplib.SMTP_SSL(host, SMTP_PORT, timeout=30, context=ssl_context) as server:
                if SMTP_USERNAME and SMTP_PASSWORD:
                    server.login(SMTP_USERNAME, SMTP_PASSWORD)
                server.send_message(message)

    host_candidates: list[str] = []
    seen_hosts: set[str] = set()
    for host in [SMTP_HOST, *SMTP_FALLBACK_HOSTS]:
        normalized = host.strip().lower()
        if not normalized or normalized in seen_hosts:
            continue
        seen_hosts.add(normalized)
        host_candidates.append(host.strip())
    if SMTP_HOST.strip().lower() == "smtp.office365.com" and "smtp-mail.outlook.com" not in seen_hosts:
        host_candidates.append("smtp-mail.outlook.com")

    errors: list[str] = []
    for host in host_candidates:
        for attempt in range(1, SMTP_MAX_RETRIES + 1):
            try:
                _send_once(host=host, verify_tls=SMTP_TLS_VERIFY)
                return True, None
            except Exception as exc:
                error_text = str(exc)
                cert_error = "CERTIFICATE_VERIFY_FAILED" in error_text.upper()
                if cert_error and SMTP_TLS_VERIFY and SMTP_TLS_ALLOW_INSECURE_FALLBACK:
                    try:
                        _send_once(host=host, verify_tls=False)
                        return True, None
                    except Exception as fallback_exc:
                        error_text = str(fallback_exc)

                errors.append(f"{host} tentativa {attempt}/{SMTP_MAX_RETRIES}: {error_text}")
                is_auth_error = isinstance(exc, smtplib.SMTPAuthenticationError) or "AUTHENTICATION" in error_text.upper()
                if is_auth_error:
                    break
                if attempt < SMTP_MAX_RETRIES and SMTP_RETRY_BACKOFF_SECONDS > 0:
                    time.sleep(SMTP_RETRY_BACKOFF_SECONDS * attempt)

    if not errors:
        return False, "Falha de envio sem detalhe."
    return False, " | ".join(errors[-4:])


def safe_json_dict(raw_value: str | None) -> dict:
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def normalize_order_distribution(raw_value: str | None) -> dict:
    payload = safe_json_dict(raw_value)
    approvals_raw = payload.get("approvals")
    notifications_raw = payload.get("notifications")
    approvals = [item for item in approvals_raw if isinstance(item, dict)] if isinstance(approvals_raw, list) else []
    notifications = (
        [item for item in notifications_raw if isinstance(item, dict)]
        if isinstance(notifications_raw, list)
        else []
    )
    payload["approvals"] = approvals
    payload["notifications"] = notifications
    return payload


def status_stage_label(status: str) -> str:
    if status == ORDER_STATUS_LEGACY_AWAITING_FINANCIAL_SIGNATURE:
        return "Diretor Comercial"
    if status == ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE:
        return "Diretor Comercial"
    if status == ORDER_STATUS_AWAITING_SIGNATURE:
        return "Isabel"
    if status == ORDER_STATUS_BILLED:
        return "Faturado"
    if status == ORDER_STATUS_DONE:
        return "Concluido"
    return status


def next_status_for_signature(status: str) -> str | None:
    if status in {
        ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE,
        ORDER_STATUS_NEGATIVE,
        ORDER_STATUS_RETURNED,
        ORDER_STATUS_LEGACY_AWAITING_FINANCIAL_SIGNATURE,
    }:
        return ORDER_STATUS_AWAITING_SIGNATURE
    if status == ORDER_STATUS_AWAITING_SIGNATURE:
        return ORDER_STATUS_DONE
    return None


def format_signed_at_for_pdf(signed_at_iso: str | None) -> str:
    if not signed_at_iso:
        return "-"
    try:
        parsed = datetime.fromisoformat(str(signed_at_iso).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")
    except ValueError:
        return str(signed_at_iso)


def serialize_order_row(row: dict) -> dict:
    extracted = safe_json_dict(row.get("extracted_json"))
    distribution = normalize_order_distribution(row.get("distribution_json"))
    return {
        "id": int(row["id"]),
        "externalId": row["external_id"],
        "orderNumber": row["order_number"],
        "consultantId": int(row["consultant_id"]),
        "requestedByUserId": int(row["requested_by_user_id"]),
        "customerCode": row["customer_code"],
        "customerName": row["customer_name"],
        "customerIdDoc": row["customer_id_doc"],
        "orderValue": cents_to_brl(int(row["order_value_cents"])),
        "openBalance": cents_to_brl(int(row["open_balance_cents"])),
        "creditLimit": cents_to_brl(int(row["credit_limit_cents"])),
        "overLimit": cents_to_brl(int(row["over_limit_cents"])),
        "status": row["status"],
        "statusReason": row["status_reason"],
        "extracted": extracted,
        "distribution": distribution,
        "originalPdfPath": row["original_pdf_path"],
        "signedPdfPath": row["signed_pdf_path"],
        "analysisPdfPath": row.get("analysis_pdf_path"),
        "packagePdfPath": row.get("package_pdf_path"),
        "signatureMode": row["signature_mode"],
        "signatureHash": row["signature_hash"],
        "signedByUserId": row["signed_by_user_id"],
        "signedByName": row.get("signed_by_name"),
        "signedAt": row["signed_at"],
        "returnedReason": row["returned_reason"],
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def resolve_credit_snapshot(
    conn,
    *,
    consultant_id: int,
    customer_code: str | None,
    customer_name: str,
    lookup_customer_name: str | None,
    order_value_cents: int,
) -> dict:
    canonical_name = sanitize_company_name(customer_name)
    customer_id: int | None = None
    code = str(customer_code or "").strip()
    normalized_target_name = normalize_customer_key(canonical_name)

    candidate_names: list[str] = []
    for raw in [lookup_customer_name, canonical_name]:
        text = sanitize_company_name(raw)
        if not text or is_generic_customer_lookup_name(text):
            continue
        if text not in candidate_names:
            candidate_names.append(text)
    if not candidate_names and canonical_name:
        candidate_names.append(canonical_name)

    candidate_keys = [
        {
            "name": name,
            "exact": normalize_customer_key(name),
            "loose": normalize_loose_customer_name_key(name),
        }
        for name in candidate_names
    ]

    def name_match_score(source_name: str | None) -> int:
        source_text = sanitize_company_name(source_name)
        if not source_text:
            return 0
        source_exact = normalize_customer_key(source_text)
        source_loose = normalize_loose_customer_name_key(source_text)
        source_tokens = set(source_loose.split())
        best = 0
        for candidate in candidate_keys:
            candidate_exact = str(candidate["exact"])
            candidate_loose = str(candidate["loose"])
            if source_exact and candidate_exact and source_exact == candidate_exact:
                best = max(best, 120)
                continue
            if source_loose and candidate_loose and source_loose == candidate_loose:
                best = max(best, 110)
                continue
            if source_loose and candidate_loose and (
                source_loose in candidate_loose or candidate_loose in source_loose
            ):
                if min(len(source_loose), len(candidate_loose)) >= 14:
                    best = max(best, 95)
                    continue
            candidate_tokens = set(candidate_loose.split())
            overlap = len(source_tokens & candidate_tokens)
            if overlap >= 3:
                best = max(best, min(90, 70 + overlap * 5))
        return best

    if code:
        customer_row = conn.execute(
            """
            SELECT cust.id, cust.name
            FROM consultant_customers cc
            JOIN customers cust ON cust.id = cc.customer_id
            WHERE cc.consultant_id = ? AND cust.customer_code = ?
            ORDER BY cust.id DESC
            LIMIT 1
            """,
            (int(consultant_id), code),
        ).fetchone()
        if customer_row:
            customer_id = int(customer_row["id"])
            canonical_name = str(customer_row["name"] or canonical_name)
            normalized_target_name = normalize_customer_key(canonical_name)
    elif canonical_name:
        customer_by_name = conn.execute(
            """
            SELECT cust.id, cust.name
            FROM consultant_customers cc
            JOIN customers cust ON cust.id = cc.customer_id
            WHERE cc.consultant_id = ?
            ORDER BY cust.id DESC
            """,
            (int(consultant_id),),
        ).fetchall()
        best_row = None
        best_score = 0
        for row in customer_by_name:
            score = name_match_score(str(row["name"] or ""))
            if score > best_score:
                best_score = score
                best_row = row
        if best_row is not None and best_score >= 90:
            customer_id = int(best_row["id"])
            canonical_name = sanitize_company_name(str(best_row["name"] or canonical_name))
            normalized_target_name = normalize_customer_key(canonical_name)

    exposure_cents = 0
    if customer_id is not None:
        balance_row = conn.execute(
            """
            SELECT COALESCE(SUM(balance_cents), 0) AS total_balance
            FROM receivables
            WHERE consultant_id = ? AND customer_id = ?
            """,
            (int(consultant_id), int(customer_id)),
        ).fetchone()
        exposure_cents = int(balance_row["total_balance"] or 0)
    elif code:
        balance_row = conn.execute(
            """
            SELECT COALESCE(SUM(r.balance_cents), 0) AS total_balance
            FROM receivables r
            JOIN customers cust ON cust.id = r.customer_id
            WHERE r.consultant_id = ? AND cust.customer_code = ?
            """,
            (int(consultant_id), code),
        ).fetchone()
        exposure_cents = int(balance_row["total_balance"] or 0)
    elif normalized_target_name:
        receivable_rows = conn.execute(
            """
            SELECT cust.name, r.balance_cents
            FROM receivables r
            JOIN customers cust ON cust.id = r.customer_id
            WHERE r.consultant_id = ?
            """,
            (int(consultant_id),),
        ).fetchall()
        best_name_exact: str | None = None
        best_score = 0
        for row in receivable_rows:
            score = name_match_score(str(row["name"] or ""))
            if score > best_score:
                best_score = score
                best_name_exact = normalize_customer_key(str(row["name"] or ""))
        if best_name_exact and best_score >= 90:
            for row in receivable_rows:
                if normalize_customer_key(str(row["name"] or "")) == best_name_exact:
                    exposure_cents += int(row["balance_cents"] or 0)

    credit_rows = conn.execute(
        """
        SELECT customer_name, credit_limit_cents
        FROM credit_limits
        WHERE consultant_id = ?
        ORDER BY id DESC
        """,
        (int(consultant_id),),
    ).fetchall()
    credit_limit_cents = 0
    best_credit_score = 0
    for row in credit_rows:
        score = name_match_score(str(row["customer_name"] or ""))
        if score > best_credit_score:
            best_credit_score = score
            credit_limit_cents = int(row["credit_limit_cents"] or 0)
            if score >= 110:
                break
    if best_credit_score < 90:
        credit_limit_cents = 0

    projected = order_value_cents + exposure_cents
    over_limit_cents = max(0, projected - credit_limit_cents)
    approved = credit_limit_cents > 0
    if not approved:
        reason = "Cliente sem limite cadastrado. Converse com Isabel para revisao."
    elif projected > credit_limit_cents:
        reason = (
            "Cliente com limite cadastrado. Exposicao estimada acima do limite, "
            "encaminhado para aprovacao manual da Isabel."
        )
    else:
        reason = "Cliente com limite cadastrado. Encaminhado para aprovacao da Isabel."

    return {
        "canonicalName": canonical_name,
        "openBalanceCents": exposure_cents,
        "creditLimitCents": credit_limit_cents,
        "overLimitCents": over_limit_cents,
        "approved": approved,
        "reason": reason,
    }

def fetch_order_customer_receivables(
    conn,
    *,
    consultant_id: int,
    customer_code: str | None,
    customer_name: str,
) -> tuple[list[dict], str]:
    rows = conn.execute(
        """
        SELECT
            r.document_ref,
            r.installment,
            r.due_date,
            r.status,
            r.balance_cents,
            cust.customer_code,
            cust.name AS customer_name,
            cons.name AS consultant_name
        FROM receivables r
        JOIN customers cust ON cust.id = r.customer_id
        JOIN consultants cons ON cons.id = r.consultant_id
        WHERE r.consultant_id = ?
        ORDER BY r.balance_cents DESC, r.due_date ASC
        """,
        (int(consultant_id),),
    ).fetchall()

    selected_code = str(customer_code or "").strip()
    selected_name_key = normalize_customer_key(customer_name)
    matched: list[dict] = []
    for row in rows:
        row_code = str(row["customer_code"] or "").strip()
        row_name_key = normalize_customer_key(str(row["customer_name"] or ""))
        if selected_code:
            if row_code and row_code == selected_code:
                matched.append(dict(row))
        elif selected_name_key and row_name_key == selected_name_key:
            matched.append(dict(row))

    if not matched and selected_code and selected_name_key:
        for row in rows:
            row_name_key = normalize_customer_key(str(row["customer_name"] or ""))
            if row_name_key == selected_name_key:
                matched.append(dict(row))

    consultant_name = ""
    if rows:
        consultant_name = str(rows[0]["consultant_name"] or "")
    if not consultant_name:
        row = conn.execute(
            "SELECT name FROM consultants WHERE id = ?",
            (int(consultant_id),),
        ).fetchone()
        consultant_name = str(row["name"] or "") if row else ""

    return matched, consultant_name or f"Consultor #{consultant_id}"


def generate_client_analysis_attachment_pdf(
    *,
    external_id: str,
    consultant_name: str,
    customer_name: str,
    customer_code: str | None,
    order_number: str,
    order_value_cents: int,
    credit_snapshot: dict,
    receivable_rows: list[dict],
) -> Path:
    today = date.today()
    total_cents = 0
    overdue_cents = 0
    due7_cents = 0
    due30_cents = 0
    parsed_rows: list[dict] = []

    for row in receivable_rows:
        balance_cents = int(row.get("balance_cents") or 0)
        total_cents += balance_cents
        due_date = parse_iso_date(row.get("due_date"))
        day_diff = (due_date - today).days if due_date else None
        status = str(row.get("status") or "")
        is_overdue = "vencido" in status.lower() or (day_diff is not None and day_diff < 0)
        if is_overdue:
            overdue_cents += balance_cents
        if day_diff is not None and 0 <= day_diff <= 7:
            due7_cents += balance_cents
        if day_diff is not None and 0 <= day_diff <= 30:
            due30_cents += balance_cents
        parsed_rows.append(
            {
                "documentRef": str(row.get("document_ref") or ""),
                "installment": str(row.get("installment") or ""),
                "dueDate": str(row.get("due_date") or ""),
                "dayDiff": day_diff,
                "balanceCents": balance_cents,
            }
        )

    top_titles = sorted(parsed_rows, key=lambda item: item["balanceCents"], reverse=True)[:10]

    guidance = "Cliente apto para continuidade operacional com monitoramento padrão."
    if int(credit_snapshot.get("creditLimitCents") or 0) <= 0:
        guidance = "Cliente sem limite cadastrado. Requer tratativa antes da aprovação definitiva."
    elif int(credit_snapshot.get("overLimitCents") or 0) > 0:
        guidance = "Exposição estimada acima do limite. Aprovação manual recomendada."
    elif overdue_cents > 0:
        guidance = "Cliente com atraso em aberto. Reforçar acompanhamento preventivo."

    output_path = ORDER_ANALYSIS_DIR / f"{external_id}-analise-cliente.pdf"
    page_width = 595.27  # A4 portrait width in points
    page_height = 841.89  # A4 portrait height in points
    drawing = canvas.Canvas(str(output_path), pagesize=(page_width, page_height))

    y = page_height - 42
    drawing.setFont("Helvetica-Bold", 14)
    drawing.drawString(32, y, "Analise Cliente - Relatorio para Encaminhamento")

    drawing.setFont("Helvetica", 9)
    y -= 16
    drawing.drawString(32, y, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    y -= 14
    drawing.drawString(32, y, f"Pedido: {order_number}")
    y -= 14
    drawing.drawString(
        32,
        y,
        f"Cliente: {customer_name}  |  Codigo: {str(customer_code or '-').strip() or '-'}",
    )
    y -= 14
    drawing.drawString(32, y, f"Consultor: {consultant_name}")
    y -= 14
    drawing.drawString(32, y, f"Valor do pedido: {format_brl_from_cents(order_value_cents)}")

    y -= 28
    drawing.setFont("Helvetica-Bold", 11)
    drawing.drawString(32, y, "Resumo financeiro")
    y -= 14
    drawing.setFont("Helvetica", 9)
    drawing.drawString(32, y, f"Saldo total em aberto: {format_brl_from_cents(total_cents)}")
    y -= 13
    drawing.drawString(32, y, f"Saldo vencido: {format_brl_from_cents(overdue_cents)}")
    y -= 13
    drawing.drawString(32, y, f"Vence em 7 dias: {format_brl_from_cents(due7_cents)}")
    y -= 13
    drawing.drawString(32, y, f"Vence em 30 dias: {format_brl_from_cents(due30_cents)}")
    y -= 13
    drawing.drawString(
        32,
        y,
        f"Limite de credito: {format_brl_from_cents(int(credit_snapshot.get('creditLimitCents') or 0))}",
    )
    y -= 13
    drawing.drawString(
        32,
        y,
        f"Excesso projetado: {format_brl_from_cents(int(credit_snapshot.get('overLimitCents') or 0))}",
    )

    y -= 22
    drawing.setFont("Helvetica-Bold", 10)
    drawing.drawString(32, y, "Leitura operacional")
    y -= 13
    drawing.setFont("Helvetica", 9)
    drawing.drawString(32, y, guidance)

    y -= 22
    drawing.setFont("Helvetica-Bold", 10)
    drawing.drawString(32, y, "Top titulos por exposicao")
    y -= 14
    drawing.setFont("Helvetica-Bold", 8)
    drawing.drawString(32, y, "Documento")
    drawing.drawString(172, y, "Parcela")
    drawing.drawString(248, y, "Vencimento")
    drawing.drawString(334, y, "Prazo")
    drawing.drawString(390, y, "Valor")
    drawing.line(32, y - 3, page_width - 32, y - 3)
    y -= 14
    drawing.setFont("Helvetica", 8)
    for item in top_titles:
        if y < 70:
            break
        due_label = "-"
        if item["dayDiff"] is not None:
            if item["dayDiff"] < 0:
                due_label = f"{abs(int(item['dayDiff']))}d atraso"
            elif item["dayDiff"] == 0:
                due_label = "vence hoje"
            else:
                due_label = f"vence em {int(item['dayDiff'])}d"
        drawing.drawString(32, y, item["documentRef"][:24] or "-")
        drawing.drawString(172, y, item["installment"][:12] or "-")
        drawing.drawString(248, y, item["dueDate"] or "-")
        drawing.drawString(334, y, due_label)
        drawing.drawRightString(page_width - 36, y, format_brl_from_cents(item["balanceCents"]))
        y -= 12

    drawing.setFont("Helvetica", 7.5)
    drawing.drawString(
        32,
        32,
        "Anexo gerado automaticamente para apoiar a decisão da aprovação sem interromper o fluxo operacional.",
    )
    drawing.save()
    return output_path


def build_order_package_pdf(
    *,
    external_id: str,
    order_pdf_path: Path,
    analysis_pdf_path: Path | None,
) -> Path:
    if analysis_pdf_path is None or not analysis_pdf_path.exists() or not order_pdf_path.exists():
        return order_pdf_path

    package_path = ORDER_PACKAGE_DIR / f"{external_id}-pacote.pdf"
    writer = PdfWriter()
    for source_path in [order_pdf_path, analysis_pdf_path]:
        reader = PdfReader(str(source_path))
        for page in reader.pages:
            writer.add_page(page)

    if len(writer.pages) == 0:
        return order_pdf_path

    with package_path.open("wb") as stream:
        writer.write(stream)

    if package_path.stat().st_size > ORDER_MAX_PDF_BYTES:
        package_path.unlink(missing_ok=True)
        return order_pdf_path

    return package_path


def build_signature_overlay(
    *,
    page_width: float,
    page_height: float,
    order_number: str,
    customer_name: str,
    order_value_cents: int,
    approvals: list[dict],
    signature_canvas_bytes: bytes | None,
):
    packet = BytesIO()
    drawing = canvas.Canvas(packet, pagesize=(page_width, page_height))

    margin = 28
    display_approvals = approvals[-3:] if approvals else []
    box_height = max(150, 96 + (len(display_approvals) * 28))
    box_height = min(box_height, int(page_height - 40))
    y = 22
    drawing.setStrokeColorRGB(0.16, 0.34, 0.31)
    drawing.setFillColorRGB(0.94, 0.98, 0.95)
    drawing.roundRect(margin, y, page_width - (margin * 2), box_height, 10, stroke=1, fill=1)

    drawing.setFillColorRGB(0.08, 0.30, 0.20)
    drawing.setFont("Helvetica-Bold", 11)
    drawing.drawString(margin + 12, y + box_height - 20, "APROVADO DIGITALMENTE - ESTEIRA DE ASSINATURAS")

    drawing.setFillColorRGB(0.15, 0.15, 0.15)
    drawing.setFont("Helvetica", 9)
    header_y = y + box_height - 36
    drawing.drawString(margin + 12, header_y, f"Pedido: {order_number}")
    drawing.drawString(margin + 12, header_y - 14, f"Cliente: {customer_name}")
    drawing.drawString(
        margin + 12,
        header_y - 28,
        f"Valor: R$ {cents_to_brl(order_value_cents):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
    )

    line_y = header_y - 48
    drawing.setFont("Helvetica-Bold", 8.6)
    drawing.drawString(margin + 12, line_y, "Assinaturas registradas:")
    line_y -= 12
    for index, approval in enumerate(display_approvals):
        role_label = str(approval.get("roleLabel") or approval.get("stageLabel") or "Responsavel")
        signed_by_name = str(approval.get("signedByName") or approval.get("actorName") or "-")
        signature_mode = str(approval.get("signatureMode") or "-").upper()
        signed_at_iso = str(approval.get("signedAt") or "")
        signature_hash = str(approval.get("signatureHash") or "-")
        hash_preview = f"{signature_hash[:18]}..." if signature_hash and signature_hash != "-" else "-"
        drawing.setFont("Helvetica-Bold", 8.4)
        drawing.drawString(margin + 12, line_y, f"{index + 1}. {role_label}: {signed_by_name}")
        drawing.setFont("Helvetica", 8)
        drawing.drawString(
            margin + 16,
            line_y - 10,
            f"{format_signed_at_for_pdf(signed_at_iso)} | {signature_mode} | hash {hash_preview}",
        )
        line_y -= 24

    signature_samples: list[tuple[str, bytes]] = []
    for approval in reversed(display_approvals):
        canvas_path_raw = str(approval.get("signatureCanvasPath") or "").strip()
        if not canvas_path_raw:
            continue
        canvas_path = Path(canvas_path_raw)
        if not canvas_path.exists():
            continue
        try:
            signature_samples.append((str(approval.get("roleLabel") or "Assinatura"), canvas_path.read_bytes()))
        except OSError:
            continue
        if len(signature_samples) >= 2:
            break
    if not signature_samples and signature_canvas_bytes:
        signature_samples.append(("Assinatura", signature_canvas_bytes))

    sample_y = y + 18
    for role_label, sample_bytes in signature_samples:
        image = ImageReader(BytesIO(sample_bytes))
        drawing.drawImage(
            image,
            page_width - 218,
            sample_y,
            width=168,
            height=48,
            preserveAspectRatio=True,
            mask="auto",
        )
        drawing.setFont("Helvetica", 7.2)
        drawing.drawString(page_width - 218, sample_y - 8, role_label[:30])
        sample_y += 58

    drawing.save()
    packet.seek(0)
    return PdfReader(packet).pages[0]


def generate_signed_order_pdf(
    *,
    original_pdf_path: Path,
    signed_pdf_path: Path,
    signature_image_path: Path | None,
    order_number: str,
    customer_name: str,
    order_value_cents: int,
    signed_by_name: str,
    signature_mode: str,
    signed_at_iso: str | None = None,
    signature_hash: str | None = None,
    approvals: list[dict] | None = None,
) -> tuple[str, int]:
    if not original_pdf_path.exists():
        raise HTTPException(status_code=404, detail="PDF original do pedido nao encontrado.")

    reader = PdfReader(str(original_pdf_path))
    if not reader.pages:
        raise HTTPException(status_code=400, detail="PDF do pedido sem paginas.")

    original_bytes = original_pdf_path.read_bytes()
    resolved_signed_at_iso = signed_at_iso or utc_now_iso()
    resolved_signature_hash = signature_hash or hash_order_signature(
        pdf_bytes=original_bytes,
        order_number=order_number,
        signed_by=signed_by_name,
        signed_at_iso=resolved_signed_at_iso,
    )
    resolved_approvals = approvals or [
        {
            "roleLabel": "Assinatura digital",
            "signedByName": signed_by_name,
            "signedAt": resolved_signed_at_iso,
            "signatureMode": signature_mode,
            "signatureHash": resolved_signature_hash,
        }
    ]

    signature_canvas_bytes = signature_image_path.read_bytes() if signature_image_path else None

    writer = PdfWriter()
    for index, page in enumerate(reader.pages):
        if index == len(reader.pages) - 1:
            overlay = build_signature_overlay(
                page_width=float(page.mediabox.width),
                page_height=float(page.mediabox.height),
                order_number=order_number,
                customer_name=customer_name,
                order_value_cents=order_value_cents,
                approvals=resolved_approvals,
                signature_canvas_bytes=signature_canvas_bytes,
            )
            page.merge_page(overlay)
        writer.add_page(page)

    signed_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    with signed_pdf_path.open("wb") as stream:
        writer.write(stream)

    size = signed_pdf_path.stat().st_size
    if size > ORDER_MAX_PDF_BYTES:
        signed_pdf_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="PDF final excede 10MB.")

    return resolved_signature_hash, size


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


def ensure_bootstrap_named_admin(
    *,
    name: str,
    username: str,
    password: str,
    email_env_key: str,
    is_admin: bool = True,
    legacy_usernames: list[str] | None = None,
) -> None:
    username_clean = str(username).strip().lower()
    if not username_clean:
        return

    alias_keys = [username_clean]
    for legacy in legacy_usernames or []:
        normalized_legacy = str(legacy or "").strip().lower()
        if normalized_legacy and normalized_legacy not in alias_keys:
            alias_keys.append(normalized_legacy)

    raw_email = str(os.getenv(email_env_key, "")).strip().lower()
    bootstrap_email = raw_email if EMAIL_REGEX.match(raw_email) else None

    with get_connection() as conn:
        placeholders = ", ".join("?" for _ in alias_keys)
        row = conn.execute(
            f"""
            SELECT id
            FROM consultants
            WHERE lower(username) IN ({placeholders})
            ORDER BY id ASC
            LIMIT 1
            """,
            tuple(alias_keys),
        ).fetchone()
        password_hash = hash_password(password)
        if row:
            if bootstrap_email:
                conn.execute(
                    """
                    UPDATE consultants
                    SET
                        name = ?,
                        username = ?,
                        password_hash = ?,
                        is_admin = ?,
                        is_active = 1,
                        email = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        username_clean,
                        password_hash,
                        1 if is_admin else 0,
                        bootstrap_email,
                        int(row["id"]),
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE consultants
                    SET
                        name = ?,
                        username = ?,
                        password_hash = ?,
                        is_admin = ?,
                        is_active = 1
                    WHERE id = ?
                    """,
                    (
                        name,
                        username_clean,
                        password_hash,
                        1 if is_admin else 0,
                        int(row["id"]),
                    ),
                )
        else:
            conn.execute(
                """
                INSERT INTO consultants (name, username, password_hash, is_admin, email, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (
                    name,
                    username_clean,
                    password_hash,
                    1 if is_admin else 0,
                    bootstrap_email,
                ),
            )
        conn.commit()


def ensure_bootstrap_isabel_and_marcos_admins() -> None:
    ensure_bootstrap_named_admin(
        name="Isabel",
        username=ISABEL_USERNAME,
        password=DEFAULT_ISABEL_PASSWORD,
        email_env_key="ISABEL_EMAIL",
        legacy_usernames=["isabel"],
    )
    ensure_bootstrap_named_admin(
        name="Marcos",
        username=MARCOS_USERNAME,
        password=DEFAULT_MARCOS_PASSWORD,
        email_env_key="MARCOS_EMAIL",
        legacy_usernames=["marcos"],
    )


def ensure_bootstrap_vitor_financeiro_user() -> None:
    ensure_bootstrap_named_admin(
        name="Vitor Financeiro",
        username=VITOR_FINANCIAL_USERNAME,
        password=DEFAULT_VITOR_FINANCIAL_PASSWORD,
        email_env_key="VITOR_FINANCEIRO_EMAIL",
        is_admin=False,
        legacy_usernames=["vitor_financeiro", "vitor-financeiro"],
    )


def migrate_legacy_order_signature_status() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE order_requests
            SET
                status = ?,
                status_reason = CASE
                    WHEN COALESCE(status_reason, '') = '' THEN ?
                    ELSE status_reason
                END,
                updated_at = ?
            WHERE status = ?
            """,
            (
                ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE,
                "Fluxo atualizado: aguardando assinatura do Diretor Comercial.",
                utc_now_iso(),
                ORDER_STATUS_LEGACY_AWAITING_FINANCIAL_SIGNATURE,
            ),
        )
        conn.commit()


init_db()
ensure_bootstrap_admin()
ensure_bootstrap_isabel_and_marcos_admins()
ensure_bootstrap_vitor_financeiro_user()
migrate_legacy_order_signature_status()


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
            email=user.email,
        )
    return build_auth_session(user)


@app.get("/api/consultants")
def consultants(authorization: str | None = Header(default=None)) -> list[dict]:
    user = get_current_user_from_header(authorization)
    available = list_consultants()
    if user.is_admin or is_operational_username(user.username) or is_financial_username(user.username):
        return [
            {
                "id": int(item["id"]),
                "name": item["name"],
                "username": item["username"],
                "role": "admin" if item["is_admin"] else "consultor",
                "email": item.get("email"),
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
            "email": user.email,
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
    credit_rows = fetch_credit_limits_for_user(user=user, selected_consultant_id=consultantId)
    customer_rows = fetch_consultant_customers_for_user(user=user, selected_consultant_id=consultantId)
    df = rows_to_dataframe(rows)
    return build_client_health_for_api(
        df,
        credit_rows=credit_rows,
        customer_rows=customer_rows,
    )


@app.get("/api/dashboard/credit-limits")
def dashboard_credit_limits(
    consultantId: int | None = None,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    credit_rows = fetch_credit_limits_for_user(user=user, selected_consultant_id=consultantId)
    receivable_rows = fetch_receivables_for_user(user=user, selected_consultant_id=consultantId)
    return build_credit_limits_for_api(credit_rows=credit_rows, receivable_rows=receivable_rows)


@app.post("/api/dashboard/customers")
def dashboard_add_customer(
    customerName: str = Form(...),
    customerCode: str | None = Form(default=None),
    consultantId: int | None = Form(default=None),
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    customer_name = sanitize_company_name(customerName)
    if not customer_name:
        raise HTTPException(status_code=400, detail="Nome do cliente obrigatorio.")

    requested_consultant_id = int(consultantId) if consultantId is not None else None
    has_full_scope = user.is_admin or is_financial_username(user.username)
    if has_full_scope:
        if requested_consultant_id is None:
            raise HTTPException(
                status_code=400,
                detail="Informe o consultor para cadastrar cliente neste escopo.",
            )
        target_consultant_id = requested_consultant_id
    else:
        target_consultant_id = int(user.id)
        if requested_consultant_id is not None and requested_consultant_id != target_consultant_id:
            raise HTTPException(status_code=403, detail="Sem permissao para cadastrar cliente para este consultor.")

    consultant = get_consultant_by_id(target_consultant_id)
    if not consultant or not consultant.get("is_active"):
        raise HTTPException(status_code=400, detail="Consultor informado nao esta ativo.")

    try:
        created = upsert_consultant_customer(
            consultant_id=target_consultant_id,
            customer_name=customer_name,
            customer_code=customerCode,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not bool(created.get("linkCreated")):
        raise HTTPException(status_code=409, detail="Cliente ja existe na carteira deste consultor.")

    return {
        "item": {
            "consultantId": int(created["consultant_id"]),
            "consultantName": str(created.get("consultant_name") or ""),
            "customerName": str(created.get("customer_name") or ""),
            "customerCode": str(created.get("customer_code") or ""),
            "created": bool(created.get("created")),
            "linkCreated": bool(created.get("linkCreated")),
        }
    }


@app.get("/api/admin/ingestion/history")
def admin_ingestion_history(
    limit: int = 30,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)
    return {"items": list_ingestion_batches(limit=limit)}


@app.post("/api/admin/import")
def admin_import(
    mode: str = Form(default="update"),
    strictVendors: str = Form(default="false"),
    appendMode: str = Form(default="false"),
    allowSkippedLines: int = Form(default=0),
    allowSkippedCreditRows: int = Form(default=10),
    inputProfile: str = Form(default="auto"),
    actorUsername: str | None = Form(default=None),
    pdf: UploadFile | None = File(default=None),
    excel: UploadFile | None = File(default=None),
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)
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
    backup_snapshot: dict | None = None
    if op_mode == "update":
        backup_snapshot = create_report_update_backup(
            operation_id=operation_id,
            actor_username=user.username,
        )
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
        f"Usuario autenticado: {user.username}",
        f"Modo: {op_mode}",
        f"Perfil de entrada: {input_profile}",
        f"Append mode: {'sim' if append_mode else 'nao'}",
        f"Estrito consultores: {'sim' if strict else 'nao'}",
    ]
    if backup_snapshot:
        audit_log.append(
            f"[BACKUP] Snapshot criado antes da atualização: {backup_snapshot.get('filePath')}"
        )
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
            "backupSnapshot": backup_snapshot,
        }

    pdf_report = None
    credit_report = None
    excel_receivables_report = None

    actor_value = " ".join(str(actorUsername or "").split())
    if actor_value and actor_value.lower() != user.username.lower():
        audit_log.append(
            f"[AUDITORIA] actorUsername recebido ({actor_value}) ignorado; usuario autenticado usado ({user.username})."
        )
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
            fallback_consultant_name = DEFAULT_REPORT_CONSULTANT
            credit_report = parse_credit_excel(
                temp_excel,
                customer_hints=customer_hints,
                default_consultant_name=fallback_consultant_name,
            )
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
                default_consultant_name=fallback_consultant_name,
            )
            report_layout_validation = None
            if input_profile == "report_v1":
                report_layout_validation = validate_report_v1_workbook_layout(
                    credit_report.workbook_sheets
                )
                warnings.extend(
                    [
                        f"[EXCEL:REPORT_LAYOUT:{item.code}] {item.message}"
                        for item in report_layout_validation.warnings
                    ]
                )
                warnings.extend(
                    [
                        f"[EXCEL:REPORT_LAYOUT:{item.code}] {item.message}"
                        for item in report_layout_validation.errors
                    ]
                )

                expected_columns = {"dueDate", "documentRef", "customerName", "installmentValue"}
                profile_ok = any(
                    (
                        summary.receivable_section_detected
                        and expected_columns.issubset(set(summary.detected_columns))
                    )
                    for summary in excel_receivables_report.sheet_summaries
                )
                if not profile_ok:
                    warnings.append(
                        "[EXCEL:REPORT_PROFILE_MISMATCH] "
                        "Formato fora do perfil report_v1. O sistema aplicou leitura flexivel automatica."
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
                        f"Fallback de consultor automatico: {DEFAULT_REPORT_CONSULTANT}",
                        (
                            "Abas report_v1 detectadas: "
                            + ", ".join(credit_report.workbook_sheets)
                            if input_profile == "report_v1"
                            else "Abas detectadas: " + ", ".join(credit_report.workbook_sheets)
                        ),
                        f"Aba referencia credito: {credit_stats['sheet_name']}",
                        f"Aba referencia titulos: {receivable_stats['sheet_name']}",
                        (
                            "Layout report_v1 -> "
                            f"report={len(report_layout_validation.stats['report_sheets'])} | "
                            f"credito={len(report_layout_validation.stats['credit_sheets'])} | "
                            f"pedidos={len(report_layout_validation.stats['order_sheets'])}"
                            if report_layout_validation
                            else "Layout report_v1 -> nao aplicavel"
                        ),
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
                    (
                        "[EXCEL] abas detectadas: "
                        + ", ".join(credit_report.workbook_sheets)
                    ),
                    (
                        "[EXCEL] layout report_v1 -> "
                        f"report={len(report_layout_validation.stats['report_sheets'])} | "
                        f"credito={len(report_layout_validation.stats['credit_sheets'])} | "
                        f"pedidos={len(report_layout_validation.stats['order_sheets'])}"
                        if report_layout_validation
                        else "[EXCEL] layout report_v1 -> nao aplicavel"
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


@app.post("/api/admin/clear-data")
def admin_clear_data(
    actorUsername: str | None = Form(default=None),
    removeConsultants: str = Form(default="true"),
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)

    actor_value = " ".join(str(actorUsername or "").split())
    if actor_value and actor_value.lower() != user.username.lower():
        raise HTTPException(status_code=400, detail="Actor informado nao corresponde ao usuario autenticado.")

    operation_id = f"CLR-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6].upper()}"
    processed_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    removed = reset_operational_data(remove_non_admin_consultants=to_bool(removeConsultants))

    for token, current_user in list(TOKEN_STORE.items()):
        if not is_operational_username(current_user.username):
            TOKEN_STORE.pop(token, None)

    return {
        "success": True,
        "message": "Base limpa com sucesso.",
        "operationId": operation_id,
        "processedAt": processed_at,
        "removed": removed,
    }


@app.post("/api/admin/import-report-v1")
def admin_import_report_v1(
    excel: UploadFile | None = File(default=None),
    replaceBase: str = Form(default="false"),
    actorUsername: str | None = Form(default=None),
    authorization: str | None = Header(default=None),
) -> dict:
    if not excel:
        raise HTTPException(status_code=400, detail="Selecione o arquivo report.")

    append_mode = "false" if to_bool(replaceBase) else "true"

    return admin_import(
        mode="update",
        strictVendors="false",
        appendMode=append_mode,
        allowSkippedLines=0,
        allowSkippedCreditRows=0,
        inputProfile="auto",
        actorUsername=actorUsername,
        pdf=None,
        excel=excel,
        authorization=authorization,
    )


def fetch_order_row_or_404(conn, order_id: int) -> dict:
    row = conn.execute(
        """
        SELECT *
        FROM order_requests
        WHERE id = ?
        """,
        (int(order_id),),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Pedido de aprovacao nao encontrado.")
    return dict(row)


def ensure_order_access(user: AuthenticatedUser, order_row: dict) -> None:
    if is_operational_username(user.username):
        return
    if is_financial_username(user.username):
        if order_row["status"] != ORDER_STATUS_DELETED:
            return
        raise HTTPException(
            status_code=403,
            detail="Perfil financeiro nao acessa pedidos excluidos.",
        )
    if int(order_row["requested_by_user_id"]) == int(user.id):
        return
    if int(order_row["consultant_id"]) == int(user.id):
        return
    raise HTTPException(status_code=403, detail="Sem permissao para acessar este pedido.")


@app.post("/api/pedidos/extrair")
async def pedidos_extrair(
    pdf: UploadFile = File(...),
    authorization: str | None = Header(default=None),
) -> dict:
    _ = get_current_user_from_header(authorization)
    name = (pdf.filename or "").lower().strip()
    if not name.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Envie um arquivo PDF.")

    raw = await pdf.read()
    if len(raw) > ORDER_MAX_PDF_BYTES:
        raise HTTPException(status_code=400, detail="PDF acima de 10MB.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        temp_file.write(raw)
        temp_path = Path(temp_file.name)

    try:
        extracted = extract_order_data_from_pdf(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)

    return {
        "extracted": extracted,
        "warnings": [
            "Alguns campos podem exigir ajuste manual antes do encaminhamento."
        ],
    }


@app.post("/api/pedidos/encaminhar")
async def pedidos_encaminhar(
    pdf: UploadFile = File(...),
    consultantId: int = Form(...),
    customerCode: str | None = Form(default=None),
    customerName: str = Form(...),
    lookupCustomerName: str | None = Form(default=None),
    orderValue: float = Form(...),
    orderNumber: str | None = Form(default=None),
    customerIdDoc: str | None = Form(default=None),
    routeByEmail: str | bool = Form(default=False),
    recipientEmails: str | None = Form(default=None),
    observations: str | None = Form(default=None),
    attachClientAnalysis: str | bool = Form(default=True),
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    consultant_id = int(consultantId)
    ensure_operational_user(user)
    if orderValue <= 0:
        raise HTTPException(status_code=400, detail="Valor do pedido deve ser maior que zero.")

    consultant = get_consultant_by_id(consultant_id)
    if not consultant or not consultant.get("is_active"):
        raise HTTPException(status_code=400, detail="Consultor informado nao esta ativo.")

    filename = (pdf.filename or "").lower().strip()
    if not filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Envie um arquivo PDF.")

    raw_pdf = await pdf.read()
    if len(raw_pdf) > ORDER_MAX_PDF_BYTES:
        raise HTTPException(status_code=400, detail="PDF acima de 10MB.")

    external_id = build_order_external_id()
    original_pdf_path = ORDER_ORIGINAL_DIR / f"{external_id}.pdf"
    original_pdf_path.write_bytes(raw_pdf)

    extracted = extract_order_data_from_pdf(original_pdf_path)
    resolved_order_number = str(orderNumber or extracted.get("orderNumber") or external_id).strip()
    resolved_customer_name = str(customerName or extracted.get("customerName") or "").strip()
    if not resolved_customer_name:
        raise HTTPException(status_code=400, detail="Nome do cliente obrigatorio.")

    order_value_cents = brl_to_cents(orderValue)
    attach_client_analysis = to_bool(attachClientAnalysis)
    route_by_email = to_bool(routeByEmail)
    manual_recipients = split_email_targets(recipientEmails if route_by_email else None)
    observations_text = str(observations or "").strip()
    if len(observations_text) > 1200:
        raise HTTPException(status_code=400, detail="Observacoes devem ter no maximo 1200 caracteres.")
    observations_block = f"Observacoes: {observations_text}\n" if observations_text else ""
    if route_by_email and not manual_recipients:
        raise HTTPException(
            status_code=400,
            detail="Informe ao menos um e-mail valido para encaminhamento manual.",
        )
    warnings: list[str] = []
    with get_connection() as conn:
        config = ensure_order_email_config(conn)
        credit_snapshot = resolve_credit_snapshot(
            conn,
            consultant_id=consultant_id,
            customer_code=customerCode,
            customer_name=resolved_customer_name,
            lookup_customer_name=lookupCustomerName,
            order_value_cents=order_value_cents,
        )
        canonical_resolved_name = str(credit_snapshot.get("canonicalName") or "").strip()
        if canonical_resolved_name:
            resolved_customer_name = canonical_resolved_name
        status = ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE if credit_snapshot["approved"] else ORDER_STATUS_NEGATIVE
        status_reason = credit_snapshot["reason"]

        analysis_pdf_path: Path | None = None
        if attach_client_analysis:
            try:
                receivable_rows, consultant_name = fetch_order_customer_receivables(
                    conn,
                    consultant_id=consultant_id,
                    customer_code=customerCode,
                    customer_name=resolved_customer_name,
                )
                analysis_pdf_path = generate_client_analysis_attachment_pdf(
                    external_id=external_id,
                    consultant_name=consultant_name,
                    customer_name=resolved_customer_name,
                    customer_code=str(customerCode or "").strip() or None,
                    order_number=resolved_order_number,
                    order_value_cents=order_value_cents,
                    credit_snapshot=credit_snapshot,
                    receivable_rows=receivable_rows,
                )
            except Exception as exc:
                warnings.append(f"Nao foi possivel gerar anexo de analise cliente: {exc}")

        attachment_pdf_path = original_pdf_path
        if analysis_pdf_path is not None:
            try:
                attachment_pdf_path = build_order_package_pdf(
                    external_id=external_id,
                    order_pdf_path=original_pdf_path,
                    analysis_pdf_path=analysis_pdf_path,
                )
            except Exception as exc:
                warnings.append(f"Nao foi possivel gerar pacote com anexo de analise: {exc}")
        package_pdf_path: Path | None = (
            attachment_pdf_path
            if attachment_pdf_path != original_pdf_path and attachment_pdf_path.exists()
            else None
        )

        cursor = conn.execute(
            """
            INSERT INTO order_requests (
                external_id,
                order_number,
                consultant_id,
                requested_by_user_id,
                customer_code,
                customer_name,
                customer_id_doc,
                order_value_cents,
                open_balance_cents,
                credit_limit_cents,
                over_limit_cents,
                status,
                status_reason,
                extracted_json,
                original_pdf_path,
                analysis_pdf_path,
                package_pdf_path,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                external_id,
                resolved_order_number,
                consultant_id,
                int(user.id),
                str(customerCode or "").strip() or None,
                resolved_customer_name,
                str(customerIdDoc or extracted.get("customerIdDoc") or "").strip() or None,
                order_value_cents,
                int(credit_snapshot["openBalanceCents"]),
                int(credit_snapshot["creditLimitCents"]),
                int(credit_snapshot["overLimitCents"]),
                status,
                status_reason,
                json.dumps(extracted, ensure_ascii=False),
                original_pdf_path.as_posix(),
                analysis_pdf_path.as_posix() if analysis_pdf_path else None,
                package_pdf_path.as_posix() if package_pdf_path else None,
                utc_now_iso(),
            ),
        )
        order_id = int(cursor.lastrowid)
        record_order_event(
            conn,
            order_request_id=order_id,
            event_type="CREATED",
            actor_user_id=int(user.id),
            actor_name=user.name,
            to_status=status,
            message="Pedido encaminhado para analise de limite e roteamento.",
            payload={
                "consultantId": consultant_id,
                "customerCode": str(customerCode or "").strip() or None,
                "orderValue": cents_to_brl(order_value_cents),
                "routeByEmail": route_by_email,
                "manualRecipients": manual_recipients,
                "observations": observations_text or None,
                "attachClientAnalysis": attach_client_analysis,
            },
        )

        notifications: list[dict] = []
        if status == ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE:
            recipients = split_email_targets(config["marcos_emails"])
            routed_recipients: list[str] = []
            for recipient in recipients:
                subject = f"[Aprovacao pendente] Pedido {resolved_order_number} - {resolved_customer_name}"
                body = (
                    "Pedido encaminhado para assinatura do Diretor Comercial.\n\n"
                    f"Pedido: {resolved_order_number}\n"
                    f"Cliente: {resolved_customer_name}\n"
                    f"Valor: R$ {cents_to_brl(order_value_cents):,.2f}\n"
                    f"Etapa atual: {status_stage_label(status)}\n"
                    f"{observations_block}"
                    f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                    f"Painel de Status: {ORDER_APPROVAL_PANEL_URL}\n"
                )
                success, error_message = send_email_with_optional_pdf(
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    pdf_path=attachment_pdf_path,
                )
                notifications.append(
                    {
                        "kind": "signature_request",
                        "to": recipient,
                        "success": success,
                        "error": error_message,
                    }
                )
                routed_recipients.append(recipient)
                record_order_email_log(
                    conn,
                    order_request_id=order_id,
                    email_kind="signature_request",
                    recipient=recipient,
                    subject=subject,
                    success=success,
                    error_message=error_message,
                )
            manual_forwarded = [item for item in manual_recipients if item not in routed_recipients]
            for recipient in manual_forwarded:
                subject = f"[Encaminhamento manual] Pedido {resolved_order_number} - {resolved_customer_name}"
                body = (
                    "Encaminhamento manual solicitado no momento do envio para Status.\n\n"
                    f"Solicitante: {user.name}\n"
                    f"Pedido: {resolved_order_number}\n"
                    f"Cliente: {resolved_customer_name}\n"
                    f"Valor: R$ {cents_to_brl(order_value_cents):,.2f}\n"
                    f"Status atual: {status_stage_label(status)}\n\n"
                    f"{observations_block}"
                    f"Painel de Status: {ORDER_APPROVAL_PANEL_URL}\n"
                )
                success, error_message = send_email_with_optional_pdf(
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    pdf_path=attachment_pdf_path,
                )
                notifications.append(
                    {
                        "kind": "manual_forward",
                        "to": recipient,
                        "success": success,
                        "error": error_message,
                    }
                )
                record_order_email_log(
                    conn,
                    order_request_id=order_id,
                    email_kind="manual_forward",
                    recipient=recipient,
                    subject=subject,
                    success=success,
                    error_message=error_message,
                )
            record_order_event(
                conn,
                order_request_id=order_id,
                event_type="ROUTED_SIGNATURE",
                actor_user_id=int(user.id),
                actor_name=user.name,
                from_status=status,
                to_status=status,
                message="Pedido roteado para assinatura do Diretor Comercial.",
                payload={
                    "recipients": recipients,
                    "manualRecipients": manual_forwarded,
                    "routeByEmail": route_by_email,
                    "observations": observations_text or None,
                },
            )
        else:
            recipients = split_email_targets(config["isabel_emails"])
            recipients.extend(list_active_admin_email_targets(conn))
            deduped: list[str] = []
            seen: set[str] = set()
            for recipient in recipients:
                if recipient not in seen:
                    seen.add(recipient)
                    deduped.append(recipient)
            routed_recipients: list[str] = []
            for recipient in deduped:
                subject = f"[Sem limite cadastrado] Pedido {resolved_order_number} - {resolved_customer_name}"
                body = (
                    "Pedido bloqueado automaticamente por ausencia de limite cadastrado.\n\n"
                    f"Pedido: {resolved_order_number}\n"
                    f"Cliente: {resolved_customer_name}\n"
                    f"Valor pedido: R$ {cents_to_brl(order_value_cents):,.2f}\n"
                    f"Saldo em aberto: R$ {cents_to_brl(int(credit_snapshot['openBalanceCents'])):,.2f}\n"
                    f"Limite de credito: R$ {cents_to_brl(int(credit_snapshot['creditLimitCents'])):,.2f}\n"
                    f"Excesso: R$ {cents_to_brl(int(credit_snapshot['overLimitCents'])):,.2f}\n\n"
                    f"{observations_block}"
                    "Status registrado como NEGADO_SEM_LIMITE. Cadastre/atualize o limite para seguir para assinatura."
                )
                success, error_message = send_email_with_optional_pdf(
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    pdf_path=attachment_pdf_path,
                )
                notifications.append(
                    {
                        "kind": "credit_negative",
                        "to": recipient,
                        "success": success,
                        "error": error_message,
                    }
                )
                routed_recipients.append(recipient)
                record_order_email_log(
                    conn,
                    order_request_id=order_id,
                    email_kind="credit_negative",
                    recipient=recipient,
                    subject=subject,
                    success=success,
                    error_message=error_message,
                )
            manual_forwarded = [item for item in manual_recipients if item not in routed_recipients]
            for recipient in manual_forwarded:
                subject = f"[Encaminhamento manual] Pedido {resolved_order_number} - {resolved_customer_name}"
                body = (
                    "Encaminhamento manual solicitado no momento do envio para Status.\n\n"
                    f"Solicitante: {user.name}\n"
                    f"Pedido: {resolved_order_number}\n"
                    f"Cliente: {resolved_customer_name}\n"
                    f"Valor: R$ {cents_to_brl(order_value_cents):,.2f}\n"
                    f"Status atual: {status}\n\n"
                    f"{observations_block}"
                    "Observacao: cliente sem limite cadastrado, fluxo segue para tratativa.\n"
                    f"Painel de Status: {ORDER_APPROVAL_PANEL_URL}\n"
                )
                success, error_message = send_email_with_optional_pdf(
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    pdf_path=attachment_pdf_path,
                )
                notifications.append(
                    {
                        "kind": "manual_forward",
                        "to": recipient,
                        "success": success,
                        "error": error_message,
                    }
                )
                record_order_email_log(
                    conn,
                    order_request_id=order_id,
                    email_kind="manual_forward",
                    recipient=recipient,
                    subject=subject,
                    success=success,
                    error_message=error_message,
                )
            record_order_event(
                conn,
                order_request_id=order_id,
                event_type="NEGATIVE_REGISTERED",
                actor_user_id=int(user.id),
                actor_name=user.name,
                from_status=status,
                to_status=status,
                message="Pedido registrado como negativo por ausencia de limite cadastrado.",
                payload={
                    "recipients": deduped,
                    "manualRecipients": manual_forwarded,
                    "routeByEmail": route_by_email,
                    "observations": observations_text or None,
                },
            )

        failed_notifications = [item for item in notifications if not bool(item.get("success"))]
        if failed_notifications:
            smtp_missing = any(
                str(item.get("error") or "").strip().lower().startswith("smtp_host nao configurado")
                for item in failed_notifications
            )
            if smtp_missing:
                warnings.append(
                    "SMTP nao configurado no servidor. Configure SMTP_HOST/SMTP_PORT/SMTP_USERNAME/"
                    "SMTP_PASSWORD/SMTP_FROM para envio de e-mail."
                )

            manual_failed_targets = sorted(
                {
                    str(item.get("to")).strip().lower()
                    for item in failed_notifications
                    if item.get("kind") == "manual_forward" and str(item.get("to") or "").strip()
                }
            )
            if manual_failed_targets:
                warnings.append(
                    "Falha ao enviar encaminhamento manual para: "
                    + ", ".join(manual_failed_targets)
                    + "."
                )

            automatic_failed_count = len(
                [item for item in failed_notifications if item.get("kind") != "manual_forward"]
            )
            if automatic_failed_count > 0:
                warnings.append(
                    f"{automatic_failed_count} envio(s) automatico(s) tambem falharam. "
                    "Consulte Status > Historico para detalhes."
                )

        conn.execute(
            """
            UPDATE order_requests
            SET distribution_json = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                json.dumps(
                    {
                        "notifications": notifications,
                        "smtpConfigured": bool(SMTP_HOST),
                        "routeByEmail": route_by_email,
                        "manualRecipients": manual_recipients,
                        "observations": observations_text or None,
                        "attachClientAnalysis": attach_client_analysis,
                        "analysisPdfGenerated": bool(analysis_pdf_path),
                        "warnings": warnings,
                    },
                    ensure_ascii=False,
                ),
                utc_now_iso(),
                order_id,
            ),
        )
        conn.commit()
        created = fetch_order_row_or_404(conn, order_id)

    return {
        "order": serialize_order_row(created),
        "credit": {
            "approved": credit_snapshot["approved"],
            "openBalance": cents_to_brl(int(credit_snapshot["openBalanceCents"])),
            "creditLimit": cents_to_brl(int(credit_snapshot["creditLimitCents"])),
            "overLimit": cents_to_brl(int(credit_snapshot["overLimitCents"])),
            "reason": status_reason,
        },
        "warnings": warnings,
    }


@app.get("/api/pedidos/status")
def pedidos_status(
    status: str | None = None,
    customer: str | None = None,
    dateFrom: str | None = None,
    dateTo: str | None = None,
    limit: int = 300,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_status_access(user)

    filters: list[str] = []
    params: list[object] = []
    if status and status in ORDER_STATUS_ALL:
        filters.append("o.status = ?")
        params.append(status)
    else:
        filters.append("o.status <> ?")
        params.append(ORDER_STATUS_DELETED)
    if customer:
        filters.append("o.customer_name LIKE ?")
        params.append(f"%{customer.strip()}%")
    if dateFrom:
        filters.append("substr(o.created_at, 1, 10) >= ?")
        params.append(str(dateFrom))
    if dateTo:
        filters.append("substr(o.created_at, 1, 10) <= ?")
        params.append(str(dateTo))

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    capped_limit = max(1, min(int(limit), 20000))
    params.append(capped_limit)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                o.*,
                cons.name AS consultant_name,
                req.name AS requested_by_name,
                signer.name AS signed_by_name
            FROM order_requests o
            JOIN consultants cons ON cons.id = o.consultant_id
            JOIN consultants req ON req.id = o.requested_by_user_id
            LEFT JOIN consultants signer ON signer.id = o.signed_by_user_id
            {where_sql}
            ORDER BY o.created_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    items = []
    for row in rows:
        payload = serialize_order_row(dict(row))
        payload["consultantName"] = row["consultant_name"]
        payload["requestedByName"] = row["requested_by_name"]
        payload["signedByName"] = row["signed_by_name"]
        items.append(payload)
    return {"items": items}


@app.get("/api/pedidos/resumo")
def pedidos_resumo(
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_status_access(user)

    today_iso = datetime.now(timezone.utc).date().isoformat()
    with get_connection() as conn:
        total = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_requests WHERE status <> ?",
                (ORDER_STATUS_DELETED,),
            ).fetchone()[0]
        )
        pending = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM order_requests
                WHERE status IN (?, ?)
                  AND status <> ?
                """,
                (
                    ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE,
                    ORDER_STATUS_AWAITING_SIGNATURE,
                    ORDER_STATUS_DELETED,
                ),
            ).fetchone()[0]
        )
        negative = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_requests WHERE status = ? AND status <> ?",
                (ORDER_STATUS_NEGATIVE, ORDER_STATUS_DELETED),
            ).fetchone()[0]
        )
        returned = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_requests WHERE status = ? AND status <> ?",
                (ORDER_STATUS_RETURNED, ORDER_STATUS_DELETED),
            ).fetchone()[0]
        )
        done = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_requests WHERE status = ? AND status <> ?",
                (ORDER_STATUS_DONE, ORDER_STATUS_DELETED),
            ).fetchone()[0]
        )
        billed = int(
            conn.execute(
                "SELECT COUNT(*) FROM order_requests WHERE status = ? AND status <> ?",
                (ORDER_STATUS_BILLED, ORDER_STATUS_DELETED),
            ).fetchone()[0]
        )
        signed_today = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM order_requests
                WHERE signed_at IS NOT NULL
                  AND status <> ?
                  AND substr(signed_at, 1, 10) = ?
                """,
                (ORDER_STATUS_DELETED, today_iso),
            ).fetchone()[0]
        )

    return {
        "total": total,
        "pendingSignature": pending,
        "negativeNoLimit": negative,
        "returnedForReview": returned,
        "done": done,
        "billed": billed,
        "signedToday": signed_today,
    }


@app.get("/api/pedidos/comprovantes")
def pedidos_comprovantes_financeiros(
    customer: str | None = None,
    dateFrom: str | None = None,
    dateTo: str | None = None,
    limit: int = 300,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_financial_receipts_access(user)

    filters: list[str] = ["o.status IN (?, ?)"]
    params: list[object] = [ORDER_STATUS_DONE, ORDER_STATUS_BILLED]

    if customer:
        filters.append("o.customer_name LIKE ?")
        params.append(f"%{customer.strip()}%")
    if dateFrom:
        filters.append("substr(o.created_at, 1, 10) >= ?")
        params.append(str(dateFrom))
    if dateTo:
        filters.append("substr(o.created_at, 1, 10) <= ?")
        params.append(str(dateTo))

    where_sql = f"WHERE {' AND '.join(filters)}"
    capped_limit = max(1, min(int(limit), 20000))
    params.append(capped_limit)

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT
                o.*,
                cons.name AS consultant_name,
                req.name AS requested_by_name,
                signer.name AS signed_by_name
            FROM order_requests o
            JOIN consultants cons ON cons.id = o.consultant_id
            JOIN consultants req ON req.id = o.requested_by_user_id
            LEFT JOIN consultants signer ON signer.id = o.signed_by_user_id
            {where_sql}
            ORDER BY o.updated_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

    items: list[dict] = []
    for row in rows:
        payload = serialize_order_row(dict(row))
        payload["consultantName"] = row["consultant_name"]
        payload["requestedByName"] = row["requested_by_name"]
        payload["signedByName"] = row["signed_by_name"]
        items.append(payload)
    return {"items": items}


@app.post("/api/pedidos/{order_id}/assinar")
def pedidos_assinar(
    order_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    signature_mode = str(payload.get("signatureMode") or ORDER_SIGNATURE_MODE).strip().lower()
    if signature_mode not in {"canvas", "hash"}:
        raise HTTPException(status_code=400, detail="Modo de assinatura invalido.")

    canvas_payload = payload.get("signatureCanvasBase64")
    signature_canvas_bytes = decode_canvas_signature(canvas_payload)
    if signature_mode == "canvas":
        if not signature_canvas_bytes:
            raise HTTPException(status_code=400, detail="Assinatura manuscrita obrigatoria no modo canvas.")
    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        current_status = str(order_row["status"] or "").strip()
        if current_status == ORDER_STATUS_LEGACY_AWAITING_FINANCIAL_SIGNATURE:
            current_status = ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE
        next_status = next_status_for_signature(current_status)
        if not next_status:
            raise HTTPException(status_code=409, detail="Pedido nao esta em etapa de assinatura.")

        stage_code = ""
        stage_label = ""
        next_label = status_stage_label(next_status) if next_status else ""
        is_commercial_stage_signature = current_status in {
            ORDER_STATUS_AWAITING_COMMERCIAL_SIGNATURE,
            ORDER_STATUS_NEGATIVE,
            ORDER_STATUS_RETURNED,
        }
        if is_commercial_stage_signature:
            ensure_commercial_signature_user(user)
            stage_code = "comercial"
            stage_label = "Diretor Comercial"
        elif current_status == ORDER_STATUS_AWAITING_SIGNATURE:
            ensure_isabel(user)
            stage_code = "isabel"
            stage_label = "Isabel"
        else:
            raise HTTPException(status_code=409, detail="Pedido nao esta em etapa valida de assinatura.")

        signature_image_path: Path | None = None
        if signature_mode == "canvas":
            signature_image_path = ORDER_SIGNATURE_DIR / f"{order_id}-{stage_code}-{uuid.uuid4().hex[:8]}.png"
            signature_image_path.write_bytes(signature_canvas_bytes or b"")

        source_pdf_path = Path(order_row["signed_pdf_path"]) if order_row.get("signed_pdf_path") else None
        if source_pdf_path is None or not source_pdf_path.exists():
            source_pdf_path = Path(order_row["original_pdf_path"])
        if not source_pdf_path.exists():
            raise HTTPException(status_code=404, detail="PDF base do pedido nao encontrado para assinatura.")

        signed_at_iso = utc_now_iso()
        signature_hash = hash_order_signature(
            pdf_bytes=source_pdf_path.read_bytes(),
            order_number=str(order_row["order_number"]),
            signed_by=f"{stage_label}:{user.name}",
            signed_at_iso=signed_at_iso,
        )

        distribution = normalize_order_distribution(order_row.get("distribution_json"))
        approvals = list(distribution.get("approvals") or [])
        notifications = list(distribution.get("notifications") or [])
        approval_entry = {
            "stage": stage_code,
            "stageLabel": stage_label,
            "roleLabel": stage_label,
            "signedByUserId": int(user.id),
            "signedByName": user.name,
            "signedAt": signed_at_iso,
            "signatureMode": signature_mode,
            "signatureHash": signature_hash,
            "signatureCanvasPath": signature_image_path.as_posix() if signature_image_path else None,
        }
        approvals.append(approval_entry)
        distribution["approvals"] = approvals

        config = ensure_order_email_config(conn)
        failed_notifications = 0
        final_size = 0
        signed_pdf_path: Path | None = None
        final_status = next_status
        stage_message = f"Assinado por {stage_label}. Aguardando {next_label}."

        package_pdf_path = Path(order_row["package_pdf_path"]) if order_row.get("package_pdf_path") else None
        stage_attachment = package_pdf_path if package_pdf_path and package_pdf_path.exists() else Path(
            order_row["original_pdf_path"]
        )

        def send_and_log(kind: str, recipients: list[str], subject: str, body: str, pdf_path: Path | None) -> None:
            nonlocal failed_notifications
            for recipient in recipients:
                success, error_message = send_email_with_optional_pdf(
                    recipient=recipient,
                    subject=subject,
                    body=body,
                    pdf_path=pdf_path,
                )
                notifications.append(
                    {
                        "kind": kind,
                        "to": recipient,
                        "success": success,
                        "error": error_message,
                    }
                )
                if not success:
                    failed_notifications += 1
                record_order_email_log(
                    conn,
                    order_request_id=order_id,
                    email_kind=kind,
                    recipient=recipient,
                    subject=subject,
                    success=success,
                    error_message=error_message,
                )

        if is_commercial_stage_signature:
            original_pdf_path = Path(order_row["original_pdf_path"])
            signed_pdf_path = ORDER_SIGNED_DIR / f"{order_row['external_id']}-signed.pdf"
            _, final_size = generate_signed_order_pdf(
                original_pdf_path=original_pdf_path,
                signed_pdf_path=signed_pdf_path,
                signature_image_path=signature_image_path,
                order_number=order_row["order_number"],
                customer_name=order_row["customer_name"],
                order_value_cents=int(order_row["order_value_cents"]),
                signed_by_name=user.name,
                signature_mode=signature_mode,
                signed_at_iso=signed_at_iso,
                signature_hash=signature_hash,
                approvals=approvals,
            )

            attachment_for_isabel = (
                signed_pdf_path
                if signed_pdf_path.exists()
                else (stage_attachment if stage_attachment.exists() else None)
            )
            recipients = split_email_targets(config.get("isabel_emails"))
            send_and_log(
                "request_isabel_signature",
                recipients,
                f"[Esteira assinatura] Isabel - Pedido {order_row['order_number']}",
                (
                    "Diretor Comercial concluiu a assinatura. Pedido segue para assinatura final da Isabel.\n\n"
                    f"Pedido: {order_row['order_number']}\n"
                    f"Cliente: {order_row['customer_name']}\n"
                    f"Valor: R$ {cents_to_brl(int(order_row['order_value_cents'])):,.2f}\n"
                    f"Proxima etapa: {status_stage_label(ORDER_STATUS_AWAITING_SIGNATURE)}\n\n"
                    f"Painel de Status: {ORDER_APPROVAL_PANEL_URL}\n"
                ),
                attachment_for_isabel,
            )
        else:
            original_pdf_path = Path(order_row["original_pdf_path"])
            signed_pdf_path = ORDER_SIGNED_DIR / f"{order_row['external_id']}-signed.pdf"
            _, final_size = generate_signed_order_pdf(
                original_pdf_path=original_pdf_path,
                signed_pdf_path=signed_pdf_path,
                signature_image_path=signature_image_path,
                order_number=order_row["order_number"],
                customer_name=order_row["customer_name"],
                order_value_cents=int(order_row["order_value_cents"]),
                signed_by_name=user.name,
                signature_mode=signature_mode,
                signed_at_iso=signed_at_iso,
                signature_hash=signature_hash,
                approvals=approvals,
            )

            requester_email = get_requester_email(conn, int(order_row["requested_by_user_id"]))
            vitor_targets = split_email_targets(config.get("vitor_emails"))
            marcos_targets = split_email_targets(config.get("marcos_emails"))
            isabel_targets = split_email_targets(config.get("isabel_emails"))
            if user.email:
                isabel_targets.extend(split_email_targets(user.email))

            via3_targets = marcos_targets.copy()
            if requester_email:
                via3_targets.append(requester_email)
            via3_targets = list(dict.fromkeys(via3_targets))
            isabel_targets = list(dict.fromkeys(isabel_targets))

            common_body = (
                "Pedido finalizado com assinatura em esteira.\n\n"
                f"Pedido: {order_row['order_number']}\n"
                f"Cliente: {order_row['customer_name']}\n"
                f"Valor: R$ {cents_to_brl(int(order_row['order_value_cents'])):,.2f}\n"
                f"Finalizado por: {user.name}\n"
                f"Data assinatura final: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
            )
            send_and_log(
                "via1_vitor",
                vitor_targets,
                f"[Via 1/4] Pedido finalizado {order_row['order_number']}",
                "Primeira via digital do pedido finalizado.\n\n" + common_body,
                signed_pdf_path,
            )
            send_and_log(
                "via2_vitor",
                vitor_targets,
                f"[Via 2/4] Confirmacao pedido {order_row['order_number']}",
                "Segunda via digital para confirmacao de recebimento.\n\n" + common_body,
                signed_pdf_path,
            )
            send_and_log(
                "via3_marcos_admin",
                via3_targets,
                f"[Via 3/4] Pedido finalizado {order_row['order_number']}",
                "Terceira via digital destinada a Marcos e administrador iniciador.\n\n" + common_body,
                signed_pdf_path,
            )
            send_and_log(
                "via4_isabel",
                isabel_targets,
                f"[Via 4/4] Confirmacao assinatura {order_row['order_number']}",
                "Quarta via: confirmacao do fechamento digital do pedido.\n\n" + common_body,
                signed_pdf_path,
            )

            final_status = ORDER_STATUS_DONE
            stage_message = (
                "Pedido finalizado com todas as assinaturas e concluido para faturamento."
                if failed_notifications == 0
                else "Pedido concluido para faturamento, com falhas parciais no envio de e-mails."
            )

        distribution["notifications"] = notifications
        now_iso = utc_now_iso()
        conn.execute(
            """
            UPDATE order_requests
            SET
                status = ?,
                status_reason = ?,
                signed_pdf_path = ?,
                signature_mode = ?,
                signature_hash = ?,
                signature_canvas_path = ?,
                signed_by_user_id = ?,
                signed_at = ?,
                distribution_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                final_status,
                stage_message,
                signed_pdf_path.as_posix() if signed_pdf_path else order_row.get("signed_pdf_path"),
                signature_mode,
                signature_hash,
                signature_image_path.as_posix() if signature_image_path else None,
                int(user.id),
                signed_at_iso,
                json.dumps(distribution, ensure_ascii=False),
                now_iso,
                int(order_id),
            ),
        )
        record_order_event(
            conn,
            order_request_id=order_id,
            event_type="SIGNED_STAGE",
            actor_user_id=int(user.id),
            actor_name=user.name,
            from_status=current_status,
            to_status=final_status,
            message=stage_message,
            payload={
                "stage": stage_code,
                "stageLabel": stage_label,
                "nextStage": status_stage_label(final_status),
                "signatureMode": signature_mode,
                "signedPdfSizeBytes": final_size if final_size > 0 else None,
                "failedEmailCount": failed_notifications,
            },
        )
        conn.commit()
        updated_row = fetch_order_row_or_404(conn, order_id)

    return {
        "order": serialize_order_row(updated_row),
        "downloadUrl": f"/api/pedidos/{order_id}/download",
        "signatureMode": signature_mode,
        "failedEmails": failed_notifications,
        "billed": False,
        "nextStatus": updated_row["status"],
        "nextStageLabel": status_stage_label(updated_row["status"]),
    }


@app.post("/api/pedidos/{order_id}/assinar-concluir")
def pedidos_assinar_concluir(
    order_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    sign_result = pedidos_assinar(order_id=order_id, payload=payload, authorization=authorization)
    billed = bool(sign_result.get("billed"))

    return {
        "order": sign_result.get("order"),
        "downloadUrl": sign_result.get("downloadUrl"),
        "signatureMode": sign_result.get("signatureMode"),
        "failedEmails": int(sign_result.get("failedEmails") or 0),
        "billed": billed,
        "nextStatus": sign_result.get("nextStatus"),
        "nextStageLabel": sign_result.get("nextStageLabel"),
    }


@app.post("/api/pedidos/{order_id}/assinar-preview")
def pedidos_assinar_preview(
    order_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
):
    user = get_current_user_from_header(authorization)
    signature_canvas_bytes = decode_canvas_signature(payload.get("signatureCanvasBase64"))
    if not signature_canvas_bytes:
        raise HTTPException(status_code=400, detail="Desenhe a assinatura antes de gerar o PDF.")

    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        ensure_order_access(user, order_row)
        if order_row["status"] == ORDER_STATUS_DELETED:
            raise HTTPException(status_code=409, detail="Solicitacao excluida nao pode gerar assinatura manual.")

        signed_path = Path(order_row["signed_pdf_path"]) if order_row.get("signed_pdf_path") else None
        source_pdf_path = signed_path if signed_path and signed_path.exists() else Path(order_row["original_pdf_path"])
        if not source_pdf_path.exists():
            raise HTTPException(status_code=404, detail="PDF base do pedido nao encontrado.")

        signature_image_path = ORDER_SIGNATURE_DIR / f"preview-{order_id}-{uuid.uuid4().hex[:8]}.png"
        signature_image_path.write_bytes(signature_canvas_bytes)
        preview_pdf_path = ORDER_SIGNED_DIR / f"{order_row['external_id']}-preview-{uuid.uuid4().hex[:8]}.pdf"
        try:
            generate_signed_order_pdf(
                original_pdf_path=source_pdf_path,
                signed_pdf_path=preview_pdf_path,
                signature_image_path=signature_image_path,
                order_number=order_row["order_number"],
                customer_name=order_row["customer_name"],
                order_value_cents=int(order_row["order_value_cents"]),
                signed_by_name=user.name,
                signature_mode="canvas",
            )
        finally:
            signature_image_path.unlink(missing_ok=True)

        record_order_event(
            conn,
            order_request_id=int(order_id),
            event_type="MANUAL_SIGNATURE_PREVIEW",
            actor_user_id=int(user.id),
            actor_name=user.name,
            from_status=order_row["status"],
            to_status=order_row["status"],
            message="PDF gerado com assinatura manual sem alterar o status.",
            payload={"previewPdfPath": preview_pdf_path.as_posix()},
        )
        conn.commit()

    filename = f"{order_row['order_number']}-assinado-manual.pdf"
    return FileResponse(path=preview_pdf_path, media_type="application/pdf", filename=filename)


@app.post("/api/pedidos/{order_id}/faturar")
def pedidos_faturar(
    order_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)
    note = str(payload.get("note") or "").strip()

    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        if order_row["status"] == ORDER_STATUS_DELETED:
            raise HTTPException(status_code=409, detail="Solicitacao excluida nao pode ser faturada.")
        if order_row["status"] == ORDER_STATUS_BILLED:
            return {"order": serialize_order_row(order_row)}
        if order_row["status"] != ORDER_STATUS_DONE:
            raise HTTPException(
                status_code=409,
                detail="Somente pedidos concluidos podem receber baixa de faturado.",
            )

        status_reason = f"Pedido faturado por {user.name}."
        if note:
            status_reason = f"{status_reason} Obs: {note}"

        conn.execute(
            """
            UPDATE order_requests
            SET
                status = ?,
                status_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                ORDER_STATUS_BILLED,
                status_reason,
                utc_now_iso(),
                int(order_id),
            ),
        )
        record_order_event(
            conn,
            order_request_id=int(order_id),
            event_type="BILLED",
            actor_user_id=int(user.id),
            actor_name=user.name,
            from_status=order_row["status"],
            to_status=ORDER_STATUS_BILLED,
            message=status_reason,
            payload={"note": note or None},
        )
        conn.commit()
        updated = fetch_order_row_or_404(conn, order_id)

    return {"order": serialize_order_row(updated)}


@app.post("/api/pedidos/{order_id}/devolver")
def pedidos_devolver(
    order_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_isabel(user)

    reason = str(payload.get("reason") or payload.get("justificativa") or "").strip()
    if len(reason) < 5:
        raise HTTPException(status_code=400, detail="Justificativa obrigatoria com no minimo 5 caracteres.")

    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        if order_row["status"] != ORDER_STATUS_AWAITING_SIGNATURE:
            raise HTTPException(status_code=409, detail="Pedido nao esta pendente para devolucao.")
        conn.execute(
            """
            UPDATE order_requests
            SET
                status = ?,
                returned_reason = ?,
                status_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                ORDER_STATUS_RETURNED,
                reason,
                "Pedido devolvido para revisao pela Isabel.",
                utc_now_iso(),
                int(order_id),
            ),
        )
        record_order_event(
            conn,
            order_request_id=order_id,
            event_type="RETURNED",
            actor_user_id=int(user.id),
            actor_name=user.name,
            from_status=order_row["status"],
            to_status=ORDER_STATUS_RETURNED,
            message=reason,
        )
        conn.commit()
        updated = fetch_order_row_or_404(conn, order_id)

    return {"order": serialize_order_row(updated)}


@app.delete("/api/pedidos/{order_id}")
def pedidos_excluir(
    order_id: int,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)

    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        if order_row["status"] == ORDER_STATUS_DELETED:
            raise HTTPException(status_code=409, detail="Solicitacao ja foi excluida.")
        if order_row["status"] in {
            ORDER_STATUS_AWAITING_SIGNATURE,
            ORDER_STATUS_SIGNED_DISTRIBUTING,
            ORDER_STATUS_DONE,
            ORDER_STATUS_BILLED,
        } or order_row.get("signed_at"):
            raise HTTPException(
                status_code=409,
                detail="Pedidos com assinatura registrada nao podem ser excluidos para preservar historico e auditoria.",
            )

        now_iso = utc_now_iso()
        conn.execute(
            """
            UPDATE order_requests
            SET
                status = ?,
                status_reason = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                ORDER_STATUS_DELETED,
                f"Solicitacao excluida manualmente por {user.name}.",
                now_iso,
                int(order_id),
            ),
        )
        record_order_event(
            conn,
            order_request_id=int(order_id),
            event_type="DELETED",
            actor_user_id=int(user.id),
            actor_name=user.name,
            from_status=order_row["status"],
            to_status=ORDER_STATUS_DELETED,
            message="Solicitacao removida da fila de acompanhamento.",
        )
        conn.commit()
        updated = fetch_order_row_or_404(conn, order_id)

    return {"order": serialize_order_row(updated)}


@app.get("/api/pedidos/{order_id}/status")
def pedidos_status_by_id(
    order_id: int,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        ensure_order_access(user, order_row)
        events = conn.execute(
            """
            SELECT event_type, actor_name, from_status, to_status, message, payload_json, created_at
            FROM order_request_events
            WHERE order_request_id = ?
            ORDER BY id DESC
            """,
            (int(order_id),),
        ).fetchall()

    return {
        "order": serialize_order_row(order_row),
        "events": [
            {
                "eventType": row["event_type"],
                "actorName": row["actor_name"],
                "fromStatus": row["from_status"],
                "toStatus": row["to_status"],
                "message": row["message"],
                "payload": json.loads(row["payload_json"] or "{}"),
                "createdAt": row["created_at"],
            }
            for row in events
        ],
    }


@app.get("/api/pedidos/{order_id}/download")
def pedidos_download(
    order_id: int,
    authorization: str | None = Header(default=None),
):
    user = get_current_user_from_header(authorization)
    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        ensure_order_access(user, order_row)

        signed_path = Path(order_row["signed_pdf_path"]) if order_row.get("signed_pdf_path") else None
        needs_stage_preview = (
            order_row.get("status") == ORDER_STATUS_AWAITING_SIGNATURE
            and (signed_path is None or not signed_path.exists())
        )
        if needs_stage_preview:
            distribution = normalize_order_distribution(order_row.get("distribution_json"))
            approvals = list(distribution.get("approvals") or [])
            if approvals:
                last_approval = approvals[-1]
                signature_path_raw = str(last_approval.get("signatureCanvasPath") or "").strip()
                signature_image_path = Path(signature_path_raw) if signature_path_raw else None
                if signature_image_path is not None and not signature_image_path.exists():
                    signature_image_path = None

                stage_signed_path = ORDER_SIGNED_DIR / f"{order_row['external_id']}-signed.pdf"
                generate_signed_order_pdf(
                    original_pdf_path=Path(order_row["original_pdf_path"]),
                    signed_pdf_path=stage_signed_path,
                    signature_image_path=signature_image_path,
                    order_number=str(order_row["order_number"] or ""),
                    customer_name=str(order_row["customer_name"] or ""),
                    order_value_cents=int(order_row["order_value_cents"] or 0),
                    signed_by_name=str(last_approval.get("signedByName") or "Diretor Comercial"),
                    signature_mode=str(last_approval.get("signatureMode") or order_row.get("signature_mode") or "hash"),
                    signed_at_iso=str(last_approval.get("signedAt") or order_row.get("signed_at") or utc_now_iso()),
                    signature_hash=str(last_approval.get("signatureHash") or order_row.get("signature_hash") or ""),
                    approvals=approvals,
                )
                conn.execute(
                    """
                    UPDATE order_requests
                    SET signed_pdf_path = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (stage_signed_path.as_posix(), utc_now_iso(), int(order_id)),
                )
                conn.commit()
                order_row["signed_pdf_path"] = stage_signed_path.as_posix()

    signed_path = Path(order_row["signed_pdf_path"]) if order_row.get("signed_pdf_path") else None
    file_path = signed_path if signed_path and signed_path.exists() else Path(order_row["original_pdf_path"])
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo PDF nao encontrado.")

    filename = (
        f"{order_row['order_number']}-assinado.pdf"
        if signed_path and signed_path.exists()
        else f"{order_row['order_number']}-original.pdf"
    )
    return FileResponse(path=file_path, media_type="application/pdf", filename=filename)


@app.get("/api/pedidos/{order_id}/download-analise")
def pedidos_download_analise(
    order_id: int,
    authorization: str | None = Header(default=None),
):
    user = get_current_user_from_header(authorization)
    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        ensure_order_access(user, order_row)

        analysis_path_raw = str(order_row.get("analysis_pdf_path") or "").strip()
        analysis_pdf_path = Path(analysis_path_raw) if analysis_path_raw else None

        if analysis_pdf_path is None or not analysis_pdf_path.exists():
            receivable_rows, consultant_name = fetch_order_customer_receivables(
                conn,
                consultant_id=int(order_row["consultant_id"]),
                customer_code=order_row.get("customer_code"),
                customer_name=order_row.get("customer_name"),
            )
            credit_snapshot = {
                "openBalanceCents": int(order_row.get("open_balance_cents") or 0),
                "creditLimitCents": int(order_row.get("credit_limit_cents") or 0),
                "overLimitCents": int(order_row.get("over_limit_cents") or 0),
            }
            analysis_pdf_path = generate_client_analysis_attachment_pdf(
                external_id=str(order_row["external_id"]),
                consultant_name=consultant_name,
                customer_name=str(order_row["customer_name"] or ""),
                customer_code=order_row.get("customer_code"),
                order_number=str(order_row["order_number"] or ""),
                order_value_cents=int(order_row.get("order_value_cents") or 0),
                credit_snapshot=credit_snapshot,
                receivable_rows=receivable_rows,
            )
            conn.execute(
                """
                UPDATE order_requests
                SET analysis_pdf_path = ?, updated_at = ?
                WHERE id = ?
                """,
                (analysis_pdf_path.as_posix(), utc_now_iso(), int(order_id)),
            )
            record_order_event(
                conn,
                order_request_id=int(order_id),
                event_type="ANALYSIS_ATTACHMENT_GENERATED",
                actor_user_id=int(user.id),
                actor_name=user.name,
                from_status=order_row["status"],
                to_status=order_row["status"],
                message="Anexo de analise financeira gerado para visualizacao.",
                payload={"analysisPdfPath": analysis_pdf_path.as_posix(), "onDemand": True},
            )
            conn.commit()

    if analysis_pdf_path is None or not analysis_pdf_path.exists():
        raise HTTPException(status_code=404, detail="Anexo de saude financeira nao encontrado para este pedido.")

    filename = f"{order_row['order_number']}-analise-cliente.pdf"
    return FileResponse(path=analysis_pdf_path, media_type="application/pdf", filename=filename)


@app.get("/api/pedidos/{order_id}/assinatura")
def pedidos_assinatura_download(
    order_id: int,
    authorization: str | None = Header(default=None),
):
    user = get_current_user_from_header(authorization)
    with get_connection() as conn:
        order_row = fetch_order_row_or_404(conn, order_id)
        ensure_order_access(user, order_row)

    signature_path_raw = str(order_row.get("signature_canvas_path") or "").strip()
    signature_path = Path(signature_path_raw) if signature_path_raw else None
    if signature_path and signature_path.exists():
        filename = f"{order_row['order_number']}-assinatura.png"
        return FileResponse(path=signature_path, media_type="image/png", filename=filename)

    if str(order_row.get("signature_mode") or "").strip().lower() == "hash":
        raise HTTPException(
            status_code=404,
            detail="Assinatura registrada em modo hash (sem desenho manual). Consulte o PDF assinado.",
        )
    raise HTTPException(status_code=404, detail="Assinatura manual registrada nao encontrada.")


@app.get("/api/pedidos/config")
def pedidos_config_get(
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_status_access(user)
    with get_connection() as conn:
        config = ensure_order_email_config(conn)
        updated_by = (
            conn.execute(
                "SELECT name FROM consultants WHERE id = ?",
                (config["updated_by_user_id"],),
            ).fetchone()
            if config.get("updated_by_user_id")
            else None
        )

    return {
        "config": {
            "isabelEmails": split_email_targets(config.get("isabel_emails")),
            "vitorEmails": split_email_targets(config.get("vitor_emails")),
            "marcosEmails": split_email_targets(config.get("marcos_emails")),
            "updatedBy": updated_by["name"] if updated_by else None,
            "updatedAt": config.get("updated_at"),
            "createdAt": config.get("created_at"),
        }
    }


@app.put("/api/pedidos/config")
def pedidos_config_update(
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)

    def join_payload_emails(key: str) -> str:
        raw_value = payload.get(key, [])
        if isinstance(raw_value, list):
            return ",".join(str(item) for item in raw_value)
        return str(raw_value or "")

    isabel_emails = normalize_email_targets(join_payload_emails("isabelEmails"))
    vitor_emails = normalize_email_targets(join_payload_emails("vitorEmails"))
    marcos_emails = normalize_email_targets(join_payload_emails("marcosEmails"))
    if not isabel_emails or not vitor_emails or not marcos_emails:
        raise HTTPException(status_code=400, detail="Isabel, Vitor e Marcos precisam de ao menos um e-mail.")

    with get_connection() as conn:
        _ = ensure_order_email_config(conn)
        conn.execute(
            """
            UPDATE order_email_config
            SET
                isabel_emails = ?,
                vitor_emails = ?,
                marcos_emails = ?,
                updated_by_user_id = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                isabel_emails,
                vitor_emails,
                marcos_emails,
                int(user.id),
                utc_now_iso(),
            ),
        )
        conn.commit()
        updated = ensure_order_email_config(conn)

    return {
        "config": {
            "isabelEmails": split_email_targets(updated.get("isabel_emails")),
            "vitorEmails": split_email_targets(updated.get("vitor_emails")),
            "marcosEmails": split_email_targets(updated.get("marcos_emails")),
            "updatedAt": updated.get("updated_at"),
        }
    }


@app.get("/api/pedidos/admin-emails")
def pedidos_admin_emails(
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)
    return {"items": list_active_admin_users()}


@app.put("/api/pedidos/admin-emails/{consultant_id}")
def pedidos_admin_email_update(
    consultant_id: int,
    payload: dict,
    authorization: str | None = Header(default=None),
) -> dict:
    user = get_current_user_from_header(authorization)
    ensure_operational_user(user)

    consultant = get_consultant_by_id(int(consultant_id))
    if not consultant or not consultant.get("is_admin"):
        raise HTTPException(status_code=404, detail="Administrador nao encontrado.")

    email = normalize_email_value(payload.get("email"))
    update_consultant_email(consultant_id=int(consultant_id), email=email)
    updated = get_consultant_by_id(int(consultant_id))
    return {
        "item": {
            "id": int(updated["id"]),
            "name": updated["name"],
            "username": updated["username"],
            "email": updated["email"],
        }
    }


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

