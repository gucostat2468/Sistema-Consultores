from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable
import re
import unicodedata

from src.excel_sheet_loader import load_excel_sheets, normalize_loose_key
from src.pdf_parser import ReceivableRecord


@dataclass(frozen=True)
class ExcelReceivableSheetSummary:
    sheet_name: str
    rows_scanned: int
    candidate_rows: int
    records_count: int
    skipped_rows_count: int
    consultants_count: int
    receivable_section_detected: bool
    header_signature: str | None
    detected_columns: list[str]


@dataclass(frozen=True)
class ExcelReceivablesParseReport:
    records: list[ReceivableRecord]
    vendors: list[str]
    skipped_rows: list[str]
    sheet_name: str
    rows_scanned: int
    candidate_rows: int
    workbook_sheets: list[str]
    scanned_sheets: list[str]
    processed_sheets: list[str]
    sheet_summaries: list[ExcelReceivableSheetSummary]


@dataclass(frozen=True)
class _SheetParseResult:
    records: list[ReceivableRecord]
    vendors: list[str]
    skipped_rows: list[str]
    rows_scanned: int
    candidate_rows: int
    receivable_section_detected: bool
    header_signature: str | None
    detected_columns: list[str]


NF_RE = re.compile(r"(?:NT|NF|DOC)\s*[:\-]?\s*([A-Za-z0-9./-]+)(?:\s+(\d+/\d+))?", re.IGNORECASE)
DIGITS_RE = re.compile(r"\d{5,8}")

CUSTOMER_HINT_STOPWORDS = {
    "LTDA",
    "EPP",
    "EIRELI",
    "SA",
    "S A",
    "ME",
    "DE",
    "DO",
    "DA",
    "DOS",
    "DAS",
    "COMERCIO",
    "SERVICO",
    "SERVICOS",
    "EQUIPAMENTOS",
    "DISTRIBUIDORA",
    "AGRICOLA",
    "AGRICOLAS",
}


HEADER_ALIASES: dict[str, set[str]] = {
    "dueDate": {
        "vencimento",
        "venc",
        "vcto",
        "vencto",
        "dt vcto",
        "dt vencto",
        "data",
        "data de vencimento",
        "vencto",
        "data vencimento",
        "dt vencimento",
    },
    "documentRef": {
        "nf",
        "nt",
        "nf/nt",
        "nota fiscal",
        "titulo",
        "duplicata",
        "documento",
        "doc",
        "numero documento",
    },
    "customerName": {
        "cliente",
        "nome cliente",
        "sacado",
        "devedor",
        "nome",
        "razao social",
        "empresa",
        "subdealer/cliente",
        "subdealer",
    },
    "consultantName": {
        "consultor",
        "vendedor",
        "representante",
    },
    "customerCode": {
        "codigo",
        "cod",
        "cod sacado",
        "cod cliente",
        "codigo cliente",
        "id cliente",
        "id documento",
    },
    "installmentValue": {
        "valor",
        "vl",
        "valor parcela",
        "valor da parcela",
        "valor parcelar",
        "vlr parcela",
        "valor titulo",
    },
    "balance": {
        "saldo",
        "saldo aberto",
        "valor aberto",
        "saldo em aberto",
        "valor",
    },
    "issueDate": {
        "emissao",
        "data emissao",
        "dt emissao",
    },
    "status": {
        "situacao",
        "status",
    },
    "installment": {
        "parcela",
    },
}


def parse_receivables_excel(
    path: str | Path,
    *,
    sheet_name_hint: str = "moviment",
    parse_all_sheets: bool = True,
    customer_hints: dict[str, dict[str, str | None]] | None = None,
    document_hints: dict[str, str] | None = None,
    default_consultant_name: str | None = None,
) -> ExcelReceivablesParseReport:
    excel_path = Path(path)
    sheet_map = load_excel_sheets(excel_path)
    workbook_sheets = list(sheet_map.keys())
    primary_sheet = resolve_sheet_name(workbook_sheets, sheet_name_hint)
    default_snapshot = datetime.fromtimestamp(excel_path.stat().st_mtime).isoformat()

    if parse_all_sheets:
        scanned_sheets = [primary_sheet] + [
            sheet_name for sheet_name in workbook_sheets if sheet_name != primary_sheet
        ]
    else:
        scanned_sheets = [primary_sheet]

    records: list[ReceivableRecord] = []
    skipped_rows: list[str] = []
    vendors_seen: dict[str, None] = {}
    candidate_rows_total = 0
    rows_scanned_total = 0
    processed_sheets: list[str] = []
    sheet_summaries: list[ExcelReceivableSheetSummary] = []

    for sheet_name in scanned_sheets:
        worksheet = sheet_map[sheet_name]
        sheet_result = parse_receivable_worksheet(
            worksheet,
            sheet_name=sheet_name,
            default_snapshot=default_snapshot,
            customer_hints=customer_hints or {},
            document_hints=document_hints or {},
            default_consultant_name=default_consultant_name,
        )

        records.extend(sheet_result.records)
        skipped_rows.extend(sheet_result.skipped_rows)
        candidate_rows_total += sheet_result.candidate_rows
        rows_scanned_total += sheet_result.rows_scanned
        for vendor_name in sheet_result.vendors:
            vendors_seen[vendor_name] = None

        if sheet_result.receivable_section_detected:
            processed_sheets.append(sheet_name)

        sheet_summaries.append(
            ExcelReceivableSheetSummary(
                sheet_name=sheet_name,
                rows_scanned=sheet_result.rows_scanned,
                candidate_rows=sheet_result.candidate_rows,
                records_count=len(sheet_result.records),
                skipped_rows_count=len(sheet_result.skipped_rows),
                consultants_count=len(sheet_result.vendors),
                receivable_section_detected=sheet_result.receivable_section_detected,
                header_signature=sheet_result.header_signature,
                detected_columns=sheet_result.detected_columns,
            )
        )

    return ExcelReceivablesParseReport(
        records=records,
        vendors=list(vendors_seen.keys()),
        skipped_rows=skipped_rows,
        sheet_name=primary_sheet,
        rows_scanned=rows_scanned_total,
        candidate_rows=candidate_rows_total,
        workbook_sheets=workbook_sheets,
        scanned_sheets=scanned_sheets,
        processed_sheets=processed_sheets,
        sheet_summaries=sheet_summaries,
    )


def parse_receivable_worksheet(
    worksheet,
    *,
    sheet_name: str,
    default_snapshot: str,
    customer_hints: dict[str, dict[str, str | None]],
    document_hints: dict[str, str],
    default_consultant_name: str | None,
) -> _SheetParseResult:
    records: list[ReceivableRecord] = []
    skipped_rows: list[str] = []
    vendors_seen: dict[str, None] = {}
    candidate_rows = 0
    receivable_section_detected = False
    header_signature: str | None = None
    column_map: dict[str, int] = {}

    max_columns = min(max(int(getattr(worksheet, "max_column", 1)), 1), 40)
    for row_index in range(1, worksheet.max_row + 1):
        row_values = [worksheet.cell(row_index, col_index).value for col_index in range(1, max_columns + 1)]
        if is_empty_row(row_values):
            continue

        if not receivable_section_detected:
            detected = detect_receivables_header_row(row_values)
            if detected:
                receivable_section_detected = True
                header_signature = detected["signature"]
                column_map = detected["columns"]
                continue
            inferred = infer_headerless_column_map(row_values)
            if inferred:
                receivable_section_detected = True
                header_signature = "headerless_auto_v1"
                column_map = inferred
            else:
                continue

        if is_totals_row(row_values):
            continue

        if row_is_mostly_empty(row_values):
            continue

        candidate_rows += 1
        parsed = parse_receivable_row(
            row_values=row_values,
            row_index=row_index,
            sheet_name=sheet_name,
            column_map=column_map,
            default_snapshot=default_snapshot,
            customer_hints=customer_hints,
            document_hints=document_hints,
            default_consultant_name=default_consultant_name,
        )
        if parsed is None:
            skipped_rows.append(f"{sheet_name}!r{row_index}: linha sem dados minimos para titulo")
            continue

        records.append(parsed)
        vendors_seen[parsed.vendor_name] = None

    return _SheetParseResult(
        records=records,
        vendors=list(vendors_seen.keys()),
        skipped_rows=skipped_rows,
        rows_scanned=worksheet.max_row,
        candidate_rows=candidate_rows,
        receivable_section_detected=receivable_section_detected,
        header_signature=header_signature,
        detected_columns=sorted(column_map.keys()),
    )


def infer_headerless_column_map(row_values: list[object]) -> dict[str, int] | None:
    date_idx: int | None = None
    amount_candidates: list[int] = []
    customer_candidates: list[tuple[int, int]] = []
    document_idx: int | None = None
    status_idx: int | None = None

    for idx, value in enumerate(row_values, start=1):
        text = clean_text(value)
        if not text:
            continue

        as_date = to_iso_date(value)
        if as_date and date_idx is None:
            date_idx = idx

        cents = parse_money_to_cents(value)
        if cents is not None and as_date is None:
            amount_candidates.append(idx)

        if document_idx is None and NF_RE.search(text):
            document_idx = idx

        lowered = text.lower()
        if status_idx is None and ("venc" in lowered or lowered in {"a vencer", "vencido"}):
            status_idx = idx

        if is_probable_customer_text(text, as_date is not None, cents is not None):
            customer_candidates.append((idx, len(text)))

    if not amount_candidates or not customer_candidates:
        return None

    amount_idx = next((idx for idx in amount_candidates if idx != date_idx), amount_candidates[0])
    customer_idx = None
    for idx, _ in sorted(customer_candidates, key=lambda item: item[1], reverse=True):
        if idx not in {date_idx, amount_idx}:
            customer_idx = idx
            break
    if customer_idx is None:
        return None

    mapped: dict[str, int] = {
        "customerName": customer_idx,
        "installmentValue": amount_idx,
        "balance": amount_idx,
    }
    if date_idx is not None:
        mapped["dueDate"] = date_idx
    if document_idx is not None and document_idx not in {date_idx, customer_idx, amount_idx}:
        mapped["documentRef"] = document_idx
    if status_idx is not None and status_idx not in {date_idx, customer_idx, amount_idx}:
        mapped["status"] = status_idx
    return mapped


def is_probable_customer_text(text: str, is_date: bool, is_numeric_money: bool) -> bool:
    if is_date or is_numeric_money:
        return False
    if len(text) < 3:
        return False
    if not any(ch.isalpha() for ch in text):
        return False
    if NF_RE.search(text):
        return False
    normalized = text.strip()
    if re.fullmatch(r"[\d\s./-]+", normalized):
        return False
    return True


def parse_receivable_row(
    *,
    row_values: list[object],
    row_index: int,
    sheet_name: str,
    column_map: dict[str, int],
    default_snapshot: str,
    customer_hints: dict[str, dict[str, str | None]],
    document_hints: dict[str, str],
    default_consultant_name: str | None,
) -> ReceivableRecord | None:
    customer_name = clean_text(value_at(row_values, column_map.get("customerName")))
    if not customer_name:
        return None

    hint = resolve_customer_hint(customer_name, customer_hints)

    document_ref_raw = clean_text(value_at(row_values, column_map.get("documentRef")))
    installment_raw = clean_text(value_at(row_values, column_map.get("installment")))
    document_ref, installment = parse_document_fields(document_ref_raw, installment_raw, row_index=row_index)
    document_id = extract_document_id(document_ref, fallback_row=row_index)

    customer_code = clean_text(value_at(row_values, column_map.get("customerCode")))
    if not customer_code:
        customer_code = str(hint.get("customerCode") or "")
    if not customer_code:
        customer_code = extract_customer_code(customer_name) or ""

    consultant_name = clean_text(value_at(row_values, column_map.get("consultantName")))
    if not consultant_name:
        consultant_name = str(hint.get("consultantName") or "")
    if not consultant_name and document_id in document_hints:
        consultant_name = normalize_spaces(str(document_hints[document_id]))
    if not consultant_name and default_consultant_name:
        consultant_name = normalize_spaces(default_consultant_name)
    if not consultant_name:
        return None

    due_date = to_iso_date(value_at(row_values, column_map.get("dueDate")))
    issue_date = to_iso_date(value_at(row_values, column_map.get("issueDate")))
    if not due_date and issue_date:
        due_date = issue_date
    if not due_date:
        due_date = date.today().isoformat()
    if not issue_date:
        issue_date = due_date
    balance_cents = parse_money_to_cents(value_at(row_values, column_map.get("balance")))
    installment_value_cents = parse_money_to_cents(value_at(row_values, column_map.get("installmentValue")))
    if balance_cents is None and installment_value_cents is None:
        return None

    if balance_cents is None:
        balance_cents = installment_value_cents
    if installment_value_cents is None:
        installment_value_cents = balance_cents

    note_number = document_ref

    status_text = clean_text(value_at(row_values, column_map.get("status")))
    status = normalize_status(status_text, due_date)

    raw_line = " | ".join(
        [
            str(value_at(row_values, column_map.get("dueDate")) or ""),
            document_ref_raw,
            customer_name,
            str(value_at(row_values, column_map.get("installmentValue")) or ""),
            str(value_at(row_values, column_map.get("balance")) or ""),
        ]
    )

    return ReceivableRecord(
        vendor_name=consultant_name,
        customer_name=customer_name,
        customer_code=customer_code,
        status=status,
        document_id=document_id,
        document_ref=document_ref,
        note_number=note_number,
        installment=installment,
        issue_date=issue_date,
        due_date=due_date,
        balance_cents=int(balance_cents),
        installment_value_cents=int(installment_value_cents),
        source_page=row_index,
        report_generated_at=default_snapshot,
        raw_line=f"{sheet_name}!r{row_index} | {raw_line}",
    )


def detect_receivables_header_row(row_values: list[object]) -> dict[str, object] | None:
    normalized_headers: list[str] = []
    for value in row_values:
        normalized_headers.append(normalize_key(clean_text(value)))

    mapped: dict[str, int] = {}
    for idx, header in enumerate(normalized_headers, start=1):
        if not header:
            continue
        for field, aliases in HEADER_ALIASES.items():
            if field in mapped:
                continue
            if any(normalize_key(alias) == header for alias in aliases):
                mapped[field] = idx
                break

    # Aceita layout minimo (cliente + valor), com vencimento opcional.
    required = {"customerName"}
    has_amount = "balance" in mapped or "installmentValue" in mapped
    if required.issubset(mapped.keys()) and has_amount:
        signature = "|".join([header for header in normalized_headers if header])
        return {
            "columns": mapped,
            "signature": signature,
        }
    return None


def parse_document_fields(document_ref_raw: str, installment_raw: str, *, row_index: int) -> tuple[str, str]:
    document_ref = document_ref_raw
    installment = installment_raw or "1/1"

    match = NF_RE.search(document_ref_raw)
    if match:
        document_ref = normalize_spaces(match.group(1))
        if match.group(2):
            installment = normalize_spaces(match.group(2))
    if not document_ref:
        document_ref = f"XLS-{row_index}"
    if not installment:
        installment = "1/1"
    return document_ref, installment


def extract_document_id(document_ref: str, *, fallback_row: int) -> str:
    digits = "".join(ch for ch in document_ref if ch.isdigit())
    if digits:
        return digits
    return str(900000000 + fallback_row)


def extract_customer_code(value: str) -> str | None:
    match = DIGITS_RE.search(value)
    if not match:
        return None
    return match.group(0)


def normalize_status(status_text: str, due_date_iso: str) -> str:
    lowered = status_text.lower()
    if "vencido" in lowered:
        return "Vencido"
    if "a vencer" in lowered or "vence" in lowered:
        return "A Vencer"

    due_date = datetime.fromisoformat(due_date_iso).date()
    return "Vencido" if due_date < date.today() else "A Vencer"


def resolve_sheet_name(sheet_names: Iterable[str], hint: str) -> str:
    sheet_list = list(sheet_names)
    if not sheet_list:
        raise ValueError("Nenhuma aba encontrada no arquivo Excel.")

    normalized_hint = normalize_key(hint)
    for sheet_name in sheet_list:
        if normalize_key(sheet_name) == normalized_hint:
            return sheet_name
    for sheet_name in sheet_list:
        if normalized_hint in normalize_key(sheet_name):
            return sheet_name
    return sheet_list[0]


def parse_money_to_cents(value: object) -> int | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float, Decimal)):
        decimal_value = Decimal(str(value))
        return int((decimal_value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    text = clean_text(value)
    if not text:
        return None

    normalized = (
        text.replace("R$", "")
        .replace(" ", "")
        .replace(".", "")
        .replace(",", ".")
    )
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        return None
    try:
        decimal_value = Decimal(normalized)
    except InvalidOperation:
        return None
    return int((decimal_value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def to_iso_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        # Excel serial date compatibility (works for arquivos lidos via pandas/openpyxl).
        try:
            serial = float(value)
            if 20000 <= serial <= 60000:
                base = datetime(1899, 12, 30)
                return (base + timedelta(days=serial)).date().isoformat()
        except (TypeError, ValueError, OverflowError):
            return None

    text = clean_text(value)
    if not text:
        return None
    for pattern in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def value_at(row_values: list[object], index_1based: int | None) -> object:
    if index_1based is None:
        return None
    index_0 = index_1based - 1
    if index_0 < 0 or index_0 >= len(row_values):
        return None
    return row_values[index_0]


def is_empty_row(values: list[object]) -> bool:
    for value in values:
        if value is None:
            continue
        if str(value).strip() != "":
            return False
    return True


def row_is_mostly_empty(values: list[object]) -> bool:
    filled = 0
    for value in values:
        if value is None:
            continue
        if str(value).strip():
            filled += 1
    return filled == 0


def is_totals_row(values: list[object]) -> bool:
    text = " ".join(clean_text(value) for value in values if value is not None)
    key = normalize_key(text)
    return "TOTAL GERAL" in key or key.startswith("TOTAL")


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return normalize_spaces(str(value))


def normalize_spaces(value: str) -> str:
    return " ".join(value.strip().split())


def normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return normalize_loose_key(ascii_name.replace("_", " "))


def resolve_customer_hint(
    customer_name: str,
    customer_hints: dict[str, dict[str, str | None]],
) -> dict[str, str | None]:
    primary = normalize_key(customer_name)
    if primary in customer_hints:
        return customer_hints[primary]

    simplified = re.sub(r"\(.*?\)", " ", customer_name)
    simplified = re.sub(r"[-_/]", " ", simplified)
    simplified = re.sub(r"\b(MATRIZ|FILIAL|UNIDADE)\b", " ", simplified, flags=re.IGNORECASE)
    simplified_key = normalize_key(simplified)
    if simplified_key in customer_hints:
        return customer_hints[simplified_key]

    fuzzy_hint = find_fuzzy_customer_hint(customer_name, customer_hints)
    if fuzzy_hint:
        return fuzzy_hint

    return {}


def find_fuzzy_customer_hint(
    customer_name: str,
    customer_hints: dict[str, dict[str, str | None]],
) -> dict[str, str | None] | None:
    target_tokens = tokenize_customer_key(customer_name)
    if len(target_tokens) < 2:
        return None

    best_key: str | None = None
    best_score = 0.0

    for key in customer_hints.keys():
        candidate_tokens = tokenize_customer_key(key)
        if len(candidate_tokens) < 2:
            continue

        overlap = len(target_tokens.intersection(candidate_tokens))
        if overlap < 2:
            continue

        coverage = overlap / max(len(target_tokens), 1)
        if coverage < 0.6:
            continue

        score = (
            coverage
            + (overlap * 0.02)
            - (abs(len(candidate_tokens) - len(target_tokens)) * 0.01)
        )
        if score > best_score:
            best_score = score
            best_key = key

    if best_key is None:
        return None
    return customer_hints.get(best_key)


def tokenize_customer_key(value: str) -> set[str]:
    key = normalize_key(value)
    tokens: set[str] = set()
    for token in key.split():
        if len(token) <= 2:
            continue
        if token in CUSTOMER_HINT_STOPWORDS:
            continue
        tokens.add(token)
    return tokens
