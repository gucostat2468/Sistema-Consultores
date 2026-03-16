from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable
import re
import unicodedata

from src.excel_sheet_loader import load_excel_sheets, normalize_loose_key

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

@dataclass(frozen=True)
class CreditLimitRecord:
    consultant_name: str
    customer_name: str
    cnpj: str | None
    credit_limit_cents: int | None
    credit_used_cents: int | None
    credit_available_cents: int | None
    limit_policy: str | None
    note: str | None
    updated_at: str | None
    raw_limit_value: str | None
    raw_used_value: str | None
    raw_available_value: str | None
    source_row: int
    source_sheet: str
    source_section: str


@dataclass(frozen=True)
class CreditSheetSummary:
    sheet_name: str
    rows_scanned: int
    candidate_rows: int
    records_count: int
    skipped_rows_count: int
    consultants_count: int
    credit_section_detected: bool


@dataclass(frozen=True)
class CreditParseReport:
    records: list[CreditLimitRecord]
    consultants: list[str]
    skipped_rows: list[str]
    sheet_name: str
    rows_scanned: int
    candidate_rows: int
    workbook_sheets: list[str]
    scanned_sheets: list[str]
    processed_sheets: list[str]
    sheet_summaries: list[CreditSheetSummary]


@dataclass(frozen=True)
class _SheetParseResult:
    records: list[CreditLimitRecord]
    consultants: list[str]
    skipped_rows: list[str]
    rows_scanned: int
    candidate_rows: int
    credit_section_detected: bool


def parse_credit_excel(
    path: str | Path,
    sheet_name_hint: str = "Limite de crédito",
    *,
    parse_all_sheets: bool = True,
    customer_hints: dict[str, dict[str, str | None]] | None = None,
    default_consultant_name: str | None = None,
) -> CreditParseReport:
    excel_path = Path(path)
    sheet_map = load_excel_sheets(excel_path)
    workbook_sheets = list(sheet_map.keys())
    primary_sheet = resolve_sheet_name(workbook_sheets, sheet_name_hint)
    default_updated_at = datetime.fromtimestamp(excel_path.stat().st_mtime).isoformat()

    if parse_all_sheets:
        scanned_sheets = [primary_sheet] + [
            sheet_name for sheet_name in workbook_sheets if sheet_name != primary_sheet
        ]
    else:
        scanned_sheets = [primary_sheet]

    records: list[CreditLimitRecord] = []
    skipped_rows: list[str] = []
    consultants_seen: dict[str, None] = {}
    candidate_rows_total = 0
    rows_scanned_total = 0
    processed_sheets: list[str] = []
    sheet_summaries: list[CreditSheetSummary] = []

    for sheet_name in scanned_sheets:
        worksheet = sheet_map[sheet_name]
        sheet_result = parse_credit_worksheet(
            worksheet,
            sheet_name,
            default_updated_at=default_updated_at,
            customer_hints=customer_hints or {},
            default_consultant_name=default_consultant_name,
        )

        records.extend(sheet_result.records)
        skipped_rows.extend(sheet_result.skipped_rows)
        candidate_rows_total += sheet_result.candidate_rows
        rows_scanned_total += sheet_result.rows_scanned
        for consultant_name in sheet_result.consultants:
            consultants_seen[consultant_name] = None

        if sheet_result.credit_section_detected:
            processed_sheets.append(sheet_name)

        sheet_summaries.append(
            CreditSheetSummary(
                sheet_name=sheet_name,
                rows_scanned=sheet_result.rows_scanned,
                candidate_rows=sheet_result.candidate_rows,
                records_count=len(sheet_result.records),
                skipped_rows_count=len(sheet_result.skipped_rows),
                consultants_count=len(sheet_result.consultants),
                credit_section_detected=sheet_result.credit_section_detected,
            )
        )

    return CreditParseReport(
        records=records,
        consultants=list(consultants_seen.keys()),
        skipped_rows=skipped_rows,
        sheet_name=primary_sheet,
        rows_scanned=rows_scanned_total,
        candidate_rows=candidate_rows_total,
        workbook_sheets=workbook_sheets,
        scanned_sheets=scanned_sheets,
        processed_sheets=processed_sheets,
        sheet_summaries=sheet_summaries,
    )


def parse_credit_worksheet(
    worksheet,
    sheet_name: str,
    *,
    default_updated_at: str | None,
    customer_hints: dict[str, dict[str, str | None]],
    default_consultant_name: str | None,
) -> _SheetParseResult:
    records: list[CreditLimitRecord] = []
    skipped_rows: list[str] = []
    consultants_seen: dict[str, None] = {}

    current_consultant: str | None = None
    inside_credit_section = False
    credit_section_detected = False
    last_customer_name: str | None = None
    candidate_rows = 0

    for row_index in range(1, int(getattr(worksheet, "max_row", 0)) + 1):
        raw_cells = [worksheet.cell(row_index, col_index).value for col_index in range(1, 8)]
        c1, c2, c3, c4, c5, c6, c7 = raw_cells

        if is_empty_row(raw_cells):
            continue

        c1_text = clean_text(c1)
        c2_text = clean_text(c2)

        if is_consultant_header_row(c1_text, c2, c3, c4, c5):
            current_consultant = c1_text
            consultants_seen[current_consultant] = None
            inside_credit_section = False
            last_customer_name = None
            continue

        if is_credit_header_row(c1_text, c2_text):
            inside_credit_section = True
            credit_section_detected = True
            last_customer_name = None
            continue

        if not inside_credit_section:
            continue

        if c1_text and "RAZAO SOCIAL" in normalize_key(c1_text):
            # A planilha pode ter uma tabela de report fora da seção principal de crédito.
            inside_credit_section = False
            last_customer_name = None
            continue

        if c1_text and is_consultant_header_row(c1_text, c2, c3, c4, c5):
            current_consultant = c1_text
            consultants_seen[current_consultant] = None
            inside_credit_section = False
            last_customer_name = None
            continue

        if not c1_text and not c2_text and not any(
            value is not None for value in [c3, c4, c5, c6, c7]
        ):
            continue

        candidate_rows += 1

        customer_name = c1_text or last_customer_name
        if not customer_name:
            skipped_rows.append(f"{sheet_name}!r{row_index}: cliente nao identificado")
            continue
        last_customer_name = customer_name

        consultant_name = current_consultant
        if not consultant_name:
            hint = resolve_customer_hint(customer_name, customer_hints)
            consultant_name = clean_text(hint.get("consultantName"))
        if not consultant_name and default_consultant_name:
            consultant_name = clean_text(default_consultant_name)

        if not consultant_name:
            skipped_rows.append(f"{sheet_name}!r{row_index}: consultor nao identificado")
            continue
        consultants_seen[consultant_name] = None

        limit_cents, limit_text, raw_limit = parse_money_or_text(c3)
        used_cents, used_text, raw_used = parse_money_or_text(c4)
        available_cents, available_text, raw_available = parse_money_or_text(c5)

        policy_texts = [text for text in [limit_text, used_text, available_text] if text]
        limit_policy = "; ".join(policy_texts) if policy_texts else None

        record = CreditLimitRecord(
            consultant_name=consultant_name,
            customer_name=customer_name,
            cnpj=c2_text or None,
            credit_limit_cents=limit_cents,
            credit_used_cents=used_cents,
            credit_available_cents=available_cents,
            limit_policy=limit_policy,
            note=clean_text(c6) or None,
            updated_at=to_iso_datetime(c7) or default_updated_at,
            raw_limit_value=raw_limit,
            raw_used_value=raw_used,
            raw_available_value=raw_available,
            source_row=row_index,
            source_sheet=sheet_name,
            source_section="limite_credito",
        )
        records.append(record)

    return _SheetParseResult(
        records=records,
        consultants=list(consultants_seen.keys()),
        skipped_rows=skipped_rows,
        rows_scanned=worksheet.max_row,
        candidate_rows=candidate_rows,
        credit_section_detected=credit_section_detected,
    )


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


def is_consultant_header_row(c1_text: str, c2: object, c3: object, c4: object, c5: object) -> bool:
    if not c1_text:
        return False
    if any(value is not None and str(value).strip() != "" for value in [c2, c3, c4, c5]):
        return False
    key = normalize_key(c1_text)
    if "SUBDEALER" in key or "CLIENTE" in key or "RAZAO SOCIAL" in key:
        return False
    if key.startswith("TOTAL"):
        return False
    return True


def is_credit_header_row(c1_text: str, c2_text: str) -> bool:
    key1 = normalize_key(c1_text)
    key2 = normalize_key(c2_text)
    return "SUBDEALER" in key1 and "CNPJ" in key2


def parse_money_or_text(value: object) -> tuple[int | None, str | None, str | None]:
    if value is None:
        return None, None, None

    if isinstance(value, bool):
        raw = str(value)
        return None, raw, raw

    if isinstance(value, (int, float, Decimal)):
        cents = int((Decimal(str(value)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        return cents, None, format_decimal(value)

    text = clean_text(value)
    if not text:
        return None, None, None

    parsed = parse_decimal_text(text)
    if parsed is None:
        return None, text, text
    cents = int((parsed * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return cents, None, text


def parse_decimal_text(text: str) -> Decimal | None:
    normalized = text.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    if not re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        return None
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def to_iso_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()

    text = clean_text(value)
    if not text:
        return None
    for pattern in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, pattern).isoformat()
        except ValueError:
            continue
    return text


def is_empty_row(values: list[object]) -> bool:
    for value in values:
        if value is None:
            continue
        if str(value).strip() != "":
            return False
    return True


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


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


def format_decimal(value: int | float | Decimal) -> str:
    decimal_value = Decimal(str(value))
    return f"{decimal_value.normalize()}" if decimal_value % 1 else f"{decimal_value:.0f}"
