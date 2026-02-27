from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re
from typing import Iterable

import pypdf


VENDOR_RE = re.compile(r"VENDEDOR\s*:\s*([^\n\r]+)")
REPORT_DATETIME_RE = re.compile(r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})")

ROW_RE = re.compile(
    r"^(?P<document_id>\d+)\s+NT:(?P<document_ref>\S+)\s+"
    r"(?P<installment>\d+/\d+)\s+(?P<status>.+?)\s+"
    r"(?P<balance>\d{1,3}(?:\.\d{3})*,\d{2})(?P<tail>.+?)\s+"
    r"(?P<due_date>\d{2}/\d{2}/\d{4})\s+(?P<note_number>\S+)\s+"
    r"(?P<installment_value>\d{1,3}(?:\.\d{3})*,\d{2})$"
)

TAIL_RE = re.compile(
    r"^(?P<customer_name>.*?)(?P<customer_code>\d{5,8})(?P<issue_date>\d{2}/\d{2}/\d{4})$"
)

IGNORE_TOKENS = (
    "CONTAS A RECEBER EM ABERTO",
    "Por Vendedor",
    "Com Filtros Adicionais",
    "ID Documento Situação",
    "Cta Corrente",
    "Vlr. Parcela",
    "www.futurasistemas.com.br",
)


@dataclass(frozen=True)
class ReceivableRecord:
    vendor_name: str
    customer_name: str
    customer_code: str
    status: str
    document_id: str
    document_ref: str
    note_number: str
    installment: str
    issue_date: str
    due_date: str
    balance_cents: int
    installment_value_cents: int
    source_page: int
    report_generated_at: str | None
    raw_line: str


@dataclass(frozen=True)
class ParseReport:
    records: list[ReceivableRecord]
    vendors: list[str]
    skipped_lines: list[str]
    pages_count: int
    candidate_lines_count: int


def parse_pdf(pdf_path: str | Path) -> ParseReport:
    path = Path(pdf_path)
    reader = pypdf.PdfReader(str(path))
    records: list[ReceivableRecord] = []
    skipped_lines: list[str] = []
    vendors_seen: dict[str, None] = {}
    candidate_lines_count = 0

    for page_index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        lines = [normalize_spaces(line) for line in text.splitlines() if line.strip()]
        vendor_name = extract_vendor_name(lines)
        report_generated_at = extract_report_generated_at(lines)

        if not vendor_name:
            continue
        vendors_seen[vendor_name] = None

        for line in lines:
            if should_skip_line(line):
                continue
            candidate_lines_count += 1
            parsed = parse_receivable_line(
                line=line,
                vendor_name=vendor_name,
                source_page=page_index,
                report_generated_at=report_generated_at,
            )
            if parsed is None:
                skipped_lines.append(f"p{page_index}: {line}")
                continue
            records.append(parsed)

    return ParseReport(
        records=records,
        vendors=list(vendors_seen.keys()),
        skipped_lines=skipped_lines,
        pages_count=len(reader.pages),
        candidate_lines_count=candidate_lines_count,
    )


def parse_receivable_line(
    *,
    line: str,
    vendor_name: str,
    source_page: int,
    report_generated_at: str | None,
) -> ReceivableRecord | None:
    match = ROW_RE.match(line)
    if not match:
        return None

    tail = normalize_spaces(match.group("tail"))
    tail_match = TAIL_RE.match(tail)
    if not tail_match:
        return None

    customer_name = normalize_spaces(tail_match.group("customer_name"))
    customer_code = tail_match.group("customer_code")
    issue_date = br_date_to_iso(tail_match.group("issue_date"))
    due_date = br_date_to_iso(match.group("due_date"))
    status = normalize_status(match.group("status"))

    return ReceivableRecord(
        vendor_name=vendor_name,
        customer_name=customer_name,
        customer_code=customer_code,
        status=status,
        document_id=match.group("document_id"),
        document_ref=match.group("document_ref"),
        note_number=match.group("note_number"),
        installment=match.group("installment"),
        issue_date=issue_date,
        due_date=due_date,
        balance_cents=brl_to_cents(match.group("balance")),
        installment_value_cents=brl_to_cents(match.group("installment_value")),
        source_page=source_page,
        report_generated_at=report_generated_at,
        raw_line=line,
    )


def should_skip_line(line: str) -> bool:
    if any(token in line for token in IGNORE_TOKENS):
        return True
    if "Total Geral" in line:
        return True
    if line.endswith("Total :"):
        return True
    if "VENDEDOR :" in line:
        return True
    return False


def extract_vendor_name(lines: Iterable[str]) -> str | None:
    for line in lines:
        match = VENDOR_RE.search(line)
        if match:
            return normalize_spaces(match.group(1))
    return None


def extract_report_generated_at(lines: Iterable[str]) -> str | None:
    for line in lines:
        match = REPORT_DATETIME_RE.search(line)
        if match:
            dt = datetime.strptime(
                f"{match.group(1)} {match.group(2)}",
                "%d/%m/%Y %H:%M:%S",
            )
            return dt.isoformat()
    return None


def normalize_status(status: str) -> str:
    cleaned = normalize_spaces(status).lower()
    if "a vencer" in cleaned:
        return "A Vencer"
    if "vencido" in cleaned:
        return "Vencido"
    return normalize_spaces(status)


def br_date_to_iso(value: str) -> str:
    return datetime.strptime(value, "%d/%m/%Y").date().isoformat()


def brl_to_cents(value: str) -> int:
    decimal_value = Decimal(value.replace(".", "").replace(",", "."))
    return int((decimal_value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def normalize_spaces(value: str) -> str:
    return " ".join(value.strip().split())
