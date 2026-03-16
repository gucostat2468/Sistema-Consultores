from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Literal
import re
import unicodedata

from src.credit_excel_parser import CreditParseReport
from src.pdf_parser import ParseReport
from src.receivables_excel_parser import ExcelReceivablesParseReport


EXPECTED_CONSULTANTS = [
    "LUIZ CARLOS ABRANCHES",
    "ALISSON ROCHA ALENCAR",
    "DUSTIN AKIHIRO MAENO",
    "THIAGO GOMES",
    "GABRIEL MOURA DE ALMEIDA",
    "KALLEBE GOMES DA SILVA",
    "GUILHERME APARECIDO VANZELLA",
]

ALLOWED_STATUS = {"A Vencer", "Vencido"}
CONSULTANT_ALIASES = {
    "LUIZ CARLOS": "LUIZ CARLOS ABRANCHES",
    "LUIZ CARLOS ABRANCHES": "LUIZ CARLOS ABRANCHES",
    "ALISSON ROCHA": "ALISSON ROCHA ALENCAR",
    "ALISSON ROCHA ALENCAR": "ALISSON ROCHA ALENCAR",
    "DUSTIN TOCANTINS": "DUSTIN AKIHIRO MAENO",
    "DUSTIN AKIHIRO MAENO": "DUSTIN AKIHIRO MAENO",
    "THIAGO GOMES": "THIAGO GOMES",
    "GABRIEL MOURA": "GABRIEL MOURA DE ALMEIDA",
    "GABRIEL MOURA DE ALMEIDA": "GABRIEL MOURA DE ALMEIDA",
    "KALLEBE GOMES": "KALLEBE GOMES DA SILVA",
    "KALLEBE GOMES DA SILVA": "KALLEBE GOMES DA SILVA",
    "KALLEBE GOMES": "KALLEBE GOMES DA SILVA",
    "GUILHERME": "GUILHERME APARECIDO VANZELLA",
    "GUILHERME APARECIDO VANZELLA": "GUILHERME APARECIDO VANZELLA",
}


@dataclass(frozen=True)
class ValidationIssue:
    level: Literal["error", "warning"]
    code: str
    message: str


@dataclass(frozen=True)
class ValidationResult:
    issues: list[ValidationIssue]
    stats: dict[str, object]

    @property
    def errors(self) -> list[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "warning"]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def validate_parse_report(
    report: ParseReport,
    *,
    expected_vendor_names: list[str] | None = None,
    strict_vendor_match: bool = True,
    max_skipped_lines: int = 0,
) -> ValidationResult:
    issues: list[ValidationIssue] = []

    records_count = len(report.records)
    skipped_count = len(report.skipped_lines)
    vendors_count = len(report.vendors)
    candidate_lines = report.candidate_lines_count
    parse_success_ratio = (
        records_count / candidate_lines if candidate_lines > 0 else 0.0
    )

    if records_count == 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_RECORDS",
                message="Nenhum registro financeiro foi extraído do PDF.",
            )
        )

    if vendors_count == 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_VENDORS",
                message="Nenhum vendedor foi identificado no PDF.",
            )
        )

    if skipped_count > max_skipped_lines:
        issues.append(
            ValidationIssue(
                level="error",
                code="SKIPPED_LINES",
                message=(
                    f"Foram encontradas {skipped_count} linhas não interpretadas "
                    f"(limite configurado: {max_skipped_lines})."
                ),
            )
        )

    if candidate_lines > 0 and parse_success_ratio < 0.98:
        level: Literal["error", "warning"] = "error" if parse_success_ratio < 0.90 else "warning"
        issues.append(
            ValidationIssue(
                level=level,
                code="LOW_PARSE_RATIO",
                message=(
                    f"Taxa de interpretação abaixo do esperado: "
                    f"{parse_success_ratio * 100:.1f}% ({records_count}/{candidate_lines})."
                ),
            )
        )

    unknown_status = sorted({record.status for record in report.records if record.status not in ALLOWED_STATUS})
    if unknown_status:
        issues.append(
            ValidationIssue(
                level="error",
                code="UNKNOWN_STATUS",
                message=f"Foram encontrados status não previstos: {', '.join(unknown_status)}.",
            )
        )

    duplicate_rows = count_duplicate_rows(report)
    if duplicate_rows > 0:
        issues.append(
            ValidationIssue(
                level="warning",
                code="DUPLICATE_ROWS",
                message=f"Foram detectadas {duplicate_rows} linhas duplicadas no arquivo.",
            )
        )

    expected = normalize_vendor_set(expected_vendor_names or [])
    found = normalize_vendor_set(report.vendors)
    missing_vendors = sorted(expected - found)
    extra_vendors = sorted(found - expected)

    if expected:
        if missing_vendors:
            issues.append(
                ValidationIssue(
                    level="error" if strict_vendor_match else "warning",
                    code="MISSING_VENDORS",
                    message=(
                        "Consultores esperados ausentes no arquivo: "
                        + ", ".join(missing_vendors)
                    ),
                )
            )
        if extra_vendors:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="EXTRA_VENDORS",
                    message="Consultores adicionais encontrados: " + ", ".join(extra_vendors),
                )
            )

    stats: dict[str, object] = {
        "pages_count": report.pages_count,
        "records_count": records_count,
        "vendors_count": vendors_count,
        "candidate_lines_count": candidate_lines,
        "skipped_lines_count": skipped_count,
        "parse_success_ratio": parse_success_ratio,
        "duplicate_rows": duplicate_rows,
        "found_vendors": report.vendors,
        "missing_vendors": missing_vendors,
        "extra_vendors": extra_vendors,
    }
    return ValidationResult(issues=issues, stats=stats)


def validate_credit_parse_report(
    report: CreditParseReport,
    *,
    expected_vendor_names: list[str] | None = None,
    strict_vendor_match: bool = False,
    max_skipped_rows: int = 10,
) -> ValidationResult:
    issues: list[ValidationIssue] = []

    records_count = len(report.records)
    skipped_count = len(report.skipped_rows)
    vendors_count = len(report.consultants)
    candidate_rows = report.candidate_rows
    parse_success_ratio = records_count / candidate_rows if candidate_rows > 0 else 0.0
    workbook_sheets_count = len(report.workbook_sheets)
    scanned_sheets_count = len(report.scanned_sheets)
    processed_sheets_count = len(report.processed_sheets)

    if records_count == 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_CREDIT_RECORDS",
                message="Nenhum registro de limite de crédito foi extraído da planilha.",
            )
        )

    if skipped_count > max_skipped_rows:
        issues.append(
            ValidationIssue(
                level="error",
                code="CREDIT_SKIPPED_ROWS",
                message=(
                    f"Planilha com {skipped_count} linhas ignoradas "
                    f"(limite configurado: {max_skipped_rows})."
                ),
            )
        )

    if processed_sheets_count == 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_CREDIT_SECTION_FOUND",
                message=(
                    "Nenhuma seção de crédito foi encontrada nas abas lidas. "
                    "Verifique cabeçalho com 'Subdealer' e 'CNPJ'."
                ),
            )
        )

    if candidate_rows > 0 and parse_success_ratio < 0.90:
        issues.append(
            ValidationIssue(
                level="error",
                code="LOW_CREDIT_PARSE_RATIO",
                message=(
                    f"Taxa de leitura da planilha abaixo do esperado: "
                    f"{parse_success_ratio * 100:.1f}% ({records_count}/{candidate_rows})."
                ),
            )
        )

    if scanned_sheets_count < workbook_sheets_count:
        issues.append(
            ValidationIssue(
                level="warning",
                code="PARTIAL_SHEET_SCAN",
                message=(
                    f"Apenas {scanned_sheets_count} de {workbook_sheets_count} abas foram lidas "
                    "na validação da planilha."
                ),
            )
        )

    if vendors_count == 0:
        issues.append(
            ValidationIssue(
                level="warning",
                code="NO_CREDIT_VENDORS",
                message="Nenhum consultor foi identificado na planilha de crédito.",
            )
        )

    records_without_numeric = sum(
        1
        for record in report.records
        if record.credit_limit_cents is None
        and record.credit_used_cents is None
        and record.credit_available_cents is None
    )
    if records_without_numeric > 0:
        issues.append(
            ValidationIssue(
                level="warning",
                code="CREDIT_NON_NUMERIC_ROWS",
                message=(
                    f"{records_without_numeric} registros sem valores numéricos de crédito "
                    "(apenas política/observação)."
                ),
            )
        )

    expected = normalize_vendor_set(expected_vendor_names or [])
    found = normalize_vendor_set(report.consultants)
    missing_vendors = sorted(expected - found)
    extra_vendors = sorted(found - expected)

    if expected:
        if missing_vendors:
            issues.append(
                ValidationIssue(
                    level="error" if strict_vendor_match else "warning",
                    code="MISSING_CREDIT_VENDORS",
                    message=(
                        "Consultores esperados ausentes na planilha de crédito: "
                        + ", ".join(missing_vendors)
                    ),
                )
            )
        if extra_vendors:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="EXTRA_CREDIT_VENDORS",
                    message="Consultores adicionais na planilha de crédito: " + ", ".join(extra_vendors),
                )
            )

    stats: dict[str, object] = {
        "sheet_name": report.sheet_name,
        "workbook_sheets": report.workbook_sheets,
        "workbook_sheets_count": workbook_sheets_count,
        "scanned_sheets": report.scanned_sheets,
        "scanned_sheets_count": scanned_sheets_count,
        "processed_sheets": report.processed_sheets,
        "processed_sheets_count": processed_sheets_count,
        "rows_scanned": report.rows_scanned,
        "candidate_rows": candidate_rows,
        "records_count": records_count,
        "skipped_rows_count": skipped_count,
        "parse_success_ratio": parse_success_ratio,
        "vendors_count": vendors_count,
        "found_vendors": report.consultants,
        "missing_vendors": missing_vendors,
        "extra_vendors": extra_vendors,
        "records_without_numeric": records_without_numeric,
        "sheet_summaries": [
            {
                "sheet_name": item.sheet_name,
                "rows_scanned": item.rows_scanned,
                "candidate_rows": item.candidate_rows,
                "records_count": item.records_count,
                "skipped_rows_count": item.skipped_rows_count,
                "consultants_count": item.consultants_count,
                "credit_section_detected": item.credit_section_detected,
            }
            for item in report.sheet_summaries
        ],
    }
    return ValidationResult(issues=issues, stats=stats)


def validate_excel_receivables_parse_report(
    report: ExcelReceivablesParseReport,
    *,
    expected_vendor_names: list[str] | None = None,
    strict_vendor_match: bool = False,
    max_skipped_rows: int = 0,
) -> ValidationResult:
    issues: list[ValidationIssue] = []

    records_count = len(report.records)
    skipped_count = len(report.skipped_rows)
    vendors_count = len(report.vendors)
    candidate_rows = report.candidate_rows
    parse_success_ratio = records_count / candidate_rows if candidate_rows > 0 else 0.0
    workbook_sheets_count = len(report.workbook_sheets)
    scanned_sheets_count = len(report.scanned_sheets)
    processed_sheets_count = len(report.processed_sheets)

    if records_count == 0 and processed_sheets_count > 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_EXCEL_RECEIVABLE_RECORDS",
                message="Nenhum título foi extraído da planilha de movimentações em aberto.",
            )
        )

    if skipped_count > max_skipped_rows:
        issues.append(
            ValidationIssue(
                level="error",
                code="EXCEL_RECEIVABLE_SKIPPED_ROWS",
                message=(
                    f"Planilha com {skipped_count} linhas ignoradas "
                    f"(limite configurado: {max_skipped_rows})."
                ),
            )
        )

    if candidate_rows > 0 and parse_success_ratio < 0.90:
        issues.append(
            ValidationIssue(
                level="error",
                code="LOW_EXCEL_RECEIVABLE_PARSE_RATIO",
                message=(
                    f"Taxa de leitura da planilha de títulos abaixo do esperado: "
                    f"{parse_success_ratio * 100:.1f}% ({records_count}/{candidate_rows})."
                ),
            )
        )

    if scanned_sheets_count < workbook_sheets_count:
        issues.append(
            ValidationIssue(
                level="warning",
                code="PARTIAL_EXCEL_RECEIVABLE_SHEET_SCAN",
                message=(
                    f"Apenas {scanned_sheets_count} de {workbook_sheets_count} abas foram lidas "
                    "na validação da planilha de títulos."
                ),
            )
        )

    if vendors_count == 0 and records_count > 0:
        issues.append(
            ValidationIssue(
                level="error",
                code="NO_EXCEL_RECEIVABLE_VENDORS",
                message="A planilha de títulos não permitiu identificar consultor por cliente.",
            )
        )

    unknown_status = sorted({record.status for record in report.records if record.status not in ALLOWED_STATUS})
    if unknown_status:
        issues.append(
            ValidationIssue(
                level="error",
                code="UNKNOWN_EXCEL_RECEIVABLE_STATUS",
                message=f"Foram encontrados status não previstos: {', '.join(unknown_status)}.",
            )
        )

    expected = normalize_vendor_set(expected_vendor_names or [])
    found = normalize_vendor_set(report.vendors)
    missing_vendors = sorted(expected - found)
    extra_vendors = sorted(found - expected)

    if expected and records_count > 0:
        if missing_vendors:
            issues.append(
                ValidationIssue(
                    level="error" if strict_vendor_match else "warning",
                    code="MISSING_EXCEL_RECEIVABLE_VENDORS",
                    message=(
                        "Consultores esperados ausentes na planilha de títulos: "
                        + ", ".join(missing_vendors)
                    ),
                )
            )
        if extra_vendors:
            issues.append(
                ValidationIssue(
                    level="warning",
                    code="EXTRA_EXCEL_RECEIVABLE_VENDORS",
                    message="Consultores adicionais na planilha de títulos: " + ", ".join(extra_vendors),
                )
            )

    duplicate_rows = count_excel_receivable_duplicate_rows(report)
    if duplicate_rows > 0:
        issues.append(
            ValidationIssue(
                level="warning",
                code="DUPLICATE_EXCEL_RECEIVABLE_ROWS",
                message=f"Foram detectadas {duplicate_rows} linhas duplicadas na planilha de títulos.",
            )
        )

    stats: dict[str, object] = {
        "sheet_name": report.sheet_name,
        "workbook_sheets": report.workbook_sheets,
        "workbook_sheets_count": workbook_sheets_count,
        "scanned_sheets": report.scanned_sheets,
        "scanned_sheets_count": scanned_sheets_count,
        "processed_sheets": report.processed_sheets,
        "processed_sheets_count": processed_sheets_count,
        "rows_scanned": report.rows_scanned,
        "candidate_rows": candidate_rows,
        "records_count": records_count,
        "skipped_rows_count": skipped_count,
        "parse_success_ratio": parse_success_ratio,
        "vendors_count": vendors_count,
        "found_vendors": report.vendors,
        "missing_vendors": missing_vendors,
        "extra_vendors": extra_vendors,
        "duplicate_rows": duplicate_rows,
        "sheet_summaries": [
            {
                "sheet_name": item.sheet_name,
                "rows_scanned": item.rows_scanned,
                "candidate_rows": item.candidate_rows,
                "records_count": item.records_count,
                "skipped_rows_count": item.skipped_rows_count,
                "consultants_count": item.consultants_count,
                "receivable_section_detected": item.receivable_section_detected,
                "header_signature": item.header_signature,
                "detected_columns": item.detected_columns,
            }
            for item in report.sheet_summaries
        ],
    }
    return ValidationResult(issues=issues, stats=stats)


def validate_report_v1_workbook_layout(
    sheet_names: list[str],
    *,
    expected_sheet_count: int = 4,
) -> ValidationResult:
    issues: list[ValidationIssue] = []
    normalized_pairs = [
        (sheet_name, normalize_sheet_key(sheet_name))
        for sheet_name in sheet_names
    ]

    report_sheets = [name for name, key in normalized_pairs if key.startswith("REPORT")]
    credit_sheets = [name for name, key in normalized_pairs if "LIMITE DE CREDITO" in key]
    order_sheets = [
        name
        for name, key in normalized_pairs
        if "PEDIDOS" in key and "GERAL" in key
    ]
    covered = set(report_sheets + credit_sheets + order_sheets)
    extra_sheets = [name for name in sheet_names if name not in covered]

    if len(sheet_names) != expected_sheet_count:
        issues.append(
            ValidationIssue(
                level="error",
                code="REPORT_V1_SHEET_COUNT",
                message=(
                    f"O perfil report_v1 exige exatamente {expected_sheet_count} abas; "
                    f"arquivo recebido com {len(sheet_names)}."
                ),
            )
        )

    if not report_sheets:
        issues.append(
            ValidationIssue(
                level="error",
                code="REPORT_V1_MISSING_REPORT_SHEET",
                message="Aba principal do report nao encontrada (esperado nome com 'Report').",
            )
        )

    if len(credit_sheets) < 2:
        issues.append(
            ValidationIssue(
                level="error",
                code="REPORT_V1_CREDIT_SHEETS",
                message=(
                    "O perfil report_v1 exige duas abas de limite de credito "
                    "(ex.: 'Limite de credito' e variacao)."
                ),
            )
        )

    if not order_sheets:
        issues.append(
            ValidationIssue(
                level="error",
                code="REPORT_V1_MISSING_ORDERS_SHEET",
                message="Aba de apoio de pedidos nao encontrada (esperado nome com 'Pedidos Geral').",
            )
        )

    if extra_sheets:
        issues.append(
            ValidationIssue(
                level="warning",
                code="REPORT_V1_EXTRA_SHEETS",
                message=(
                    "Abas adicionais fora do perfil report_v1: "
                    + ", ".join(extra_sheets)
                ),
            )
        )

    stats: dict[str, object] = {
        "sheet_names": sheet_names,
        "sheet_count": len(sheet_names),
        "expected_sheet_count": expected_sheet_count,
        "report_sheets": report_sheets,
        "credit_sheets": credit_sheets,
        "order_sheets": order_sheets,
        "extra_sheets": extra_sheets,
    }
    return ValidationResult(issues=issues, stats=stats)


def count_duplicate_rows(report: ParseReport) -> int:
    keys = [
        (
            record.vendor_name,
            record.customer_code,
            record.document_id,
            record.document_ref,
            record.installment,
            record.due_date,
            record.balance_cents,
        )
        for record in report.records
    ]
    counter = Counter(keys)
    return sum(count - 1 for count in counter.values() if count > 1)


def count_excel_receivable_duplicate_rows(report: ExcelReceivablesParseReport) -> int:
    keys = [
        (
            record.vendor_name,
            record.customer_name,
            record.document_id,
            record.document_ref,
            record.installment,
            record.due_date,
            record.balance_cents,
        )
        for record in report.records
    ]
    counter = Counter(keys)
    return sum(count - 1 for count in counter.values() if count > 1)


def normalize_vendor_set(names: list[str]) -> set[str]:
    normalized: set[str] = set()
    for name in names:
        candidate = normalize_name(name)
        if not candidate:
            continue
        candidate = CONSULTANT_ALIASES.get(candidate, candidate)
        normalized.add(candidate)
    return normalized


def normalize_name(name: str) -> str:
    return " ".join(name.upper().replace("_", " ").split())


def normalize_sheet_key(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name or ""))
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    key = re.sub(r"[^A-Z0-9]+", " ", ascii_name.upper())
    return " ".join(key.split())
