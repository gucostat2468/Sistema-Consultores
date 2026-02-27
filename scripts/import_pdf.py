from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.credit_excel_parser import parse_credit_excel
from src.db import (
    authenticate_user,
    ensure_admin_user,
    import_credit_limits,
    import_receivables,
    init_db,
    list_consultants,
)
from src.pdf_parser import parse_pdf
from src.update_validation import (
    EXPECTED_CONSULTANTS,
    validate_credit_parse_report,
    validate_parse_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Importa e valida dados de contas a receber (PDF) e limite de crédito (Excel), "
            "criando consultores automaticamente quando necessário."
        )
    )
    parser.add_argument(
        "--pdf",
        help="Caminho do arquivo PDF de contas a receber.",
    )
    parser.add_argument(
        "--excel",
        help="Caminho da planilha de limite de crédito (.xlsx).",
    )
    parser.add_argument(
        "--default-password",
        default="Consultor@123",
        help="Senha inicial para novos consultores criados no import do PDF.",
    )
    parser.add_argument(
        "--admin-password",
        default="Admin@123",
        help="Senha do usuário administrador (criado apenas se não existir).",
    )
    parser.add_argument(
        "--import-user",
        default="adm",
        help="Usuário autorizado a executar a atualização (perfil admin). Ex.: adm ou admin.",
    )
    parser.add_argument(
        "--import-password",
        help="Senha do usuário de atualização. Se omitida, será solicitada no terminal.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Mantém registros existentes e apenas adiciona novos títulos.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Executa apenas validação dos arquivos sem atualizar a base.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Permite importar mesmo com erro de validação.",
    )
    parser.add_argument(
        "--allow-skipped-lines",
        type=int,
        default=0,
        help="Quantidade máxima de linhas não interpretadas permitida no PDF.",
    )
    parser.add_argument(
        "--allow-skipped-credit-rows",
        type=int,
        default=10,
        help="Quantidade máxima de linhas ignoradas permitida na planilha de crédito.",
    )
    parser.add_argument(
        "--no-strict-vendors",
        action="store_true",
        help="Não bloqueia validação quando faltar consultor esperado.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pdf_path = Path(args.pdf).expanduser() if args.pdf else None
    excel_path = Path(args.excel).expanduser() if args.excel else None

    if pdf_path is None and excel_path is None:
        raise SystemExit("Informe ao menos um arquivo: --pdf ou --excel.")

    if pdf_path and not pdf_path.exists():
        raise SystemExit(f"Arquivo PDF não encontrado: {pdf_path}")
    if excel_path and not excel_path.exists():
        raise SystemExit(f"Arquivo Excel não encontrado: {excel_path}")

    init_db()

    pdf_report = None
    pdf_validation = None
    credit_report = None
    credit_validation = None

    if pdf_path:
        pdf_report = parse_pdf(pdf_path)
        pdf_validation = validate_parse_report(
            pdf_report,
            expected_vendor_names=EXPECTED_CONSULTANTS,
            strict_vendor_match=not args.no_strict_vendors,
            max_skipped_lines=args.allow_skipped_lines,
        )
        print_pdf_validation(pdf_validation)

    if excel_path:
        credit_report = parse_credit_excel(excel_path)
        credit_validation = validate_credit_parse_report(
            credit_report,
            expected_vendor_names=EXPECTED_CONSULTANTS,
            strict_vendor_match=not args.no_strict_vendors,
            max_skipped_rows=args.allow_skipped_credit_rows,
        )
        print_credit_validation(credit_validation)

    all_valid = all(
        validation is None or validation.is_valid
        for validation in [pdf_validation, credit_validation]
    )

    if args.validate_only:
        if all_valid:
            print("\nValidação final: APROVADO")
            return
        raise SystemExit("\nValidação final: REPROVADO")

    if not all_valid and not args.force:
        raise SystemExit(
            "\nValidação final: REPROVADO. Corrija o arquivo ou use --force para importar mesmo assim."
        )

    ensure_admin_user(password=args.admin_password)

    import_user = args.import_user.strip()
    import_password = args.import_password or args.admin_password
    actor = authenticate_user(import_user, import_password)
    if actor is None or not actor.is_admin:
        raise SystemExit(
            "Importação bloqueada: somente usuário administrador (adm/admin) pode atualizar a base."
        )

    if pdf_report:
        receivables_summary = import_receivables(
            pdf_report.records,
            source_file=pdf_path.name,
            default_password=args.default_password,
            wipe_existing=not args.append,
        )
        print("=== Importação de PDF concluída ===")
        print(f"Arquivo: {pdf_path}")
        print(f"Consultores encontrados no PDF: {len(pdf_report.vendors)}")
        print(f"Registros detectados: {receivables_summary.total_records_seen}")
        print(f"Registros importados: {receivables_summary.imported_rows}")
        print(f"Registros duplicados: {receivables_summary.duplicated_rows}")
        print(f"Linhas não interpretadas: {len(pdf_report.skipped_lines)}")

        if receivables_summary.created_consultants:
            print("\nNovos consultores criados:")
            for name, username in sorted(receivables_summary.created_consultants.items()):
                print(f"- {name}: usuário `{username}` | senha inicial `{args.default_password}`")
        else:
            print("\nNenhum consultor novo foi criado nesta execução do PDF.")

    if credit_report:
        credit_summary = import_credit_limits(
            credit_report.records,
            source_file=excel_path.name,
            wipe_existing=not args.append,
        )
        print("\n=== Importação de Excel concluída ===")
        print(f"Arquivo: {excel_path}")
        print(f"Aba de referência: {credit_report.sheet_name}")
        print(
            f"Abas lidas: {len(credit_report.scanned_sheets)} de {len(credit_report.workbook_sheets)}"
        )
        print(f"Abas com seção de crédito: {len(credit_report.processed_sheets)}")
        print(f"Registros detectados: {credit_summary.total_records_seen}")
        print(f"Registros inseridos: {credit_summary.imported_rows}")
        print(f"Registros atualizados: {credit_summary.updated_rows}")
        print(f"Registros ignorados: {credit_summary.skipped_rows}")
        if credit_summary.unresolved_consultants:
            print("Consultores não resolvidos (registros ignorados):")
            for consultant_name, count in sorted(credit_summary.unresolved_consultants.items()):
                print(f"- {consultant_name}: {count}")

    consultants = [c for c in list_consultants() if not c["is_admin"]]
    print("\nConsultores disponíveis para login:")
    for consultant in consultants:
        print(f"- {consultant['name']} ({consultant['username']})")

    if pdf_report and pdf_report.skipped_lines:
        print("\nAmostra de linhas não interpretadas (até 5):")
        for line in pdf_report.skipped_lines[:5]:
            print(f"- {line}")
    if credit_report and credit_report.skipped_rows:
        print("\nAmostra de linhas ignoradas na planilha (até 5):")
        for line in credit_report.skipped_rows[:5]:
            print(f"- {line}")


def print_pdf_validation(validation) -> None:
    stats = validation.stats
    print("=== Validação do PDF ===")
    print(f"Páginas: {stats['pages_count']}")
    print(f"Linhas candidatas: {stats['candidate_lines_count']}")
    print(
        f"Linhas interpretadas: {stats['records_count']} "
        f"({stats['parse_success_ratio'] * 100:.1f}%)"
    )
    print(f"Linhas não interpretadas: {stats['skipped_lines_count']}")
    print(f"Consultores encontrados: {stats['vendors_count']}")
    print(f"Duplicidades detectadas: {stats['duplicate_rows']}")
    print(f"Consultores no arquivo: {', '.join(stats['found_vendors'])}")

    if validation.warnings:
        print("\nAvisos:")
        for issue in validation.warnings:
            print(f"- [{issue.code}] {issue.message}")

    if validation.errors:
        print("\nErros:")
        for issue in validation.errors:
            print(f"- [{issue.code}] {issue.message}")


def print_credit_validation(validation) -> None:
    stats = validation.stats
    print("\n=== Validação da planilha de crédito ===")
    print(f"Aba de referência: {stats['sheet_name']}")
    print(f"Abas no arquivo: {stats.get('workbook_sheets_count', 0)}")
    print(f"Abas lidas: {stats.get('scanned_sheets_count', 0)}")
    print(f"Abas com seção de crédito: {stats.get('processed_sheets_count', 0)}")
    print(f"Linhas lidas: {stats['rows_scanned']}")
    print(f"Linhas candidatas: {stats['candidate_rows']}")
    print(
        f"Registros interpretados: {stats['records_count']} "
        f"({stats['parse_success_ratio'] * 100:.1f}%)"
    )
    print(f"Linhas ignoradas: {stats['skipped_rows_count']}")
    print(f"Consultores encontrados: {stats['vendors_count']}")
    print(f"Consultores na planilha: {', '.join(stats['found_vendors'])}")
    if stats.get("sheet_summaries"):
        print("Cobertura por aba:")
        for item in stats["sheet_summaries"]:
            section = "sim" if item["credit_section_detected"] else "nao"
            print(
                f"- {item['sheet_name']}: linhas={item['rows_scanned']}, "
                f"candidatas={item['candidate_rows']}, registros={item['records_count']}, "
                f"ignoradas={item['skipped_rows_count']}, consultores={item['consultants_count']}, "
                f"secao_credito={section}"
            )

    if validation.warnings:
        print("\nAvisos:")
        for issue in validation.warnings:
            print(f"- [{issue.code}] {issue.message}")

    if validation.errors:
        print("\nErros:")
        for issue in validation.errors:
            print(f"- [{issue.code}] {issue.message}")


if __name__ == "__main__":
    main()
