from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
import tempfile
import unicodedata

import pandas as pd
import plotly.express as px
import streamlit as st

from src.db import (
    AuthenticatedUser,
    authenticate_user,
    fetch_credit_limits_for_user,
    fetch_receivables_for_user,
    import_credit_limits,
    import_receivables,
    init_db,
    list_consultants,
)
from src.credit_excel_parser import parse_credit_excel
from src.metrics import (
    build_client_health,
    build_due_curve,
    build_enriched_receivables,
    build_guidance,
    build_portfolio_metrics,
    format_brl_from_cents,
    to_percentage,
)
from src.pdf_parser import parse_pdf
from src.update_validation import (
    EXPECTED_CONSULTANTS,
    validate_credit_parse_report,
    validate_parse_report,
)


st.set_page_config(
    page_title="Painel Financeiro por Consultor",
    page_icon="📊",
    layout="wide",
)

init_db()

DEFAULT_CONSULTANT_PASSWORD = "Consultor@123"


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg-main: #f6f4ef;
            --bg-card: #ffffff;
            --text-primary: #102a43;
            --text-soft: #486581;
            --accent: #e07a5f;
            --accent-2: #3d5a80;
            --ok: #2a9d8f;
            --warn: #f4a261;
            --danger: #d62828;
        }
        .stApp {
            background: radial-gradient(circle at 0% 0%, #f9efe5 0%, var(--bg-main) 55%, #f0efe9 100%);
        }
        .kpi-card {
            background: var(--bg-card);
            border-left: 6px solid var(--accent);
            border-radius: 12px;
            padding: 10px 14px;
            box-shadow: 0 4px 16px rgba(16, 42, 67, 0.08);
            min-height: 106px;
        }
        .kpi-label {
            color: var(--text-soft);
            font-size: 0.90rem;
            margin-bottom: 8px;
        }
        .kpi-value {
            color: var(--text-primary);
            font-size: 1.55rem;
            font-weight: 700;
        }
        .guidance-box {
            border-radius: 12px;
            border: 1px solid #dde3ea;
            padding: 14px;
            background: #ffffff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def get_current_user() -> AuthenticatedUser | None:
    raw_user = st.session_state.get("authenticated_user")
    if not raw_user:
        return None
    return AuthenticatedUser(**raw_user)


def do_logout() -> None:
    st.session_state.pop("authenticated_user", None)
    st.rerun()


def login_screen() -> None:
    st.title("Sistema Financeiro dos Consultores")
    st.caption(
        "Acesso por usuário para visualização isolada por carteira. "
        "Admin pode acompanhar todas as carteiras."
    )

    col_left, col_mid, col_right = st.columns([1, 1.2, 1])
    with col_mid:
        with st.form("login_form", border=True):
            username = st.text_input("Usuário")
            password = st.text_input("Senha", type="password")
            submitted = st.form_submit_button("Entrar", use_container_width=True)

        if submitted:
            user = authenticate_user(username=username, password=password)
            if user is None:
                st.error("Usuário ou senha inválidos.")
                return
            st.session_state["authenticated_user"] = asdict(user)
            st.rerun()

    with st.expander("Primeiro acesso e carga de dados"):
        st.markdown(
            """
            1. Rode o importador:
            `python scripts/import_pdf.py --pdf "C:\\Users\\Vitor\\Downloads\\relatorio geral (1).pdf" --excel "Z:\\vendedores_clientes_controle limite.xlsx"`
            2. Login de administrador (`admin` ou `adm`):
            `admin / Admin@123`
            3. Login dos consultores criados:
            senhas padrão `Consultor@123` (altere após implantação).
            """
        )


def render_pdf_validation(validation) -> None:
    st.write("**Validação do PDF (contas a receber)**")
    st.write(f"- Páginas: {validation.stats['pages_count']}")
    st.write(f"- Linhas candidatas: {validation.stats['candidate_lines_count']}")
    st.write(f"- Linhas interpretadas: {validation.stats['records_count']}")
    st.write(f"- Linhas não interpretadas: {validation.stats['skipped_lines_count']}")
    st.write(f"- Consultores encontrados: {validation.stats['vendors_count']}")

    if validation.warnings:
        st.warning("Avisos do PDF:")
        for issue in validation.warnings:
            st.write(f"- [{issue.code}] {issue.message}")

    if validation.errors:
        st.error("Erros do PDF:")
        for issue in validation.errors:
            st.write(f"- [{issue.code}] {issue.message}")


def render_credit_validation(validation) -> None:
    st.write("**Validação do Excel (leitura completa de abas)**")
    st.write(f"- Aba de referência: {validation.stats['sheet_name']}")
    st.write(
        f"- Abas no arquivo: {validation.stats.get('workbook_sheets_count', 0)}"
    )
    st.write(
        f"- Abas lidas: {validation.stats.get('scanned_sheets_count', 0)}"
    )
    st.write(
        f"- Abas com seção de crédito: {validation.stats.get('processed_sheets_count', 0)}"
    )
    st.write(f"- Linhas lidas: {validation.stats['rows_scanned']}")
    st.write(f"- Linhas candidatas: {validation.stats['candidate_rows']}")
    st.write(f"- Registros interpretados: {validation.stats['records_count']}")
    st.write(f"- Linhas ignoradas: {validation.stats['skipped_rows_count']}")
    st.write(f"- Consultores encontrados: {validation.stats['vendors_count']}")

    sheet_summaries = validation.stats.get("sheet_summaries", [])
    if sheet_summaries:
        st.write("**Cobertura por aba**")
        sheet_df = pd.DataFrame(sheet_summaries).rename(
            columns={
                "sheet_name": "Aba",
                "rows_scanned": "Linhas lidas",
                "candidate_rows": "Linhas candidatas",
                "records_count": "Registros",
                "skipped_rows_count": "Ignoradas",
                "consultants_count": "Consultores",
                "credit_section_detected": "Seção crédito",
            }
        )
        sheet_df["Seção crédito"] = sheet_df["Seção crédito"].map(
            lambda value: "Sim" if value else "Não"
        )
        st.dataframe(sheet_df, use_container_width=True, hide_index=True)

    if validation.warnings:
        st.warning("Avisos do Excel:")
        for issue in validation.warnings:
            st.write(f"- [{issue.code}] {issue.message}")

    if validation.errors:
        st.error("Erros do Excel:")
        for issue in validation.errors:
            st.write(f"- [{issue.code}] {issue.message}")


def admin_update_panel() -> None:
    with st.sidebar.expander("Atualização de dados (Admin)", expanded=False):
        with st.form("data_update_form", clear_on_submit=False):
            uploaded_pdf = st.file_uploader(
                "Relatório de contas a receber (PDF)",
                type=["pdf"],
                help="Layout padrão com vendedor, situação, saldo, parcela e vencimento.",
            )
            uploaded_excel = st.file_uploader(
                "Planilha de limite de crédito (Excel)",
                type=["xlsx", "xlsm"],
                help=(
                    "Todas as abas serão varridas; o sistema usa as seções de crédito "
                    "encontradas para atualizar a base."
                ),
            )
            action = st.radio(
                "Ação",
                options=[
                    "Somente validar arquivo",
                    "Validar e atualizar base",
                ],
                index=0,
            )
            strict_vendors = st.checkbox(
                "Exigir os 7 consultores esperados (modo estrito)",
                value=False,
            )
            append_mode = st.checkbox(
                "Acumular dados (append) em vez de substituir os existentes",
                value=False,
            )
            allow_skipped_lines = st.number_input(
                "Tolerância de linhas não interpretadas",
                min_value=0,
                max_value=100,
                value=0,
                step=1,
            )
            allow_skipped_credit_rows = st.number_input(
                "Tolerância de linhas ignoradas no Excel",
                min_value=0,
                max_value=1000,
                value=10,
                step=1,
            )
            submitted = st.form_submit_button("Executar validação/atualização")

        if not submitted:
            return
        if uploaded_pdf is None and uploaded_excel is None:
            st.error("Selecione ao menos um arquivo (PDF ou Excel) antes de executar.")
            return

        report = None
        validation = None
        credit_report = None
        credit_validation = None

        if uploaded_pdf is not None:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
                    temp_pdf.write(uploaded_pdf.getbuffer())
                    temp_pdf_path = Path(temp_pdf.name)

                report = parse_pdf(temp_pdf_path)
                validation = validate_parse_report(
                    report,
                    expected_vendor_names=EXPECTED_CONSULTANTS if strict_vendors else None,
                    strict_vendor_match=strict_vendors,
                    max_skipped_lines=int(allow_skipped_lines),
                )
            except Exception as exc:
                st.error(f"Falha ao validar PDF: {exc}")
                return
            finally:
                if "temp_pdf_path" in locals() and temp_pdf_path.exists():
                    temp_pdf_path.unlink(missing_ok=True)

        if uploaded_excel is not None:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_excel:
                    temp_excel.write(uploaded_excel.getbuffer())
                    temp_excel_path = Path(temp_excel.name)

                credit_report = parse_credit_excel(temp_excel_path)
                credit_validation = validate_credit_parse_report(
                    credit_report,
                    expected_vendor_names=EXPECTED_CONSULTANTS if strict_vendors else None,
                    strict_vendor_match=strict_vendors,
                    max_skipped_rows=int(allow_skipped_credit_rows),
                )
            except Exception as exc:
                st.error(f"Falha ao validar Excel: {exc}")
                return
            finally:
                if "temp_excel_path" in locals() and temp_excel_path.exists():
                    temp_excel_path.unlink(missing_ok=True)

        if validation is not None:
            render_pdf_validation(validation)
        if credit_validation is not None:
            render_credit_validation(credit_validation)
            stats = credit_validation.stats
            st.info(
                "Cobertura do Excel: "
                f"{stats.get('scanned_sheets_count', 0)}/{stats.get('workbook_sheets_count', 0)} abas lidas, "
                f"{stats.get('processed_sheets_count', 0)} com seção de crédito."
            )

        validations = [item for item in [validation, credit_validation] if item is not None]
        all_valid = all(item.is_valid for item in validations)

        if action == "Somente validar arquivo":
            if all_valid:
                st.success("Validação concluída com sucesso para os arquivos enviados.")
            else:
                st.error("Validação concluída com erros. Ajuste os arquivos para importar.")
            return

        if not all_valid:
            st.error("Atualização cancelada: um ou mais arquivos não passaram na validação.")
            return

        if report is not None and uploaded_pdf is not None:
            import_summary = import_receivables(
                report.records,
                source_file=uploaded_pdf.name,
                default_password=DEFAULT_CONSULTANT_PASSWORD,
                wipe_existing=not append_mode,
            )
            st.success(
                "PDF importado com sucesso. "
                f"Importados: {import_summary.imported_rows} | Duplicados: {import_summary.duplicated_rows}"
            )

        if credit_report is not None and uploaded_excel is not None:
            credit_summary = import_credit_limits(
                credit_report.records,
                source_file=uploaded_excel.name,
                wipe_existing=not append_mode,
            )
            processed_at = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            st.success(
                "Excel importado com sucesso. "
                f"Inseridos: {credit_summary.imported_rows} | Atualizados: {credit_summary.updated_rows} | Ignorados: {credit_summary.skipped_rows}"
            )
            st.write(
                f"- Protocolo de atualização: {processed_at} | arquivo `{uploaded_excel.name}`"
            )
            st.write(
                f"- Abas lidas: {len(credit_report.scanned_sheets)} de {len(credit_report.workbook_sheets)}"
            )
            st.write(
                f"- Abas com seção de crédito: {len(credit_report.processed_sheets)}"
            )
            if credit_summary.unresolved_consultants:
                st.warning("Consultores não resolvidos na planilha de crédito:")
                for consultant_name, count in sorted(credit_summary.unresolved_consultants.items()):
                    st.write(f"- {consultant_name}: {count} registro(s)")


def normalize_customer_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_name.upper().replace("_", " ").split())


def classify_credit_alert(exposure_cents: int, available_cents: int) -> str:
    if exposure_cents <= 0 and available_cents > 0:
        return "Sem exposição"
    if exposure_cents > 0 and available_cents <= 0:
        return "Sem limite livre"
    if available_cents <= 0:
        return "Sem dados"
    ratio = exposure_cents / available_cents
    if ratio > 1:
        return "Acima do limite"
    if ratio >= 0.8:
        return "Atenção"
    return "Controlado"


def render_credit_limits_section(
    *,
    credit_df: pd.DataFrame,
    client_health: pd.DataFrame,
    total_balance_cents: int,
    show_consultant: bool,
) -> None:
    st.subheader("Limite de crédito por cliente")
    if credit_df.empty:
        st.info("Sem dados de limite de crédito para o escopo selecionado.")
        return

    work = credit_df.copy()
    for column in ["credit_limit_cents", "credit_used_cents", "credit_available_cents"]:
        work[column] = pd.to_numeric(work[column], errors="coerce").fillna(0).astype(int)

    work["customer_key"] = work["customer_name"].map(normalize_customer_key)

    if not client_health.empty:
        health = client_health.copy()
        health["customer_key"] = health["customer_name"].map(normalize_customer_key)
        health = health.groupby("customer_key", as_index=False).agg(
            exposure_cents=("total_balance_cents", "sum"),
            overdue_cents=("overdue_cents", "sum"),
        )
        work = work.merge(health, on="customer_key", how="left")
    else:
        work["exposure_cents"] = 0
        work["overdue_cents"] = 0

    work["exposure_cents"] = pd.to_numeric(work["exposure_cents"], errors="coerce").fillna(0).astype(int)
    work["overdue_cents"] = pd.to_numeric(work["overdue_cents"], errors="coerce").fillna(0).astype(int)

    total_limit = int(work["credit_limit_cents"].sum())
    total_used = int(work["credit_used_cents"].sum())
    total_available = int(work["credit_available_cents"].sum())
    exposure_vs_available_ratio = (
        total_balance_cents / total_available if total_available > 0 else 0.0
    )

    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    with kpi_col1:
        kpi_card("Limite total", format_brl_from_cents(total_limit))
    with kpi_col2:
        kpi_card("Crédito utilizado", format_brl_from_cents(total_used))
    with kpi_col3:
        kpi_card("Crédito disponível", format_brl_from_cents(total_available))
    with kpi_col4:
        kpi_card("Exposição / Disponível", to_percentage(exposure_vs_available_ratio))

    work["usage_ratio"] = work.apply(
        lambda row: (row["credit_used_cents"] / row["credit_limit_cents"])
        if row["credit_limit_cents"] > 0
        else 0.0,
        axis=1,
    )
    work["exposure_vs_available"] = work.apply(
        lambda row: (row["exposure_cents"] / row["credit_available_cents"])
        if row["credit_available_cents"] > 0
        else 0.0,
        axis=1,
    )
    work["credit_alert"] = work.apply(
        lambda row: classify_credit_alert(row["exposure_cents"], row["credit_available_cents"]),
        axis=1,
    )

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        composition_df = pd.DataFrame(
            [
                {"Categoria": "Crédito utilizado", "Valor": total_used / 100},
                {"Categoria": "Crédito disponível", "Valor": total_available / 100},
            ]
        )
        fig_composition = px.pie(
            composition_df,
            names="Categoria",
            values="Valor",
            hole=0.5,
            color="Categoria",
            color_discrete_map={
                "Crédito utilizado": "#e07a5f",
                "Crédito disponível": "#2a9d8f",
            },
        )
        fig_composition.update_layout(
            margin=dict(l=10, r=10, t=30, b=10),
            title="Composição do crédito total",
        )
        st.plotly_chart(fig_composition, use_container_width=True)

    with chart_col2:
        top_usage = (
            work.sort_values(["usage_ratio", "credit_used_cents"], ascending=[False, False])
            .head(12)[["customer_name", "usage_ratio"]]
            .copy()
        )
        top_usage["usage_percent"] = top_usage["usage_ratio"] * 100
        fig_usage = px.bar(
            top_usage.sort_values("usage_percent", ascending=True),
            x="usage_percent",
            y="customer_name",
            orientation="h",
            color_discrete_sequence=["#3d5a80"],
        )
        fig_usage.update_layout(
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="% do limite utilizado",
            yaxis_title="Cliente",
            title="Clientes com maior ocupação de limite",
        )
        st.plotly_chart(fig_usage, use_container_width=True)

    display = work.copy()
    display["Limite"] = display["credit_limit_cents"].map(format_brl_from_cents)
    display["Utilizado"] = display["credit_used_cents"].map(format_brl_from_cents)
    display["Disponível"] = display["credit_available_cents"].map(format_brl_from_cents)
    display["Saldo em aberto"] = display["exposure_cents"].map(format_brl_from_cents)
    display["Vencido"] = display["overdue_cents"].map(format_brl_from_cents)
    display["Uso do limite"] = display["usage_ratio"].map(to_percentage)
    display["Exposição/Disponível"] = display["exposure_vs_available"].map(to_percentage)
    display["Situação crédito"] = display["credit_alert"]
    display["Política"] = display["limit_policy"].fillna("-")
    display["Observação"] = display["note"].fillna("-")
    updated_at = pd.to_datetime(display["updated_at"], errors="coerce")
    display["Atualizado em"] = updated_at.dt.strftime("%d/%m/%Y %H:%M").fillna("-")

    column_map = {
        "consultant_name": "Consultor",
        "customer_name": "Cliente",
        "cnpj": "CNPJ",
        "Saldo em aberto": "Saldo em aberto",
        "Vencido": "Vencido",
        "Limite": "Limite",
        "Utilizado": "Utilizado",
        "Disponível": "Disponível",
        "Uso do limite": "Uso do limite",
        "Exposição/Disponível": "Exposição/Disponível",
        "Situação crédito": "Situação crédito",
        "Política": "Política",
        "Observação": "Observação",
        "Atualizado em": "Atualizado em",
    }
    table_columns = [
        "customer_name",
        "cnpj",
        "Saldo em aberto",
        "Vencido",
        "Limite",
        "Utilizado",
        "Disponível",
        "Uso do limite",
        "Exposição/Disponível",
        "Situação crédito",
        "Política",
        "Observação",
        "Atualizado em",
    ]
    if show_consultant:
        table_columns.insert(0, "consultant_name")

    renamed = display.rename(columns=column_map)
    renamed_columns = [column_map.get(column, column) for column in table_columns]
    st.dataframe(
        renamed[renamed_columns],
        use_container_width=True,
        hide_index=True,
    )


def dashboard_screen(user: AuthenticatedUser) -> None:
    st.sidebar.title("Sessão")
    st.sidebar.write(f"**Usuário:** {user.name}")
    st.sidebar.write(f"**Perfil:** {'Administrador' if user.is_admin else 'Consultor'}")
    if user.is_admin:
        admin_update_panel()
    if st.sidebar.button("Sair"):
        do_logout()

    selected_consultant_id: int | None = None
    portfolio_title = f"Carteira de {user.name}"

    if user.is_admin:
        consultants = [c for c in list_consultants() if not c["is_admin"]]
        labels = ["Todas as carteiras"] + [f'{c["name"]} ({c["username"]})' for c in consultants]
        selected = st.sidebar.selectbox("Escopo da análise", labels)
        if selected != "Todas as carteiras":
            selected_consultant = consultants[labels.index(selected) - 1]
            selected_consultant_id = int(selected_consultant["id"])
            portfolio_title = f'Carteira de {selected_consultant["name"]}'
        else:
            portfolio_title = "Todas as carteiras"

    rows = fetch_receivables_for_user(
        user=user,
        selected_consultant_id=selected_consultant_id,
    )
    credit_rows = fetch_credit_limits_for_user(
        user=user,
        selected_consultant_id=selected_consultant_id,
    )
    df = pd.DataFrame(rows)
    credit_df = pd.DataFrame(credit_rows)
    if df.empty and credit_df.empty:
        st.title("Sem dados financeiros carregados")
        st.info(
            "Importe PDF e/ou Excel para preencher o sistema. "
            "Use o script `python scripts/import_pdf.py --pdf <arquivo.pdf> --excel <arquivo.xlsx>` "
            "ou o painel de atualização do admin."
        )
        return

    if df.empty:
        st.title("Painel de Saúde Financeira")
        st.caption(portfolio_title)
        st.info("Sem títulos do PDF para o escopo selecionado. Exibindo limites de crédito importados.")
        render_credit_limits_section(
            credit_df=credit_df,
            client_health=pd.DataFrame(),
            total_balance_cents=0,
            show_consultant=user.is_admin and selected_consultant_id is None,
        )
        return

    enriched = build_enriched_receivables(df, today=date.today())
    metrics = build_portfolio_metrics(enriched)
    client_health = build_client_health(enriched)
    due_curve = build_due_curve(enriched)
    guidance = build_guidance(metrics, client_health)

    st.title("Painel de Saúde Financeira")
    st.caption(portfolio_title)

    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    with kpi_col1:
        kpi_card("Saldo em aberto", format_brl_from_cents(metrics["total_balance_cents"]))
    with kpi_col2:
        kpi_card("Vence em 7 dias", format_brl_from_cents(metrics["due_7_cents"]))
    with kpi_col3:
        kpi_card("Vence em 30 dias", format_brl_from_cents(metrics["due_30_cents"]))
    with kpi_col4:
        kpi_card("Saldo vencido", format_brl_from_cents(metrics["overdue_cents"]))
    with kpi_col5:
        kpi_card("Clientes", str(metrics["clients_count"]))

    overdue_ratio = (
        metrics["overdue_cents"] / metrics["total_balance_cents"]
        if metrics["total_balance_cents"] > 0
        else 0.0
    )
    due_7_ratio = (
        metrics["due_7_cents"] / metrics["total_balance_cents"]
        if metrics["total_balance_cents"] > 0
        else 0.0
    )

    st.markdown(
        f"""
        <div class="guidance-box">
            <b>Leitura estratégica:</b> {guidance}<br>
            <b>Risco de inadimplência atual:</b> {to_percentage(overdue_ratio)} |
            <b>Pressão de curto prazo (7 dias):</b> {to_percentage(due_7_ratio)}
        </div>
        """,
        unsafe_allow_html=True,
    )

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.subheader("Curva de vencimentos")
        due_curve_plot = due_curve.copy()
        due_curve_plot["total_balance"] = due_curve_plot["total_balance_cents"] / 100
        fig_due = px.area(
            due_curve_plot,
            x="due_date",
            y="total_balance",
            markers=True,
            color_discrete_sequence=["#3d5a80"],
        )
        fig_due.update_layout(
            yaxis_title="Saldo (R$)",
            xaxis_title="Data de vencimento",
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(fig_due, use_container_width=True)

    with chart_col2:
        st.subheader("Distribuição por status financeiro")
        status_dist = (
            client_health.groupby("financial_status", as_index=False)["total_balance_cents"]
            .sum()
            .sort_values("total_balance_cents", ascending=False)
        )
        if status_dist.empty:
            st.info("Sem dados para distribuição de status.")
        else:
            fig_status = px.pie(
                status_dist,
                names="financial_status",
                values="total_balance_cents",
                hole=0.55,
                color="financial_status",
                color_discrete_map={
                    "Saudável": "#2a9d8f",
                    "Atenção": "#f4a261",
                    "Crítico": "#d62828",
                },
            )
            fig_status.update_layout(margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(fig_status, use_container_width=True)

    st.subheader("Top clientes por exposição")
    top_clients = (
        client_health.head(12)[["customer_name", "total_balance_cents"]]
        .sort_values("total_balance_cents", ascending=True)
        .rename(columns={"customer_name": "Cliente", "total_balance_cents": "Saldo"})
    )
    top_clients["Saldo"] = top_clients["Saldo"] / 100
    fig_top = px.bar(
        top_clients,
        x="Saldo",
        y="Cliente",
        orientation="h",
        color_discrete_sequence=["#e07a5f"],
    )
    fig_top.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig_top, use_container_width=True)

    st.subheader("Diagnóstico por cliente")
    diagnosis = client_health.copy()
    diagnosis["total_balance_cents"] = diagnosis["total_balance_cents"].map(format_brl_from_cents)
    diagnosis["overdue_cents"] = diagnosis["overdue_cents"].map(format_brl_from_cents)
    diagnosis["due_7_cents"] = diagnosis["due_7_cents"].map(format_brl_from_cents)
    diagnosis["due_30_cents"] = diagnosis["due_30_cents"].map(format_brl_from_cents)
    diagnosis = diagnosis.rename(
        columns={
            "customer_name": "Cliente",
            "customer_code": "Código",
            "total_balance_cents": "Saldo em aberto",
            "overdue_cents": "Vencido",
            "due_7_cents": "Vence 7 dias",
            "due_30_cents": "Vence 30 dias",
            "titles_count": "Qtde. títulos",
            "risk_score": "Score",
            "financial_status": "Situação",
            "recommended_action": "Ação recomendada",
        }
    )
    st.dataframe(
        diagnosis[
            [
                "Cliente",
                "Código",
                "Saldo em aberto",
                "Vencido",
                "Vence 7 dias",
                "Vence 30 dias",
                "Qtde. títulos",
                "Score",
                "Situação",
                "Ação recomendada",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Títulos detalhados")
    details = enriched.copy()
    if not user.is_admin:
        details = details.drop(columns=["consultant_name", "consultant_username", "consultant_id"], errors="ignore")

    details["issue_date"] = details["issue_date"].dt.strftime("%d/%m/%Y")
    details["due_date"] = details["due_date"].dt.strftime("%d/%m/%Y")
    details["balance"] = details["balance_cents"].map(format_brl_from_cents)
    details["installment_value"] = details["installment_value_cents"].map(format_brl_from_cents)

    display_columns = [
        "consultant_name",
        "customer_name",
        "customer_code",
        "status",
        "document_id",
        "document_ref",
        "installment",
        "issue_date",
        "due_date",
        "balance",
        "installment_value",
    ]
    if not user.is_admin:
        display_columns.remove("consultant_name")

    column_map = {
        "consultant_name": "Consultor",
        "customer_name": "Cliente",
        "customer_code": "Código",
        "status": "Situação",
        "document_id": "ID",
        "document_ref": "Documento",
        "installment": "Parcela",
        "issue_date": "Emissão",
        "due_date": "Vencto",
        "balance": "Saldo",
        "installment_value": "Vlr. Parcela",
    }
    renamed = details.rename(columns=column_map)
    display_columns_renamed = [column_map[column] for column in display_columns]
    st.dataframe(
        renamed[display_columns_renamed],
        use_container_width=True,
        hide_index=True,
    )

    render_credit_limits_section(
        credit_df=credit_df,
        client_health=client_health,
        total_balance_cents=metrics["total_balance_cents"],
        show_consultant=user.is_admin and selected_consultant_id is None,
    )


def main() -> None:
    inject_styles()
    user = get_current_user()
    if user is None:
        login_screen()
        return
    dashboard_screen(user)


if __name__ == "__main__":
    main()
