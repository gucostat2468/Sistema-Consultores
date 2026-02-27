from __future__ import annotations

from datetime import date
import math

import pandas as pd


def build_enriched_receivables(df: pd.DataFrame, *, today: date | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    current_day = today or date.today()
    enriched = df.copy()
    enriched["issue_date"] = pd.to_datetime(enriched["issue_date"], errors="coerce")
    enriched["due_date"] = pd.to_datetime(enriched["due_date"], errors="coerce")
    enriched["days_to_due"] = (enriched["due_date"].dt.date - current_day).map(
        lambda value: None if pd.isna(value) else value.days
    )
    status_lower = enriched["status"].str.lower()
    enriched["is_overdue"] = status_lower.str.contains("vencido", na=False) | (
        enriched["days_to_due"] < 0
    )
    enriched["due_in_7"] = enriched["days_to_due"].between(0, 7, inclusive="both")
    enriched["due_in_30"] = enriched["days_to_due"].between(0, 30, inclusive="both")
    enriched["overdue_balance_cents"] = enriched["balance_cents"].where(enriched["is_overdue"], 0)
    enriched["due_7_balance_cents"] = enriched["balance_cents"].where(enriched["due_in_7"], 0)
    enriched["due_30_balance_cents"] = enriched["balance_cents"].where(enriched["due_in_30"], 0)
    return enriched


def build_portfolio_metrics(df: pd.DataFrame) -> dict[str, int]:
    if df.empty:
        return {
            "total_balance_cents": 0,
            "overdue_cents": 0,
            "due_7_cents": 0,
            "due_30_cents": 0,
            "clients_count": 0,
            "titles_count": 0,
        }

    return {
        "total_balance_cents": int(df["balance_cents"].sum()),
        "overdue_cents": int(df["overdue_balance_cents"].sum()),
        "due_7_cents": int(df["due_7_balance_cents"].sum()),
        "due_30_cents": int(df["due_30_balance_cents"].sum()),
        "clients_count": int(df["customer_name"].nunique()),
        "titles_count": int(len(df)),
    }


def build_client_health(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "customer_name",
                "customer_code",
                "total_balance_cents",
                "overdue_cents",
                "due_7_cents",
                "due_30_cents",
                "titles_count",
                "overdue_ratio",
                "due_7_ratio",
                "due_15_ratio",
                "due_30_ratio",
                "severe_overdue_ratio",
                "risk_score",
                "financial_status",
                "recommended_action",
            ]
        )

    records: list[dict] = []

    for (customer_name, customer_code), group in df.groupby(["customer_name", "customer_code"]):
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
        due_7_ratio = safe_ratio(due_7_cents, total_balance_cents)
        due_15_ratio = safe_ratio(due_15_cents, total_balance_cents)
        due_30_ratio = safe_ratio(due_30_cents, total_balance_cents)
        severe_overdue_ratio = safe_ratio(
            int(round(overdue_8_30_cents + overdue_31_plus_cents)),
            total_balance_cents,
        )

        overdue_titles_count = int(overdue_mask.sum())
        due_15_titles_count = int(due_15_mask.sum())
        max_title_cents = float(balances.max()) if not balances.empty else 0.0
        max_title_share = (
            max_title_cents / total_balance_cents if total_balance_cents > 0 else 0.0
        )
        weighted_mean_days = (
            float((balances * days.clip(lower=-45, upper=60)).sum() / total_balance_cents)
            if total_balance_cents > 0
            else 60.0
        )

        overdue_severity = (
            clamp(
                (
                    overdue_1_7_cents
                    + overdue_8_30_cents * 1.4
                    + overdue_31_plus_cents * 1.9
                )
                / total_balance_cents,
                0.0,
                1.0,
            )
            if total_balance_cents > 0
            else 0.0
        )
        near_pressure = (
            clamp(
                (
                    due_7_cents
                    + due_8_15_cents * 0.65
                    + due_16_30_cents * 0.35
                )
                / total_balance_cents,
                0.0,
                1.0,
            )
            if total_balance_cents > 0
            else 0.0
        )
        title_stress = (
            clamp(
                (overdue_titles_count / titles_count) * 0.75
                + (due_15_titles_count / titles_count) * 0.25,
                0.0,
                1.0,
            )
            if titles_count > 0
            else 0.0
        )
        concentration_risk = clamp((max_title_share - 0.35) / 0.65, 0.0, 1.0)
        trajectory_risk = clamp((12.0 - weighted_mean_days) / 57.0, 0.0, 1.0)

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
        risk_score = int(round(clamp(100.0 - base_penalty - escalation_penalty, 0.0, 100.0)))

        financial_status = classify_financial_status(
            risk_score=risk_score,
            overdue_ratio=overdue_ratio,
            due_15_ratio=due_15_ratio,
            severe_overdue_ratio=severe_overdue_ratio,
        )

        records.append(
            {
                "customer_name": customer_name,
                "customer_code": customer_code,
                "total_balance_cents": total_balance_cents,
                "overdue_cents": overdue_cents,
                "due_7_cents": due_7_cents,
                "due_30_cents": due_30_cents,
                "titles_count": titles_count,
                "overdue_ratio": overdue_ratio,
                "due_7_ratio": due_7_ratio,
                "due_15_ratio": due_15_ratio,
                "due_30_ratio": due_30_ratio,
                "severe_overdue_ratio": severe_overdue_ratio,
                "risk_score": risk_score,
                "financial_status": financial_status,
                "recommended_action": {
                    "Crítico": "Priorizar contato imediato e renegociar antes de novas propostas.",
                    "Atenção": "Fazer abordagem preventiva e validar limite para próximos negócios.",
                    "Saudável": "Cliente apto para novas estratégias comerciais com monitoramento padrão.",
                }[financial_status],
            }
        )

    return (
        pd.DataFrame(records)
        .sort_values("total_balance_cents", ascending=False)
        .reset_index(drop=True)
    )


def classify_financial_status(
    *,
    risk_score: int,
    overdue_ratio: float,
    due_15_ratio: float,
    severe_overdue_ratio: float,
) -> str:
    if risk_score < 45 or overdue_ratio >= 0.22 or severe_overdue_ratio >= 0.12:
        return "Crítico"
    if risk_score < 72 or overdue_ratio >= 0.08 or due_15_ratio >= 0.38:
        return "Atenção"
    return "Saudável"


def build_due_curve(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["due_date", "total_balance_cents"])
    curve = (
        df.groupby(df["due_date"].dt.date, as_index=False)["balance_cents"]
        .sum()
        .rename(columns={"balance_cents": "total_balance_cents", "due_date": "due_date"})
        .sort_values("due_date")
    )
    return curve


def build_guidance(metrics: dict[str, int], client_health: pd.DataFrame) -> str:
    if metrics["total_balance_cents"] == 0:
        return "Sem carteira carregada. Importe o PDF para gerar análises."

    critical = int((client_health["financial_status"] == "Crítico").sum()) if not client_health.empty else 0
    attention = int((client_health["financial_status"] == "Atenção").sum()) if not client_health.empty else 0
    due_7_ratio = safe_ratio(metrics["due_7_cents"], metrics["total_balance_cents"])
    overdue_ratio = safe_ratio(metrics["overdue_cents"], metrics["total_balance_cents"])

    if critical > 0 or overdue_ratio > 0.1:
        return "Prioridade de cobrança: há clientes em faixa crítica. Concentre contato e renegociação antes de ampliar crédito."
    if attention > 0 or due_7_ratio > 0.35:
        return "Carteira sob atenção: alinhar contatos preventivos para vencimentos próximos e revisar limites por cliente."
    return "Carteira saudável: cenário favorável para propostas comerciais, mantendo monitoramento semanal."


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def format_brl_from_cents(cents: int) -> str:
    absolute = abs(cents)
    integer = absolute // 100
    decimal = absolute % 100
    integer_with_sep = f"{integer:,}".replace(",", ".")
    sign = "-" if cents < 0 else ""
    return f"{sign}R$ {integer_with_sep},{decimal:02d}"


def to_percentage(value: float) -> str:
    if math.isnan(value):
        value = 0.0
    return f"{value * 100:.1f}%"
