import { CommonModule } from '@angular/common';
import { Component, DestroyRef, inject, signal } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { distinctUntilChanged, forkJoin, map, switchMap } from 'rxjs';
import {
  ClientHealth,
  CreditAlert,
  CreditLimitItem,
  CreditSummary,
  PortfolioSummary,
  ReceivableItem
} from '../../../../core/models/dashboard.models';
import { DashboardService } from '../../../../core/services/dashboard.service';
import { daysUntil, toCurrency, toDate, toPercent } from '../../../../shared/utils/format';

interface ClientStatement {
  client: ClientHealth;
  overdueTitles: ReceivableItem[];
  upcomingTitles: ReceivableItem[];
  overdueAmount: number;
  upcomingAmount: number;
  availableBalance: number;
}

@Component({
  selector: 'app-dashboard-page',
  imports: [CommonModule],
  templateUrl: './dashboard.page.html',
  styleUrl: './dashboard.page.scss'
})
export class DashboardPage {
  private readonly route = inject(ActivatedRoute);
  private readonly dashboardService = inject(DashboardService);
  private readonly destroyRef = inject(DestroyRef);

  readonly loading = signal(true);
  readonly error = signal<string | null>(null);
  readonly exportError = signal<string | null>(null);
  readonly selectedConsultantId = signal<number | null>(null);

  readonly summary = signal<PortfolioSummary | null>(null);
  readonly clientHealth = signal<ClientHealth[]>([]);
  readonly receivables = signal<ReceivableItem[]>([]);
  readonly creditSummary = signal<CreditSummary | null>(null);
  readonly creditItems = signal<CreditLimitItem[]>([]);
  readonly statusDistribution = signal<Array<{ label: string; value: number; ratio: number }>>([]);

  constructor() {
    this.route.queryParamMap
      .pipe(
        map((params) => {
          const value = params.get('consultantId');
          const consultantId = value ? Number(value) : null;
          this.selectedConsultantId.set(consultantId);
          return consultantId;
        }),
        distinctUntilChanged(),
        switchMap((consultantId) => {
          this.loading.set(true);
          this.error.set(null);
          return forkJoin({
            summary: this.dashboardService.getPortfolioSummary(consultantId),
            clientHealth: this.dashboardService.getClientHealth(consultantId),
            receivables: this.dashboardService.getReceivables(consultantId),
            creditLimits: this.dashboardService.getCreditLimits(consultantId)
          });
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ({ summary, clientHealth, receivables, creditLimits }) => {
          this.summary.set(summary);
          this.clientHealth.set(clientHealth);
          this.receivables.set(receivables);
          this.creditSummary.set(creditLimits.summary);
          this.creditItems.set(creditLimits.items);
          this.statusDistribution.set(this.buildStatusDistribution(clientHealth));
          this.loading.set(false);
        },
        error: () => {
          this.error.set('Nao foi possivel carregar o painel agora.');
          this.loading.set(false);
        }
      });
  }

  get topClients(): ClientHealth[] {
    return this.clientHealth().slice(0, 6);
  }

  get criticalClients(): ClientHealth[] {
    return this.clientHealth()
      .filter((item) => item.status !== 'Saudavel')
      .slice(0, 5);
  }

  get nextDue(): ReceivableItem[] {
    return this.receivables()
      .filter((item) => daysUntil(item.dueDate) >= -5)
      .slice(0, 8);
  }

  get topCreditCross(): CreditLimitItem[] {
    return this.creditItems().slice(0, 8);
  }

  get reportScopeLabel(): string {
    const names = Array.from(new Set(this.receivables().map((item) => item.consultantName)));
    if (names.length === 1) {
      return `Carteira do consultor: ${names[0]}`;
    }
    if (names.length > 1) {
      return 'Carteira consolidada de consultores';
    }
    if (this.selectedConsultantId()) {
      return `Carteira do consultor #${this.selectedConsultantId()}`;
    }
    return 'Carteira';
  }

  get clientStatements(): ClientStatement[] {
    const byClient = new Map<string, ReceivableItem[]>();
    for (const item of this.receivables()) {
      const key = this.buildClientKey(item.consultantId, item.customerCode);
      const current = byClient.get(key) ?? [];
      current.push(item);
      byClient.set(key, current);
    }

    return this.clientHealth().map((client) => {
      const key = this.buildClientKey(client.consultantId, client.customerCode);
      const titles = (byClient.get(key) ?? []).sort((a, b) => a.dueDate.localeCompare(b.dueDate));
      const overdueTitles = titles.filter((item) => daysUntil(item.dueDate) < 0);
      const upcomingTitles = titles.filter((item) => daysUntil(item.dueDate) >= 0);
      const overdueAmount = sum(overdueTitles.map((item) => item.balance));
      const upcomingAmount = sum(upcomingTitles.map((item) => item.balance));

      return {
        client,
        overdueTitles,
        upcomingTitles,
        overdueAmount,
        upcomingAmount,
        availableBalance: upcomingAmount
      };
    });
  }

  badgeClass(status: ClientHealth['status']): string {
    return `status status-${status.toLowerCase()}`;
  }

  creditAlertClass(alert: CreditAlert): string {
    if (alert === 'Acima do limite' || alert === 'Sem limite livre') {
      return 'credit-alert credit-alert-high';
    }
    if (alert === 'Atencao') {
      return 'credit-alert credit-alert-medium';
    }
    if (alert === 'Controlado' || alert === 'Sem exposicao') {
      return 'credit-alert credit-alert-low';
    }
    return 'credit-alert credit-alert-neutral';
  }

  creditAlertLabel(alert: CreditAlert): string {
    if (alert === 'Acima do limite') {
      return 'Acima do limite';
    }
    if (alert === 'Sem limite livre') {
      return 'Sem limite livre';
    }
    if (alert === 'Atencao') {
      return 'Atencao';
    }
    if (alert === 'Controlado') {
      return 'Controlado';
    }
    if (alert === 'Sem exposicao') {
      return 'Sem exposicao';
    }
    return 'Sem dados';
  }

  urgencyClass(item: ReceivableItem): string {
    const days = daysUntil(item.dueDate);
    if (days < 0) {
      return 'urgency-overdue';
    }
    if (days <= 7) {
      return 'urgency-near';
    }
    return 'urgency-safe';
  }

  dueToneTextClass(item: ReceivableItem): string {
    const days = daysUntil(item.dueDate);
    if (days < 0) {
      return 'due-text-overdue';
    }
    if (days <= 7) {
      return 'due-text-near';
    }
    return 'due-text-safe';
  }

  dueLabel(item: ReceivableItem): string {
    const days = daysUntil(item.dueDate);
    if (days < 0) {
      return `${Math.abs(days)} dia(s) em atraso`;
    }
    if (days === 0) {
      return 'vence hoje';
    }
    return `vence em ${days} dia(s)`;
  }

  currency(value: number): string {
    return toCurrency(value);
  }

  percent(value: number): string {
    return toPercent(value);
  }

  date(value: string): string {
    return toDate(value);
  }

  exportPortfolioPdf(): void {
    const metrics = this.summary();
    if (!metrics) {
      return;
    }

    this.exportError.set(null);

    const popup = window.open('', '_blank');
    if (!popup) {
      this.exportError.set('Libere pop-up no navegador para exportar o PDF.');
      return;
    }

    const html = this.buildPdfDocumentHtml(
      metrics,
      this.statusDistribution(),
      this.clientStatements,
      this.reportScopeLabel
    );
    popup.document.open();
    popup.document.write(html);
    popup.document.close();
    popup.focus();
    setTimeout(() => popup.print(), 350);
  }

  private buildStatusDistribution(
    clientHealth: ClientHealth[]
  ): Array<{ label: string; value: number; ratio: number }> {
    const statuses: Array<ClientHealth['status']> = ['Saudavel', 'Atencao', 'Critico'];
    const total = clientHealth.length || 1;
    return statuses.map((status) => {
      const value = clientHealth.filter((item) => item.status === status).length;
      return {
        label: status,
        value,
        ratio: value / total
      };
    });
  }

  private buildPdfDocumentHtml(
    metrics: PortfolioSummary,
    distribution: Array<{ label: string; value: number; ratio: number }>,
    statements: ClientStatement[],
    scopeLabel: string
  ): string {
    const generatedAt = new Date();
    const distributionRows = distribution
      .map(
        (item) => `
          <div class="distribution-row">
            <span>${this.escapeHtml(item.label)}</span>
            <div class="bar-track"><div class="bar-fill" style="width: ${(item.ratio * 100).toFixed(1)}%"></div></div>
            <strong>${item.value}</strong>
          </div>
        `
      )
      .join('');

    const clientSections = statements
      .map((statement, index) => this.buildClientSectionHtml(statement, index + 1))
      .join('');

    return `
      <!doctype html>
      <html lang="pt-BR">
        <head>
          <meta charset="utf-8" />
          <title>Extrato Financeiro da Carteira</title>
          <style>
            :root {
              --line: #d9ddd6;
              --text-main: #13312e;
              --text-soft: #53706d;
              --ok: #27816c;
              --warn: #f1992f;
              --danger: #cb2f2f;
            }
            * { box-sizing: border-box; }
            body {
              margin: 0;
              padding: 22px;
              color: var(--text-main);
              font-family: "Segoe UI", Arial, sans-serif;
              background: #fff;
            }
            h1, h2, h3, h4, p { margin: 0; }
            .header {
              border: 1px solid var(--line);
              border-radius: 14px;
              padding: 14px;
              margin-bottom: 14px;
            }
            .header h1 {
              font-size: 21px;
              margin-bottom: 4px;
            }
            .header p {
              color: var(--text-soft);
              font-size: 12px;
              margin-top: 3px;
            }
            .kpi-grid {
              display: grid;
              grid-template-columns: repeat(3, minmax(0, 1fr));
              gap: 8px;
              margin-bottom: 12px;
            }
            .kpi {
              border: 1px solid var(--line);
              border-radius: 12px;
              padding: 10px;
            }
            .kpi p {
              color: var(--text-soft);
              font-size: 11px;
            }
            .kpi strong {
              display: block;
              margin-top: 4px;
              font-size: 15px;
            }
            .dashboard-card {
              border: 1px solid var(--line);
              border-radius: 12px;
              padding: 10px;
              margin-bottom: 12px;
            }
            .dashboard-card h3 {
              font-size: 13px;
              margin-bottom: 6px;
            }
            .distribution-row {
              display: grid;
              grid-template-columns: 90px 1fr auto;
              gap: 8px;
              align-items: center;
              margin-top: 7px;
              font-size: 12px;
            }
            .bar-track {
              height: 8px;
              border-radius: 999px;
              overflow: hidden;
              background: #edf1ec;
            }
            .bar-fill {
              height: 100%;
              background: linear-gradient(120deg, #f6a032 0%, #2d8c7f 100%);
            }
            .client-section {
              border: 1px solid var(--line);
              border-radius: 12px;
              padding: 12px;
              margin-top: 12px;
              break-inside: avoid;
              page-break-inside: avoid;
            }
            .client-title {
              display: flex;
              justify-content: space-between;
              gap: 12px;
              align-items: flex-start;
            }
            .client-title h3 { font-size: 15px; }
            .client-title p {
              color: var(--text-soft);
              margin-top: 2px;
              font-size: 11px;
            }
            .status {
              border-radius: 999px;
              padding: 3px 9px;
              font-size: 11px;
              font-weight: 700;
              white-space: nowrap;
            }
            .status-saudavel { background: #e7f7f0; color: #11624d; }
            .status-atencao { background: #fff1dd; color: #9c5600; }
            .status-critico { background: #fde9e9; color: #a22929; }
            .client-kpis {
              display: grid;
              grid-template-columns: repeat(4, minmax(0, 1fr));
              gap: 7px;
              margin-top: 9px;
            }
            .mini-kpi {
              border: 1px solid #e7ebe5;
              border-radius: 10px;
              padding: 8px;
            }
            .mini-kpi p {
              color: var(--text-soft);
              font-size: 10px;
            }
            .mini-kpi strong {
              display: block;
              margin-top: 3px;
              font-size: 12px;
            }
            .table-wrap { margin-top: 10px; }
            .table-wrap h4 {
              margin-bottom: 5px;
              font-size: 12px;
            }
            table {
              width: 100%;
              border-collapse: collapse;
              font-size: 10px;
            }
            th, td {
              border: 1px solid #e9ece7;
              padding: 5px;
              text-align: left;
              vertical-align: top;
            }
            th {
              background: #f7f9f5;
              color: #35504c;
            }
            .empty {
              border: 1px dashed #dbe1d9;
              border-radius: 10px;
              padding: 8px;
              font-size: 11px;
              color: var(--text-soft);
            }
            @media print {
              body { padding: 8mm; }
            }
          </style>
        </head>
        <body>
          <section class="header">
            <h1>Extrato Financeiro Completo da Carteira</h1>
            <p>${this.escapeHtml(scopeLabel)}</p>
            <p>Gerado em ${this.escapeHtml(generatedAt.toLocaleString('pt-BR'))}</p>
            <p>${this.escapeHtml(metrics.guidance)}</p>
          </section>

          <section class="kpi-grid">
            <article class="kpi">
              <p>Saldo em aberto</p>
              <strong>${this.currency(metrics.totalBalance)}</strong>
            </article>
            <article class="kpi">
              <p>Saldo vencido</p>
              <strong>${this.currency(metrics.overdue)}</strong>
            </article>
            <article class="kpi">
              <p>Vence em 7 dias</p>
              <strong>${this.currency(metrics.due7)}</strong>
            </article>
            <article class="kpi">
              <p>Vence em 30 dias</p>
              <strong>${this.currency(metrics.due30)}</strong>
            </article>
            <article class="kpi">
              <p>Risco atual</p>
              <strong>${this.percent(metrics.overdueRatio)}</strong>
            </article>
            <article class="kpi">
              <p>Clientes / Titulos</p>
              <strong>${metrics.clients} / ${metrics.titles}</strong>
            </article>
          </section>

          <section class="dashboard-card">
            <h3>Dashboard de distribuição da carteira</h3>
            ${distributionRows}
          </section>

          ${clientSections}
        </body>
      </html>
    `;
  }

  private buildClientSectionHtml(statement: ClientStatement, order: number): string {
    const client = statement.client;
    const statusClass = `status-${client.status.toLowerCase()}`;
    return `
      <section class="client-section">
        <div class="client-title">
          <div>
            <h3>${order}. ${this.escapeHtml(client.customerName)}</h3>
            <p>Codigo: ${this.escapeHtml(client.customerCode)} | Consultor: ${this.escapeHtml(client.consultantName)}</p>
            <p>Acao recomendada: ${this.escapeHtml(client.action)}</p>
          </div>
          <span class="status ${statusClass}">${this.escapeHtml(client.status)}</span>
        </div>

        <div class="client-kpis">
          <div class="mini-kpi">
            <p>Saldo total</p>
            <strong>${this.currency(client.totalBalance)}</strong>
          </div>
          <div class="mini-kpi">
            <p>Boletos atrasados</p>
            <strong>${this.currency(statement.overdueAmount)}</strong>
          </div>
          <div class="mini-kpi">
            <p>Boletos a vencer</p>
            <strong>${this.currency(statement.upcomingAmount)}</strong>
          </div>
          <div class="mini-kpi">
            <p>Saldo disponível</p>
            <strong>${this.currency(statement.availableBalance)}</strong>
          </div>
        </div>

        <div class="table-wrap">
          <h4>Extrato de boletos atrasados</h4>
          ${this.buildTitlesTableHtml(statement.overdueTitles, 'Sem boletos atrasados para este cliente.')}
        </div>

        <div class="table-wrap">
          <h4>Extrato de boletos a vencer</h4>
          ${this.buildTitlesTableHtml(statement.upcomingTitles, 'Sem boletos a vencer para este cliente.')}
        </div>
      </section>
    `;
  }

  private buildTitlesTableHtml(items: ReceivableItem[], emptyText: string): string {
    if (!items.length) {
      return `<p class="empty">${this.escapeHtml(emptyText)}</p>`;
    }

    const rows = items
      .map((item) => {
        const due = this.dueLabel(item);
        return `
          <tr>
            <td>${this.escapeHtml(item.documentRef)}</td>
            <td>${this.escapeHtml(item.installment)}</td>
            <td>${this.escapeHtml(this.date(item.issueDate))}</td>
            <td>${this.escapeHtml(this.date(item.dueDate))}</td>
            <td>${this.escapeHtml(due)}</td>
            <td>${this.escapeHtml(this.currency(item.balance))}</td>
          </tr>
        `;
      })
      .join('');

    return `
      <table>
        <thead>
          <tr>
            <th>Documento</th>
            <th>Parcela</th>
            <th>Emissao</th>
            <th>Vencimento</th>
            <th>Prazo</th>
            <th>Valor</th>
          </tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
    `;
  }

  private buildClientKey(consultantId: number, customerCode: string): string {
    return `${consultantId}::${customerCode}`;
  }

  private escapeHtml(value: string): string {
    return value
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }
}

function sum(values: number[]): number {
  return values.reduce((acc, current) => acc + current, 0);
}
