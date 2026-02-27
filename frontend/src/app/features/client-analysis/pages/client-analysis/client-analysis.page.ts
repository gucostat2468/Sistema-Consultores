import { CommonModule } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { distinctUntilChanged, forkJoin, map, switchMap } from 'rxjs';
import {
  ClientHealth,
  CreditAlert,
  CreditLimitItem,
  CreditSummary,
  ReceivableItem
} from '../../../../core/models/dashboard.models';
import { DashboardService } from '../../../../core/services/dashboard.service';
import { toCurrency, toDate, toPercent } from '../../../../shared/utils/format';
import {
  buildCreditLookupMaps,
  evaluateCreditMetrics,
  findCreditItem,
} from '../../../../shared/utils/credit-metrics';

interface ExposureBucket {
  label: string;
  value: number;
  count: number;
  ratio: number;
  tone: 'overdue' | 'near' | 'mid' | 'future';
}

interface TimeBand {
  label: string;
  amount: number;
  count: number;
  ratio: number;
  tone: 'overdue' | 'near' | 'mid' | 'future';
}

interface MatrixCell {
  count: number;
  amount: number;
  intensity: number;
}

interface MatrixRow {
  label: string;
  tone: 'overdue' | 'near' | 'safe';
  cells: MatrixCell[];
}

interface AnalysisKpis {
  total: number;
  overdue: number;
  due7: number;
  avgTicket: number;
  maxTicket: number;
  titles: number;
  available: number;
}

interface CreditDebtSnapshot {
  receivablesOpen: number;
  debtTotal: number;
  debtOverdue: number;
  debtFuture: number;
  due7: number;
  limitTotal: number;
  available: number;
  used: number;
  gap: number;
  debtToLimitRatio: number;
  debtToAvailableRatio: number;
  coverageRatio: number;
  hasRealLimit: boolean;
  sourceLabel: string;
  alert: CreditAlert;
}

interface CreditOnboarding {
  debtStory: string;
  availabilityStory: string;
  needStory: string;
  actionStory: string;
  priorityLabel: 'Critica' | 'Atencao' | 'Ok';
  priorityClass: 'guide-priority-critical' | 'guide-priority-attention' | 'guide-priority-ok';
}

interface TopTitleItem extends ReceivableItem {
  width: number;
  dayDiff: number;
}

@Component({
  selector: 'app-client-analysis-page',
  imports: [CommonModule, FormsModule],
  templateUrl: './client-analysis.page.html',
  styleUrl: './client-analysis.page.scss'
})
export class ClientAnalysisPage {
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly dashboardService = inject(DashboardService);
  private readonly destroyRef = inject(DestroyRef);

  readonly loading = signal(true);
  readonly error = signal<string | null>(null);
  readonly exportError = signal<string | null>(null);
  readonly clients = signal<ClientHealth[]>([]);
  readonly receivables = signal<ReceivableItem[]>([]);
  readonly creditSummary = signal<CreditSummary | null>(null);
  readonly creditItems = signal<CreditLimitItem[]>([]);
  readonly selectedClientKey = signal<string | null>(null);
  readonly showCreditOnboarding = signal(true);
  readonly creditLookup = computed(() => buildCreditLookupMaps(this.creditItems()));

  readonly clientOptions = computed(() =>
    this.clients().map((item) => ({
      key: this.buildClientKey(item.consultantId, item.customerCode, item.customerName),
      label: `${item.customerName} (${item.consultantName})`
    }))
  );

  readonly selectedClient = computed(() => {
    const key = this.selectedClientKey();
    if (!key) {
      return null;
    }
    return (
      this.clients().find(
        (item) => this.buildClientKey(item.consultantId, item.customerCode, item.customerName) === key
      ) ?? null
    );
  });

  readonly selectedReceivables = computed(() => {
    const client = this.selectedClient();
    if (!client) {
      return [];
    }
    return this.receivables().filter(
      (item) => {
        if (item.consultantId !== client.consultantId) {
          return false;
        }
        const selectedCode = (client.customerCode ?? '').trim();
        const currentCode = (item.customerCode ?? '').trim();
        if (selectedCode && currentCode) {
          return selectedCode === currentCode;
        }
        return (
          this.normalizeCustomerKey(item.customerName) ===
          this.normalizeCustomerKey(client.customerName)
        );
      }
    );
  });

  readonly selectedCreditItem = computed(() => {
    const client = this.selectedClient();
    if (!client) {
      return null;
    }
    return findCreditItem(
      this.creditLookup(),
      client.consultantId,
      client.customerCode,
      client.customerName
    );
  });

  readonly selectedCreditCoverageRatio = computed(() => {
    const panel = this.selectedCreditDebtPanel();
    if (!panel || panel.debtTotal <= 0) {
      return 0;
    }
    return panel.coverageRatio;
  });

  readonly selectedCreditDebtPanel = computed<CreditDebtSnapshot | null>(() => {
    const client = this.selectedClient();
    if (!client) {
      return null;
    }

    const summary = this.kpis();
    const credit = this.selectedCreditItem();
    if (!credit) {
      return null;
    }
    const metrics = evaluateCreditMetrics({
      debtOpen: summary.total,
      overdue: summary.overdue,
      credit
    });
    const limitTotal = metrics.limit;
    const available = metrics.available;
    const used = credit?.creditUsed ?? Math.max(limitTotal - available, 0);
    const debtTotal = summary.total;
    const debtOverdue = summary.overdue;
    const debtFuture = Math.max(debtTotal - debtOverdue, 0);
    const gap = Math.max(debtTotal - available, 0);
    const debtToLimitRatio = metrics.debtToLimitRatio;
    const debtToAvailableRatio = available > 0 ? debtTotal / available : debtTotal > 0 ? 9.99 : 0;
    const coverageRatio = metrics.coverageRatio;
    const alert = this.classifyDebtCreditAlert(debtTotal, available, debtOverdue);
    return {
      receivablesOpen: debtTotal,
      debtTotal,
      debtOverdue,
      debtFuture,
      due7: summary.due7,
      limitTotal,
      available,
      used,
      gap,
      debtToLimitRatio,
      debtToAvailableRatio,
      coverageRatio,
      hasRealLimit: Boolean(credit),
      sourceLabel: 'Cadastro real de limite',
      alert
    };
  });

  readonly creditOnboarding = computed<CreditOnboarding | null>(() => {
    const panel = this.selectedCreditDebtPanel();
    if (!panel) {
      return null;
    }

    const debtStory =
      panel.debtOverdue > 0
        ? `${this.currency(panel.debtOverdue)} ja vencidos e ${this.currency(panel.due7)} vencem em 7 dias.`
        : `${this.currency(panel.debtFuture)} ainda a vencer, sem atraso no momento.`;

    const availabilityStory =
      panel.available > 0
        ? `Ha ${this.currency(panel.available)} de limite livre para sustentar a operacao.`
        : 'Nao ha limite livre agora para suportar novos movimentos.';

    const needStory =
      panel.gap > 0
        ? `Faltam ${this.currency(panel.gap)} para cobrir toda a divida atual.`
        : 'O limite disponivel ja cobre 100% da divida atual.';

    const pressure = panel.debtTotal > 0 ? (panel.debtOverdue + panel.due7) / panel.debtTotal : 0;
    let priorityLabel: CreditOnboarding['priorityLabel'] = 'Ok';
    let priorityClass: CreditOnboarding['priorityClass'] = 'guide-priority-ok';
    let actionStory = 'Cenario controlado para continuidade das propostas com monitoramento padrao.';

    if (panel.gap > 0 || panel.debtToAvailableRatio > 1 || pressure >= 0.35) {
      priorityLabel = 'Atencao';
      priorityClass = 'guide-priority-attention';
      actionStory = 'Segurar novas condicoes agressivas e acompanhar recebimentos de curto prazo.';
    }
    if (panel.debtOverdue > 0 || panel.alert === 'Acima do limite' || panel.alert === 'Sem limite livre') {
      priorityLabel = 'Critica';
      priorityClass = 'guide-priority-critical';
      actionStory = 'Priorizar cobranca/renegociacao antes de ampliar limite ou nova proposta comercial.';
    }

    return {
      debtStory,
      availabilityStory,
      needStory,
      actionStory,
      priorityLabel,
      priorityClass
    };
  });

  readonly kpis = computed(() => {
    return this.calculateKpis(this.selectedReceivables());
  });

  readonly exposureBuckets = computed<ExposureBucket[]>(() => {
    return this.calculateExposureBuckets(this.selectedReceivables());
  });

  readonly donutStyle = computed(() => {
    return this.buildDonutStyle(this.exposureBuckets());
  });

  readonly topTitles = computed(() => this.calculateTopTitles(this.selectedReceivables(), null));

  readonly timeConcentration = computed<TimeBand[]>(() => {
    return this.calculateTimeConcentration(this.selectedReceivables());
  });

  readonly matrix = computed<MatrixRow[]>(() => {
    return this.calculateMatrix(this.selectedReceivables());
  });

  constructor() {
    this.route.queryParamMap
      .pipe(
        map((params) => ({
          consultantId: params.get('consultantId')
            ? Number(params.get('consultantId'))
            : null,
          clientKey: params.get('clientKey')
        })),
        distinctUntilChanged(
          (a, b) => a.consultantId === b.consultantId && a.clientKey === b.clientKey
        ),
        switchMap(({ consultantId, clientKey }) => {
          this.loading.set(true);
          this.error.set(null);
          return forkJoin({
            clientHealth: this.dashboardService.getClientHealth(consultantId),
            receivables: this.dashboardService.getReceivables(consultantId),
            creditLimits: this.dashboardService.getCreditLimits(consultantId)
          }).pipe(map((data) => ({ ...data, clientKey })));
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ({ clientHealth, receivables, creditLimits, clientKey }) => {
          this.clients.set(clientHealth);
          this.receivables.set(receivables);
          this.creditSummary.set(creditLimits.summary);
          this.creditItems.set(creditLimits.items);
          const resolvedClientKey = this.resolveClientKey(clientKey, clientHealth);
          this.selectedClientKey.set(resolvedClientKey);
          if (resolvedClientKey && resolvedClientKey !== clientKey) {
            this.router.navigate([], {
              relativeTo: this.route,
              queryParams: { clientKey: resolvedClientKey },
              queryParamsHandling: 'merge',
              replaceUrl: true
            });
          }
          this.loading.set(false);
        },
        error: () => {
          this.error.set('Nao foi possivel carregar os dados de analise deste cliente.');
          this.loading.set(false);
        }
      });
  }

  selectClient(clientKey: string): void {
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams: { clientKey },
      queryParamsHandling: 'merge'
    });
  }

  toggleCreditOnboarding(): void {
    this.showCreditOnboarding.update((value) => !value);
  }

  toneClass(tone: 'overdue' | 'near' | 'mid' | 'future'): string {
    return `tone tone-${tone}`;
  }

  matrixCellStyle(cell: MatrixCell, tone: 'overdue' | 'near' | 'safe'): string {
    const alpha = 0.28 + cell.intensity * 0.56;
    const [r, g, b] = this.matrixRgbByTone(tone);
    const textColor =
      tone === 'overdue'
        ? cell.intensity < 0.45
          ? '#6f1f1f'
          : '#ffffff'
        : tone === 'near'
          ? cell.intensity < 0.48
            ? '#6a4400'
            : '#ffffff'
          : cell.intensity < 0.34
            ? '#173733'
            : '#ffffff';
    return `background: rgba(${r}, ${g}, ${b}, ${alpha.toFixed(2)}); color: ${textColor}; box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.14);`;
  }

  statusClass(status: ClientHealth['status']): string {
    return `status status-${status.toLowerCase()}`;
  }

  creditAlertClass(alert: CreditAlert): string {
    const key = this.creditAlertKey(alert);
    return `credit-alert credit-alert-${key}`;
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

  creditNeedLabel(gap: number): string {
    if (gap <= 0) {
      return 'Sem necessidade adicional de limite.';
    }
    return 'Necessidade para cobrir 100% da divida atual.';
  }

  creditExposureWidth(ratio: number): number {
    return Math.max(0, Math.min(100, ratio * 100));
  }

  dayBadge(diff: number): string {
    if (diff < 0) {
      return `${Math.abs(diff)}d atraso`;
    }
    if (diff === 0) {
      return 'vence hoje';
    }
    return `vence em ${diff}d`;
  }

  dayBadgeClass(diff: number): string {
    const tone = this.dueToneFromDiff(diff);
    if (tone === 'overdue') {
      return 'due-text-overdue';
    }
    if (tone === 'near') {
      return 'due-text-near';
    }
    return 'due-text-safe';
  }

  topBarClass(diff: number): string {
    const tone = this.dueToneFromDiff(diff);
    if (tone === 'overdue') {
      return 'bar-fill-overdue';
    }
    if (tone === 'near') {
      return 'bar-fill-near';
    }
    return 'bar-fill-safe';
  }

  matrixRowClass(row: MatrixRow): string {
    return `matrix-row-${row.tone}`;
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

  exportAllClientsAnalysisPdf(): void {
    if (this.clients().length === 0) {
      this.exportError.set('Nao ha clientes no escopo atual para exportar.');
      return;
    }

    this.exportAnalysisPdfScope(
      this.clients(),
      'Relatorio completo - Analise Cliente',
      'Todos os dashboards da pagina Analise Cliente, organizado cliente a cliente.'
    );
  }

  exportSelectedClientAnalysisPdf(): void {
    const client = this.selectedClient();
    if (!client) {
      this.exportError.set('Selecione um cliente para exportar o relatorio isolado.');
      return;
    }

    this.exportAnalysisPdfScope(
      [client],
      'Relatorio isolado - Analise Cliente',
      `Dashboards completos do cliente selecionado: ${client.customerName}.`
    );
  }

  private exportAnalysisPdfScope(
    clientsToExport: ClientHealth[],
    title: string,
    description: string
  ): void {
    if (clientsToExport.length === 0) {
      this.exportError.set('Nao ha clientes no escopo atual para exportar.');
      return;
    }

    this.exportError.set(null);
    const popup = window.open('', '_blank');
    if (!popup) {
      this.exportError.set('Libere pop-up no navegador para exportar o PDF.');
      return;
    }

    const html = this.buildAnalysisPdfHtml(clientsToExport, title, description);
    popup.document.open();
    popup.document.write(html);
    popup.document.close();
    popup.focus();
    setTimeout(() => popup.print(), 350);
  }

  private resolveClientKey(
    queryClientKey: string | null,
    clientHealth: ClientHealth[]
  ): string | null {
    if (!clientHealth.length) {
      return null;
    }

    if (queryClientKey) {
      const exists = clientHealth.some(
        (item) =>
          this.buildClientKey(item.consultantId, item.customerCode, item.customerName) ===
          queryClientKey
      );
      if (exists) {
        return queryClientKey;
      }
    }

    const first = clientHealth[0];
    return this.buildClientKey(first.consultantId, first.customerCode, first.customerName);
  }

  private buildClientKey(consultantId: number, customerCode: string, customerName: string): string {
    const normalizedCode = (customerCode ?? '').trim();
    if (normalizedCode) {
      return `${consultantId}::code::${normalizedCode}`;
    }
    return `${consultantId}::name::${this.normalizeCustomerKey(customerName)}`;
  }

  private normalizeCustomerKey(value: string): string {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, ' ')
      .trim();
  }

  private findCreditItemForClient(client: ClientHealth): CreditLimitItem | null {
    return findCreditItem(
      this.creditLookup(),
      client.consultantId,
      client.customerCode,
      client.customerName
    );
  }

  private creditAlertKey(alert: CreditAlert): string {
    if (alert === 'Acima do limite') {
      return 'over-limit';
    }
    if (alert === 'Sem limite livre') {
      return 'no-available';
    }
    if (alert === 'Atencao') {
      return 'attention';
    }
    if (alert === 'Controlado') {
      return 'controlled';
    }
    if (alert === 'Sem exposicao') {
      return 'no-exposure';
    }
    return 'no-data';
  }

  private classifyDebtCreditAlert(totalDebt: number, available: number, overdue: number): CreditAlert {
    if (totalDebt <= 0) {
      return 'Sem exposicao';
    }
    if (available <= 0) {
      return 'Sem limite livre';
    }
    if (totalDebt > available) {
      return 'Acima do limite';
    }
    if (totalDebt >= available * 0.8 || overdue > 0) {
      return 'Atencao';
    }
    return 'Controlado';
  }

  private calculateKpis(items: ReceivableItem[]): AnalysisKpis {
    const total = sum(items.map((item) => item.balance));
    const overdue = sum(items.filter((item) => toDayDiff(item.dueDate) < 0).map((item) => item.balance));
    const due7 = sum(
      items
        .filter((item) => {
          const diff = toDayDiff(item.dueDate);
          return diff >= 0 && diff <= 7;
        })
        .map((item) => item.balance)
    );
    const avgTicket = items.length ? total / items.length : 0;
    const maxTicket = items.length ? Math.max(...items.map((item) => item.balance)) : 0;
    return {
      total,
      overdue,
      due7,
      avgTicket,
      maxTicket,
      titles: items.length,
      available: Math.max(total - overdue, 0)
    };
  }

  private calculateExposureBuckets(items: ReceivableItem[]): ExposureBucket[] {
    const buckets = [
      {
        label: 'Vencido',
        tone: 'overdue' as const,
        filter: (days: number) => days < 0
      },
      {
        label: '0 a 7 dias',
        tone: 'near' as const,
        filter: (days: number) => days >= 0 && days <= 7
      },
      {
        label: '8 a 30 dias',
        tone: 'mid' as const,
        filter: (days: number) => days >= 8 && days <= 30
      },
      {
        label: '31+ dias',
        tone: 'future' as const,
        filter: (days: number) => days > 30
      }
    ];

    const values = buckets.map((bucket) => {
      const matched = items.filter((item) => bucket.filter(toDayDiff(item.dueDate)));
      const value = sum(matched.map((item) => item.balance));
      return {
        label: bucket.label,
        tone: bucket.tone,
        value,
        count: matched.length,
        ratio: 0
      };
    });

    const total = sum(values.map((item) => item.value)) || 1;
    return values.map((item) => ({ ...item, ratio: item.value / total }));
  }

  private buildDonutStyle(buckets: ExposureBucket[]): string {
    const total = sum(buckets.map((item) => item.value));
    if (total <= 0) {
      return 'conic-gradient(#e9ece7 0 100%)';
    }

    let cursor = 0;
    const parts: string[] = [];
    for (const bucket of buckets) {
      const slice = bucket.ratio * 100;
      if (slice <= 0) {
        continue;
      }
      const start = cursor;
      const end = cursor + slice;
      parts.push(`${toneColor(bucket.tone)} ${start.toFixed(2)}% ${end.toFixed(2)}%`);
      cursor = end;
    }

    if (cursor < 100) {
      parts.push(`#e9ece7 ${cursor.toFixed(2)}% 100%`);
    }
    return `conic-gradient(${parts.join(', ')})`;
  }

  private calculateTopTitles(
    items: ReceivableItem[],
    maxItems: number | null = 8
  ): TopTitleItem[] {
    const sorted = [...items].sort((a, b) => {
      const byDueDate = a.dueDate.localeCompare(b.dueDate);
      if (byDueDate !== 0) {
        return byDueDate;
      }
      return b.balance - a.balance;
    });
    const limited = maxItems === null ? sorted : sorted.slice(0, maxItems);
    const max = limited.length ? Math.max(...limited.map((item) => item.balance)) : 1;
    return limited.map((item) => ({
      ...item,
      width: (item.balance / max) * 100,
      dayDiff: toDayDiff(item.dueDate)
    }));
  }

  private calculateTimeConcentration(items: ReceivableItem[]): TimeBand[] {
    if (items.length === 0) {
      return [];
    }

    const defs = [
      {
        label: 'Atrasado',
        tone: 'overdue' as const,
        filter: (days: number) => days < 0
      },
      {
        label: '0 a 7 dias',
        tone: 'near' as const,
        filter: (days: number) => days >= 0 && days <= 7
      },
      {
        label: '8 a 15 dias',
        tone: 'near' as const,
        filter: (days: number) => days >= 8 && days <= 15
      },
      {
        label: '16 a 30 dias',
        tone: 'mid' as const,
        filter: (days: number) => days >= 16 && days <= 30
      },
      {
        label: '31 a 45 dias',
        tone: 'future' as const,
        filter: (days: number) => days >= 31 && days <= 45
      },
      {
        label: '46+ dias',
        tone: 'future' as const,
        filter: (days: number) => days > 45
      }
    ];

    const values = defs.map((def) => {
      const matched = items.filter((item) => def.filter(toDayDiff(item.dueDate)));
      return {
        label: def.label,
        tone: def.tone,
        amount: sum(matched.map((item) => item.balance)),
        count: matched.length,
        ratio: 0
      };
    });

    const maxAmount = Math.max(1, ...values.map((item) => item.amount));
    return values.map((item) => ({
      ...item,
      ratio: item.amount / maxAmount
    }));
  }

  private calculateMatrix(items: ReceivableItem[]): MatrixRow[] {
    if (!items.length) {
      return [];
    }

    const balances = items.map((item) => item.balance).sort((a, b) => a - b);
    const q1 = quantile(balances, 0.33);
    const q2 = quantile(balances, 0.66);

    const rowDefs = [
      { label: 'Vencido', tone: 'overdue' as const, filter: (days: number) => days < 0 },
      { label: '0 a 7 dias', tone: 'near' as const, filter: (days: number) => days >= 0 && days <= 7 },
      { label: '8 a 30 dias', tone: 'safe' as const, filter: (days: number) => days >= 8 && days <= 30 },
      { label: '31+ dias', tone: 'safe' as const, filter: (days: number) => days > 30 }
    ];
    const colDefs = [
      { label: 'Ticket baixo', filter: (amount: number) => amount <= q1 },
      { label: 'Ticket medio', filter: (amount: number) => amount > q1 && amount <= q2 },
      { label: 'Ticket alto', filter: (amount: number) => amount > q2 }
    ];

    const rows = rowDefs.map((row) => ({
      label: row.label,
      tone: row.tone,
      cells: colDefs.map((col) => {
        const matched = items.filter(
          (item) => row.filter(toDayDiff(item.dueDate)) && col.filter(item.balance)
        );
        return {
          count: matched.length,
          amount: sum(matched.map((item) => item.balance)),
          intensity: 0
        };
      })
    }));

    const maxAmount = Math.max(
      1,
      ...rows.flatMap((row) => row.cells.map((cell) => cell.amount))
    );

    return rows.map((row) => ({
      ...row,
      cells: row.cells.map((cell) => ({
        ...cell,
        intensity: Number((cell.amount / maxAmount).toFixed(2))
      }))
    }));
  }

  private buildAnalysisPdfHtml(
    clientsToExport: ClientHealth[],
    title: string,
    description: string
  ): string {
    const generatedAt = new Date().toLocaleString('pt-BR');
    const clientSections = clientsToExport
      .map((client, index) => this.buildClientAnalysisSectionHtml(client, index + 1))
      .join('');

    return `
      <!doctype html>
      <html lang="pt-BR">
        <head>
          <meta charset="utf-8" />
          <title>Analise Cliente - Relatorio Completo</title>
          <style>
            :root {
              --line: #d9ddd6;
              --text-main: #13312e;
              --text-soft: #53706d;
            }
            * {
              box-sizing: border-box;
              -webkit-print-color-adjust: exact;
              print-color-adjust: exact;
            }
            body {
              margin: 0;
              padding: 20px;
              color: var(--text-main);
              font-family: "Segoe UI", Arial, sans-serif;
              background: #fff;
            }
            h1, h2, h3, h4, p { margin: 0; }
            .header {
              border: 1px solid var(--line);
              border-radius: 14px;
              padding: 12px;
              margin-bottom: 12px;
            }
            .header h1 { font-size: 21px; }
            .header p { margin-top: 4px; color: var(--text-soft); font-size: 12px; }
            .client {
              border: 1px solid var(--line);
              border-radius: 12px;
              padding: 12px;
              margin-top: 10px;
              break-inside: avoid;
              page-break-inside: avoid;
            }
            .title {
              display: flex;
              justify-content: space-between;
              gap: 10px;
              align-items: flex-start;
            }
            .title h3 { font-size: 15px; }
            .title p { margin-top: 2px; font-size: 11px; color: var(--text-soft); }
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
            .kpis {
              margin-top: 8px;
              display: grid;
              grid-template-columns: repeat(6, minmax(0, 1fr));
              gap: 6px;
            }
            .kpi {
              border: 1px solid #e6ebe4;
              border-radius: 10px;
              padding: 6px;
            }
            .kpi p { color: var(--text-soft); font-size: 10px; }
            .kpi strong { display: block; margin-top: 3px; font-size: 12px; }
            .credit-grid {
              display: grid;
              grid-template-columns: repeat(6, minmax(0, 1fr));
              gap: 6px;
            }
            .credit-label {
              margin-top: 6px;
              font-size: 10px;
              color: var(--text-soft);
            }
            .grid-2 {
              margin-top: 8px;
              display: grid;
              grid-template-columns: 1fr 1fr;
              gap: 8px;
            }
            .card {
              border: 1px solid #e6ebe4;
              border-radius: 10px;
              padding: 8px;
            }
            .card h4 { font-size: 12px; margin-bottom: 6px; }
            .composition-layout {
              display: grid;
              grid-template-columns: 200px 1fr;
              gap: 10px;
              align-items: center;
            }
            .donut-wrap {
              display: grid;
              place-items: center;
            }
            .donut-wrap svg {
              width: 170px;
              height: 170px;
              display: block;
            }
            .legend-item {
              display: flex;
              gap: 6px;
              align-items: flex-start;
              font-size: 10px;
              margin-top: 4px;
            }
            .legend-item > div {
              min-width: 0;
            }
            .legend-item strong {
              display: block;
              font-size: 11px;
              color: #193c38;
            }
            .legend-item p {
              margin: 1px 0 0;
              color: var(--text-soft);
              font-size: 10px;
            }
            .tone {
              display: inline-block;
              width: 10px;
              height: 10px;
              border-radius: 3px;
              margin-right: 4px;
              vertical-align: middle;
            }
            .tone-overdue { background: #c73a3a; }
            .tone-near { background: #f59a2e; }
            .tone-mid { background: #58a882; }
            .tone-future { background: #0f5b5a; }
            .bar-track {
              height: 7px;
              border-radius: 999px;
              overflow: hidden;
              background: #edf2ec;
            }
            .bar-fill {
              height: 100%;
              background: linear-gradient(120deg, #f79e2f 0%, #1b7b72 100%);
            }
            .bar-fill-overdue { background: #c73a3a; }
            .bar-fill-near { background: #f59a2e; }
            .bar-fill-mid { background: #58a882; }
            .bar-fill-future { background: #0f5b5a; }
            .time-row {
              display: grid;
              grid-template-columns: 95px 1fr 110px;
              gap: 6px;
              align-items: center;
              font-size: 10px;
              margin-top: 4px;
            }
            .time-label strong {
              display: block;
              color: #1f3f3b;
            }
            .time-label p {
              margin: 1px 0 0;
              color: var(--text-soft);
              font-size: 10px;
            }
            .time-values {
              text-align: right;
            }
            .time-values strong {
              display: block;
              color: #1f3f3b;
            }
            .time-values span {
              color: var(--text-soft);
              font-size: 10px;
            }
            .matrix {
              width: 100%;
              border-collapse: collapse;
              font-size: 10px;
            }
            .matrix th, .matrix td {
              border: 1px solid #e1e6df;
              padding: 4px;
            }
            .matrix th {
              background: #f5f8f2;
              color: #45635b;
              font-weight: 700;
            }
            .matrix td {
              text-align: center;
            }
            .top-row {
              display: grid;
              grid-template-columns: 1fr 0.95fr auto;
              gap: 6px;
              align-items: center;
              font-size: 10px;
              margin-top: 5px;
            }
            .top-info p { margin-top: 2px; color: var(--text-soft); }
            .top-track {
              height: 8px;
              border-radius: 999px;
              overflow: hidden;
              background: #eef2ec;
            }
            .top-fill {
              height: 100%;
              border-radius: 999px;
              background: linear-gradient(120deg, #f79e2f 0%, #1b7b72 100%);
            }
            .empty {
              border: 1px dashed #dbe1d9;
              border-radius: 8px;
              padding: 6px;
              font-size: 10px;
              color: var(--text-soft);
            }
            @media print {
              body { padding: 8mm; }
            }
          </style>
        </head>
        <body>
          <section class="header">
            <h1>${this.escapeHtml(title)}</h1>
            <p>${this.escapeHtml(description)}</p>
            <p>Gerado em ${this.escapeHtml(generatedAt)}</p>
            <p>Total de clientes no escopo: ${clientsToExport.length}</p>
          </section>
          ${clientSections}
        </body>
      </html>
    `;
  }

  private buildClientAnalysisSectionHtml(client: ClientHealth, order: number): string {
    const items = this.receivables().filter(
      (item) => item.customerCode === client.customerCode && item.consultantId === client.consultantId
    );
    const credit = this.findCreditItemForClient(client);
    const kpis = this.calculateKpis(items);
    const exposure = this.calculateExposureBuckets(items);
    const timeBands = this.calculateTimeConcentration(items);
    const matrix = this.calculateMatrix(items);
    const top = this.calculateTopTitles(items);
    const statusClass = `status-${client.status.toLowerCase()}`;
    const donutSvg = this.buildDonutSvg(exposure, kpis.total);

    const exposureRows = exposure
      .map(
        (bucket) => `
          <div class="legend-item">
            <span class="tone tone-${bucket.tone}"></span>
            <div>
              <strong>${this.escapeHtml(bucket.label)}</strong>
              <p>
                ${this.escapeHtml(this.currency(bucket.value))} | ${bucket.count} titulos |
                ${this.escapeHtml(this.percent(bucket.ratio))}
              </p>
            </div>
          </div>
        `
      )
      .join('');

    const timeRows = timeBands
      .map(
        (band) => `
          <div class="time-row">
            <div class="time-label">
              <strong>${this.escapeHtml(band.label)}</strong>
              <p>${band.count} titulos</p>
            </div>
            <div class="bar-track">
              <div
                class="bar-fill bar-fill-${band.tone}"
                style="width: ${(band.ratio * 100).toFixed(1)}%"
              ></div>
            </div>
            <div class="time-values">
              <strong>${this.escapeHtml(this.currency(band.amount))}</strong>
              <span>${this.escapeHtml(this.percent(kpis.total ? band.amount / kpis.total : 0))}</span>
            </div>
          </div>
        `
      )
      .join('');

    const matrixRows = matrix
      .map((row) => {
        const cells = row.cells
          .map(
            (cell) =>
              `<td style="${this.matrixCellPdfStyle(cell, row.tone)}">${cell.count} tit.<br />${this.escapeHtml(this.currency(cell.amount))}</td>`
          )
          .join('');
        return `<tr><th style="${this.matrixRowLabelPdfStyle(row.tone)}">${this.escapeHtml(row.label)}</th>${cells}</tr>`;
      })
      .join('');

    const topRows = top
      .map(
        (item) => `
          <div class="top-row">
            <div class="top-info">
              <strong>${this.escapeHtml(item.documentRef)} - Parcela ${this.escapeHtml(item.installment)}</strong>
              <p style="color:${this.pdfTopTitleTextColorByDiff(item.dayDiff)};font-weight:700;">${this.escapeHtml(this.date(item.dueDate))} | ${this.escapeHtml(this.dayBadge(item.dayDiff))}</p>
            </div>
            <div class="top-track"><div class="top-fill" style="width: ${item.width.toFixed(1)}%; background: ${this.pdfTopTitleColorByDiff(item.dayDiff)};"></div></div>
            <strong>${this.escapeHtml(this.currency(item.balance))}</strong>
          </div>
        `
      )
      .join('');

    const creditSection = credit
      ? `
        <div class="credit-grid">
          <article class="kpi"><p>Limite total</p><strong>${this.escapeHtml(this.currency(credit.creditLimit))}</strong></article>
          <article class="kpi"><p>Utilizado</p><strong>${this.escapeHtml(this.currency(credit.creditUsed))}</strong></article>
          <article class="kpi"><p>Disponivel</p><strong>${this.escapeHtml(this.currency(credit.creditAvailable))}</strong></article>
          <article class="kpi"><p>Exposicao atual</p><strong>${this.escapeHtml(this.currency(credit.exposure))}</strong></article>
          <article class="kpi"><p>Risco vencido</p><strong>${this.escapeHtml(this.currency(credit.overdue))}</strong></article>
          <article class="kpi"><p>Cobertura</p><strong>${this.escapeHtml(this.percent(credit.exposure > 0 ? credit.creditAvailable / credit.exposure : 0))}</strong></article>
        </div>
        <p class="credit-label">Status de credito: <strong>${this.escapeHtml(this.creditAlertLabel(credit.alert))}</strong> | Exposicao/Disponivel: <strong>${this.escapeHtml(this.percent(credit.exposureToAvailableRatio))}</strong></p>
      `
      : '<p class="empty">Sem cadastro de limite de credito para este cliente.</p>';

    return `
      <section class="client">
        <div class="title">
          <div>
            <h3>${order}. ${this.escapeHtml(client.customerName)}</h3>
            <p>${this.escapeHtml(client.customerCode)} | ${this.escapeHtml(client.consultantName)}</p>
            <p>${this.escapeHtml(client.action)}</p>
          </div>
          <span class="status ${statusClass}">${this.escapeHtml(client.status)} | score ${client.score}</span>
        </div>

        <div class="kpis">
          <article class="kpi"><p>Saldo total</p><strong>${this.escapeHtml(this.currency(kpis.total))}</strong></article>
          <article class="kpi"><p>Vencido</p><strong>${this.escapeHtml(this.currency(kpis.overdue))}</strong></article>
          <article class="kpi"><p>Vence em 7 dias</p><strong>${this.escapeHtml(this.currency(kpis.due7))}</strong></article>
          <article class="kpi"><p>Saldo disponivel</p><strong>${this.escapeHtml(this.currency(kpis.available))}</strong></article>
          <article class="kpi"><p>Ticket medio</p><strong>${this.escapeHtml(this.currency(kpis.avgTicket))}</strong></article>
          <article class="kpi"><p>Maior ticket</p><strong>${this.escapeHtml(this.currency(kpis.maxTicket))}</strong></article>
        </div>

        <article class="card" style="margin-top:8px;">
          <h4>Recebiveis x limite de credito</h4>
          ${creditSection}
        </article>

        <div class="grid-2">
          <article class="card">
            <h4>Composicao da exposicao</h4>
            <div class="composition-layout">
              <div class="donut-wrap">${donutSvg}</div>
              <div>${exposureRows || '<p class="empty">Sem dados para composicao.</p>'}</div>
            </div>
          </article>
          <article class="card">
            <h4>Concentracao no tempo</h4>
            ${timeRows || '<p class="empty">Sem dados para concentracao.</p>'}
          </article>
        </div>

        <div class="grid-2">
          <article class="card">
            <h4>Matriz de risco (horizonte x ticket)</h4>
            ${
              matrixRows
                ? `
              <table class="matrix">
                <thead>
                  <tr>
                    <th></th>
                    <th>Ticket baixo</th>
                    <th>Ticket medio</th>
                    <th>Ticket alto</th>
                  </tr>
                </thead>
                <tbody>${matrixRows}</tbody>
              </table>
            `
                : '<p class="empty">Sem dados para matriz.</p>'
            }
          </article>
          <article class="card">
            <h4>Top titulos por exposicao</h4>
            ${topRows || '<p class="empty">Sem titulos para ranking.</p>'}
          </article>
        </div>
      </section>
    `;
  }

  private buildDonutSvg(buckets: ExposureBucket[], total: number): string {
    const size = 176;
    const cx = 88;
    const cy = 88;
    const radius = 58;
    const stroke = 22;
    const circumference = 2 * Math.PI * radius;
    const safeBuckets = buckets.filter((bucket) => bucket.ratio > 0);
    const valueLabel = this.currency(total);
    const valueFontSize =
      valueLabel.length > 18 ? 7.6 :
      valueLabel.length > 16 ? 8.4 :
      valueLabel.length > 14 ? 9.2 :
      valueLabel.length > 12 ? 10.2 : 11.2;
    const innerDiameter = (radius - stroke / 2) * 2;
    const maxTextWidth = Math.max(56, innerDiameter - 20);

    let offset = 0;
    const rings = safeBuckets
      .map((bucket) => {
        const segment = Math.max(0, bucket.ratio * circumference);
        const ring = `
          <circle
            cx="${cx}"
            cy="${cy}"
            r="${radius}"
            fill="none"
            stroke="${toneColor(bucket.tone)}"
            stroke-width="${stroke}"
            stroke-linecap="butt"
            stroke-dasharray="${segment} ${circumference - segment}"
            stroke-dashoffset="${-offset}"
            transform="rotate(-90 ${cx} ${cy})"
          />
        `;
        offset += segment;
        return ring;
      })
      .join('');

    return `
      <svg viewBox="0 0 ${size} ${size}" xmlns="http://www.w3.org/2000/svg" aria-label="Donut de composicao">
        <circle cx="${cx}" cy="${cy}" r="${radius}" fill="none" stroke="#e8ede6" stroke-width="${stroke}" />
        ${rings}
        <circle cx="${cx}" cy="${cy}" r="${radius - stroke / 2}" fill="#ffffff" />
        <text
          x="${cx}"
          y="${cy - 2}"
          text-anchor="middle"
          dominant-baseline="middle"
          font-size="${valueFontSize}"
          font-weight="700"
          fill="#13312e"
          textLength="${maxTextWidth}"
          lengthAdjust="spacingAndGlyphs"
          style="letter-spacing:-0.01em"
        >${this.escapeHtml(valueLabel)}</text>
        <text x="${cx}" y="${cy + 20}" text-anchor="middle" dominant-baseline="middle" font-size="11" fill="#53706d">Total</text>
      </svg>
    `;
  }

  private dueToneFromDiff(diff: number): 'overdue' | 'near' | 'safe' {
    if (diff < 0) {
      return 'overdue';
    }
    if (diff <= 7) {
      return 'near';
    }
    return 'safe';
  }

  private matrixRgbByTone(tone: 'overdue' | 'near' | 'safe'): [number, number, number] {
    if (tone === 'overdue') {
      return [199, 58, 58];
    }
    if (tone === 'near') {
      return [245, 154, 46];
    }
    return [15, 91, 90];
  }

  private matrixCellPdfStyle(cell: MatrixCell, tone: 'overdue' | 'near' | 'safe'): string {
    const alpha = 0.28 + cell.intensity * 0.56;
    const [r, g, b] = this.matrixRgbByTone(tone);
    const textColor =
      tone === 'overdue'
        ? cell.intensity < 0.45
          ? '#6f1f1f'
          : '#ffffff'
        : tone === 'near'
          ? cell.intensity < 0.48
            ? '#6a4400'
            : '#ffffff'
          : cell.intensity < 0.34
            ? '#173733'
            : '#ffffff';
    return `background: rgba(${r}, ${g}, ${b}, ${alpha.toFixed(2)}); color: ${textColor}; font-weight: 600;`;
  }

  private matrixRowLabelPdfStyle(tone: 'overdue' | 'near' | 'safe'): string {
    if (tone === 'overdue') {
      return 'background: #fde9e9; color: #8f2323;';
    }
    if (tone === 'near') {
      return 'background: #fff1dd; color: #8a4d00;';
    }
    return 'background: #e7f7f0; color: #155b49;';
  }

  private pdfTopTitleColorByDiff(diff: number): string {
    const tone = this.dueToneFromDiff(diff);
    if (tone === 'overdue') {
      return '#cb2f2f';
    }
    if (tone === 'near') {
      return '#f1992f';
    }
    return '#27816c';
  }

  private pdfTopTitleTextColorByDiff(diff: number): string {
    const tone = this.dueToneFromDiff(diff);
    if (tone === 'overdue') {
      return '#8f2323';
    }
    if (tone === 'near') {
      return '#8a4d00';
    }
    return '#1a5e4d';
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

function quantile(values: number[], percentile: number): number {
  if (values.length === 0) {
    return 0;
  }
  const pos = (values.length - 1) * percentile;
  const base = Math.floor(pos);
  const rest = pos - base;
  const current = values[base] ?? values[values.length - 1];
  const next = values[base + 1] ?? current;
  return current + rest * (next - current);
}

function toDayDiff(isoDate: string): number {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const due = parseIsoLocalDate(isoDate);
  const dueDate = new Date(due.getFullYear(), due.getMonth(), due.getDate()).getTime();
  return Math.floor((dueDate - today) / 86_400_000);
}

function toneColor(tone: 'overdue' | 'near' | 'mid' | 'future'): string {
  if (tone === 'overdue') {
    return '#c73a3a';
  }
  if (tone === 'near') {
    return '#f59a2e';
  }
  if (tone === 'mid') {
    return '#58a882';
  }
  return '#0f5b5a';
}

function parseIsoLocalDate(value: string): Date {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value ?? '').trim());
  if (match) {
    return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  }
  return new Date(value);
}
