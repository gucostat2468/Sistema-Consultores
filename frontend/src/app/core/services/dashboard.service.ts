import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, delay, of } from 'rxjs';
import { environment } from '../../../environments/environment';
import {
  ClientHealth,
  Consultant,
  CreditAlert,
  DashboardCustomerItem,
  CreditLimitItem,
  CreditLimitsResponse,
  CreditSummary,
  PortfolioSummary,
  ReceivableItem
} from '../models/dashboard.models';
import { AuthService } from './auth.service';
import { MOCK_CONSULTANTS, MOCK_RECEIVABLES } from './mock-data';

@Injectable({ providedIn: 'root' })
export class DashboardService {
  private readonly http = inject(HttpClient);
  private readonly auth = inject(AuthService);

  getConsultants(): Observable<Consultant[]> {
    if (environment.useMockApi) {
      return of(MOCK_CONSULTANTS).pipe(delay(140));
    }
    return this.http.get<Consultant[]>(`${environment.apiBaseUrl}/consultants`);
  }

  getPortfolioSummary(selectedConsultantId?: number | null): Observable<PortfolioSummary> {
    if (environment.useMockApi) {
      const receivables = this.scopeReceivables(selectedConsultantId);
      return of(buildSummary(receivables)).pipe(delay(180));
    }
    const params = selectedConsultantId ? new HttpParams().set('consultantId', selectedConsultantId) : undefined;
    return this.http.get<PortfolioSummary>(`${environment.apiBaseUrl}/dashboard/summary`, { params });
  }

  getClientHealth(selectedConsultantId?: number | null): Observable<ClientHealth[]> {
    if (environment.useMockApi) {
      const receivables = this.scopeReceivables(selectedConsultantId);
      return of(buildClientHealth(receivables)).pipe(delay(200));
    }
    const params = selectedConsultantId ? new HttpParams().set('consultantId', selectedConsultantId) : undefined;
    return this.http.get<ClientHealth[]>(`${environment.apiBaseUrl}/dashboard/client-health`, { params });
  }

  getReceivables(selectedConsultantId?: number | null): Observable<ReceivableItem[]> {
    if (environment.useMockApi) {
      return of(this.scopeReceivables(selectedConsultantId)).pipe(delay(190));
    }
    const params = selectedConsultantId ? new HttpParams().set('consultantId', selectedConsultantId) : undefined;
    return this.http.get<ReceivableItem[]>(`${environment.apiBaseUrl}/dashboard/receivables`, { params });
  }

  getCreditLimits(selectedConsultantId?: number | null): Observable<CreditLimitsResponse> {
    if (environment.useMockApi) {
      const receivables = this.scopeReceivables(selectedConsultantId);
      return of(buildMockCreditLimits(receivables)).pipe(delay(170));
    }
    const params = selectedConsultantId ? new HttpParams().set('consultantId', selectedConsultantId) : undefined;
    return this.http.get<CreditLimitsResponse>(`${environment.apiBaseUrl}/dashboard/credit-limits`, {
      params
    });
  }

  addCustomer(payload: {
    customerName: string;
    customerCode?: string | null;
    consultantId?: number | null;
  }): Observable<{ item: DashboardCustomerItem }> {
    if (environment.useMockApi) {
      const session = this.auth.currentUser();
      return of({
        item: {
          consultantId: payload.consultantId ?? session?.id ?? 0,
          consultantName: session?.name ?? 'Consultor',
          customerName: payload.customerName.trim(),
          customerCode: String(payload.customerCode ?? '').trim(),
          created: true
        }
      }).pipe(delay(160));
    }

    const formData = new FormData();
    formData.append('customerName', payload.customerName);
    if (payload.customerCode) {
      formData.append('customerCode', payload.customerCode);
    }
    if (payload.consultantId != null) {
      formData.append('consultantId', String(payload.consultantId));
    }
    return this.http.post<{ item: DashboardCustomerItem }>(`${environment.apiBaseUrl}/dashboard/customers`, formData);
  }

  private scopeReceivables(selectedConsultantId?: number | null): ReceivableItem[] {
    const session = this.auth.currentUser();
    if (!session) {
      return [];
    }

    if (session.role === 'consultor') {
      return MOCK_RECEIVABLES
        .filter((item) => item.consultantId === session.id)
        .sort(sortByDueDate);
    }

    const scoped = selectedConsultantId
      ? MOCK_RECEIVABLES.filter((item) => item.consultantId === selectedConsultantId)
      : MOCK_RECEIVABLES;
    return scoped.sort(sortByDueDate);
  }
}

const byCurrency = (value: number): number => Math.round(value * 100) / 100;

const parseIsoLocalDate = (value: string): Date => {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value ?? '').trim());
  if (match) {
    return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  }
  return new Date(value);
};
const parseDate = (value: string): number => parseIsoLocalDate(value).getTime();
const sortByDueDate = (a: ReceivableItem, b: ReceivableItem): number => parseDate(a.dueDate) - parseDate(b.dueDate);

function buildSummary(receivables: ReceivableItem[]): PortfolioSummary {
  const now = new Date();
  const totalBalance = sum(receivables.map((item) => item.balance));
  const overdue = sum(receivables.filter((item) => parseIsoLocalDate(item.dueDate) < now).map((item) => item.balance));
  const due7 = sum(
    receivables
      .filter((item) => {
        const diff = dayDiff(now, parseIsoLocalDate(item.dueDate));
        return diff >= 0 && diff <= 7;
      })
      .map((item) => item.balance)
  );
  const due30 = sum(
    receivables
      .filter((item) => {
        const diff = dayDiff(now, parseIsoLocalDate(item.dueDate));
        return diff >= 0 && diff <= 30;
      })
      .map((item) => item.balance)
  );

  const clients = new Set(receivables.map((item) => item.customerCode)).size;
  const titles = receivables.length;
  const overdueRatio = totalBalance > 0 ? overdue / totalBalance : 0;
  const due7Ratio = totalBalance > 0 ? due7 / totalBalance : 0;

  let guidance =
    'Carteira saudavel: o momento e bom para acelerar propostas com rotina semanal de monitoramento.';
  if (overdueRatio > 0.12) {
    guidance =
      'Carteira em alerta: priorize cobranca ativa e renegociacao antes de ampliar limite comercial.';
  } else if (due7Ratio > 0.35) {
    guidance =
      'Pressao de curto prazo relevante: faca contato preventivo hoje para evitar inadimplencia nos proximos 7 dias.';
  }

  return {
    totalBalance: byCurrency(totalBalance),
    due7: byCurrency(due7),
    due30: byCurrency(due30),
    overdue: byCurrency(overdue),
    clients,
    titles,
    overdueRatio,
    due7Ratio,
    guidance
  };
}

function buildClientHealth(receivables: ReceivableItem[]): ClientHealth[] {
  const byClient = new Map<string, ReceivableItem[]>();
  for (const item of receivables) {
    const key = `${item.consultantId}::${item.customerCode}`;
    const current = byClient.get(key) ?? [];
    current.push(item);
    byClient.set(key, current);
  }

  const now = new Date();
  const clients: ClientHealth[] = [];
  for (const clientReceivables of byClient.values()) {
    const first = clientReceivables[0];
    const enriched = clientReceivables.map((item) => ({
      item,
      dayDiff: dayDiff(now, parseIsoLocalDate(item.dueDate))
    }));

    const totalBalance = sum(enriched.map((entry) => entry.item.balance));
    const overdue = sum(enriched.filter((entry) => entry.dayDiff < 0).map((entry) => entry.item.balance));
    const due7 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= 0 && entry.dayDiff <= 7)
        .map((entry) => entry.item.balance)
    );
    const due30 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= 0 && entry.dayDiff <= 30)
        .map((entry) => entry.item.balance)
    );

    const overdue1To7 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= -7 && entry.dayDiff <= -1)
        .map((entry) => entry.item.balance)
    );
    const overdue8To30 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= -30 && entry.dayDiff <= -8)
        .map((entry) => entry.item.balance)
    );
    const overdue31Plus = sum(
      enriched.filter((entry) => entry.dayDiff <= -31).map((entry) => entry.item.balance)
    );
    const due8To15 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= 8 && entry.dayDiff <= 15)
        .map((entry) => entry.item.balance)
    );
    const due16To30 = sum(
      enriched
        .filter((entry) => entry.dayDiff >= 16 && entry.dayDiff <= 30)
        .map((entry) => entry.item.balance)
    );
    const due15 = due7 + due8To15;

    const overdueTitles = enriched.filter((entry) => entry.dayDiff < 0).length;
    const due15Titles = enriched.filter((entry) => entry.dayDiff >= 0 && entry.dayDiff <= 15).length;
    const maxTitleShare =
      totalBalance > 0 ? Math.max(...enriched.map((entry) => entry.item.balance)) / totalBalance : 0;

    const weightedMeanDays =
      totalBalance > 0
        ? sum(
            enriched.map(
              (entry) => clamp(entry.dayDiff, -45, 60) * entry.item.balance
            )
          ) / totalBalance
        : 60;

    const overdueRatio = totalBalance > 0 ? overdue / totalBalance : 0;
    const due7Ratio = totalBalance > 0 ? due7 / totalBalance : 0;
    const due15Ratio = totalBalance > 0 ? due15 / totalBalance : 0;
    const severeOverdueRatio = totalBalance > 0 ? (overdue8To30 + overdue31Plus) / totalBalance : 0;

    const overdueSeverity =
      totalBalance > 0
        ? clamp((overdue1To7 + overdue8To30 * 1.4 + overdue31Plus * 1.9) / totalBalance, 0, 1)
        : 0;
    const nearPressure =
      totalBalance > 0
        ? clamp((due7 + due8To15 * 0.65 + due16To30 * 0.35) / totalBalance, 0, 1)
        : 0;
    const titleStress =
      enriched.length > 0
        ? clamp((overdueTitles / enriched.length) * 0.75 + (due15Titles / enriched.length) * 0.25, 0, 1)
        : 0;
    const concentrationRisk = clamp((maxTitleShare - 0.35) / 0.65, 0, 1);
    const trajectoryRisk = clamp((12 - weightedMeanDays) / 57, 0, 1);

    const basePenalty =
      overdueSeverity * 60 +
      nearPressure * 18 +
      titleStress * 8 +
      concentrationRisk * 8 +
      trajectoryRisk * 6;
    const escalationPenalty =
      Math.max(0, overdueRatio - 0.35) * 35 +
      Math.max(0, severeOverdueRatio - 0.2) * 20;

    const riskScore = Math.round(clamp(100 - basePenalty - escalationPenalty, 0, 100));
    const status = classifyStatus({
      overdueRatio,
      due15Ratio,
      severeOverdueRatio,
      riskScore
    });

    clients.push({
      consultantId: first.consultantId,
      consultantName: first.consultantName,
      customerName: first.customerName,
      customerCode: first.customerCode,
      totalBalance: byCurrency(totalBalance),
      overdue: byCurrency(overdue),
      due7: byCurrency(due7),
      due30: byCurrency(due30),
      titles: clientReceivables.length,
      score: riskScore,
      status,
      action:
        status === 'Critico'
          ? 'Contato imediato e plano de renegociacao antes de novas propostas.'
          : status === 'Atencao'
            ? 'Contato preventivo para vencimentos proximos e revisao de limite.'
            : 'Cliente apto para novas propostas com monitoramento padrao.'
    });
  }

  return clients.sort((a, b) => b.totalBalance - a.totalBalance);
}

function classifyStatus(
  params: {
    overdueRatio: number;
    due15Ratio: number;
    severeOverdueRatio: number;
    riskScore: number;
  }
): 'Saudavel' | 'Atencao' | 'Critico' {
  if (
    params.riskScore < 45 ||
    params.overdueRatio >= 0.22 ||
    params.severeOverdueRatio >= 0.12
  ) {
    return 'Critico';
  }
  if (
    params.riskScore < 72 ||
    params.overdueRatio >= 0.08 ||
    params.due15Ratio >= 0.38
  ) {
    return 'Atencao';
  }
  return 'Saudavel';
}

function sum(values: number[]): number {
  return values.reduce((acc, current) => acc + current, 0);
}

function dayDiff(now: Date, target: Date): number {
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const end = new Date(target.getFullYear(), target.getMonth(), target.getDate()).getTime();
  return Math.floor((end - start) / 86_400_000);
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function buildMockCreditLimits(receivables: ReceivableItem[]): CreditLimitsResponse {
  const now = new Date();
  const byClient = new Map<string, ReceivableItem[]>();
  for (const item of receivables) {
    const key = `${item.consultantId}::${item.customerCode}`;
    const list = byClient.get(key) ?? [];
    list.push(item);
    byClient.set(key, list);
  }

  const items: CreditLimitItem[] = [];
  for (const group of byClient.values()) {
    const first = group[0];
    const exposure = byCurrency(sum(group.map((item) => item.balance)));
    const overdue = byCurrency(
      sum(
        group
          .filter((item) => dayDiff(now, parseIsoLocalDate(item.dueDate)) < 0)
          .map((item) => item.balance)
      )
    );
    const due7 = byCurrency(
      sum(
        group
          .filter((item) => {
            const diff = dayDiff(now, parseIsoLocalDate(item.dueDate));
            return diff >= 0 && diff <= 7;
          })
          .map((item) => item.balance)
      )
    );

    const creditLimit = byCurrency(exposure * 1.4);
    const creditUsed = byCurrency(exposure * 0.55);
    const creditAvailable = byCurrency(Math.max(creditLimit - creditUsed, 0));
    const usageRatio = creditLimit > 0 ? creditUsed / creditLimit : 0;
    const exposureToAvailableRatio = creditAvailable > 0 ? exposure / creditAvailable : 0;
    const alert = classifyMockCreditAlert(exposure, creditAvailable);

    items.push({
      consultantId: first.consultantId,
      consultantName: first.consultantName,
      customerName: first.customerName,
      customerCode: first.customerCode,
      cnpj: null,
      creditLimit,
      creditUsed,
      creditAvailable,
      exposure,
      overdue,
      due7,
      usageRatio,
      exposureToAvailableRatio,
      alert,
      updatedAt: new Date().toISOString()
    });
  }

  items.sort((a, b) => {
    const byAlert = creditAlertPriority(b.alert) - creditAlertPriority(a.alert);
    if (byAlert !== 0) {
      return byAlert;
    }
    return b.exposureToAvailableRatio - a.exposureToAvailableRatio;
  });

  const summary: CreditSummary = {
    customersWithLimit: items.length,
    customersWithoutLimit: 0,
    totalLimit: byCurrency(sum(items.map((item) => item.creditLimit))),
    totalUsed: byCurrency(sum(items.map((item) => item.creditUsed))),
    totalAvailable: byCurrency(sum(items.map((item) => item.creditAvailable))),
    totalExposure: byCurrency(sum(items.map((item) => item.exposure))),
    totalOverdue: byCurrency(sum(items.map((item) => item.overdue))),
    totalDue7: byCurrency(sum(items.map((item) => item.due7))),
    uncoveredExposure: 0,
    portfolioExposure: byCurrency(sum(items.map((item) => item.exposure))),
    usageRatio: 0,
    exposureToAvailableRatio: 0,
    coverageRatio: 0,
    statusCounts: {
      acimaLimite: items.filter((item) => item.alert === 'Acima do limite').length,
      semLimiteLivre: items.filter((item) => item.alert === 'Sem limite livre').length,
      atencao: items.filter((item) => item.alert === 'Atencao').length,
      controlado: items.filter((item) => item.alert === 'Controlado').length,
      semExposicao: items.filter((item) => item.alert === 'Sem exposicao').length
    }
  };

  summary.usageRatio = summary.totalLimit > 0 ? summary.totalUsed / summary.totalLimit : 0;
  summary.exposureToAvailableRatio =
    summary.totalAvailable > 0 ? summary.totalExposure / summary.totalAvailable : 0;
  summary.coverageRatio = summary.totalExposure > 0 ? summary.totalAvailable / summary.totalExposure : 0;

  return { summary, items };
}

function classifyMockCreditAlert(exposure: number, available: number): CreditAlert {
  if (exposure <= 0 && available > 0) {
    return 'Sem exposicao';
  }
  if (exposure > 0 && available <= 0) {
    return 'Sem limite livre';
  }
  if (available <= 0) {
    return 'Sem dados';
  }
  const ratio = exposure / available;
  if (ratio > 1) {
    return 'Acima do limite';
  }
  if (ratio >= 0.8) {
    return 'Atencao';
  }
  return 'Controlado';
}

function creditAlertPriority(alert: CreditAlert): number {
  const order: Record<CreditAlert, number> = {
    'Acima do limite': 5,
    'Sem limite livre': 4,
    Atencao: 3,
    Controlado: 2,
    'Sem exposicao': 1,
    'Sem dados': 0
  };
  return order[alert] ?? 0;
}
