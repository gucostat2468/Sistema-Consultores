import { CommonModule } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { distinctUntilChanged, forkJoin, map, switchMap } from 'rxjs';
import { ClientHealth, CreditLimitItem } from '../../../../core/models/dashboard.models';
import { DashboardService } from '../../../../core/services/dashboard.service';
import { toCurrency, toPercent } from '../../../../shared/utils/format';
import {
  buildCreditLookupMaps,
  evaluateCreditMetrics,
  findCreditItem,
  toneToCoverageClass
} from '../../../../shared/utils/credit-metrics';

interface ClientsViewContext {
  search: string;
  status: 'Todos' | ClientHealth['status'];
  scrollY: number;
}

@Component({
  selector: 'app-clients-page',
  imports: [CommonModule, FormsModule, RouterLink],
  templateUrl: './clients.page.html',
  styleUrl: './clients.page.scss'
})
export class ClientsPage {
  private static readonly STORAGE_PREFIX = 'clients:view:';
  private readonly route = inject(ActivatedRoute);
  private readonly dashboardService = inject(DashboardService);
  private readonly destroyRef = inject(DestroyRef);
  private pendingScrollRestore: number | null = null;

  readonly loading = signal(true);
  readonly items = signal<ClientHealth[]>([]);
  readonly creditItems = signal<CreditLimitItem[]>([]);
  readonly search = signal('');
  readonly statusFilter = signal<'Todos' | ClientHealth['status']>('Todos');
  readonly selectedConsultantId = signal<number | null>(null);
  readonly creditLookup = computed(() => buildCreditLookupMaps(this.creditItems()));

  constructor() {
    this.destroyRef.onDestroy(() => this.rememberContext());

    this.route.queryParamMap
      .pipe(
        map((params) => {
          const raw = params.get('consultantId');
          const consultantId = raw ? Number(raw) : null;
          this.selectedConsultantId.set(consultantId);
          this.restoreContextForScope(consultantId);
          return consultantId;
        }),
        distinctUntilChanged(),
        switchMap((consultantId) => {
          this.loading.set(true);
          return forkJoin({
            clientHealth: this.dashboardService.getClientHealth(consultantId),
            creditLimits: this.dashboardService.getCreditLimits(consultantId)
          });
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(({ clientHealth, creditLimits }) => {
        this.items.set(clientHealth);
        this.creditItems.set(creditLimits.items);
        this.loading.set(false);
        this.restoreScrollPosition();
      });
  }

  setSearch(value: string): void {
    this.search.set(value);
    this.rememberContext();
  }

  setStatus(value: 'Todos' | ClientHealth['status']): void {
    this.statusFilter.set(value);
    this.rememberContext();
  }

  get filtered(): ClientHealth[] {
    const query = this.search().trim().toLowerCase();
    const status = this.statusFilter();

    return this.items().filter((item) => {
      const matchesQuery =
        query.length === 0 ||
        item.customerName.toLowerCase().includes(query) ||
        item.customerCode.toLowerCase().includes(query);
      const matchesStatus = status === 'Todos' || item.status === status;
      return matchesQuery && matchesStatus;
    });
  }

  badgeClass(status: ClientHealth['status']): string {
    return `status status-${status.toLowerCase()}`;
  }

  currency(value: number): string {
    return toCurrency(value);
  }

  percent(value: number): string {
    return toPercent(value);
  }

  buildClientKey(client: ClientHealth): string {
    return `${client.consultantId}::${client.customerCode}`;
  }

  clientCreditLimit(client: ClientHealth): number {
    return this.metricsForClient(client).limit;
  }

  clientCreditAvailable(client: ClientHealth): number {
    return this.metricsForClient(client).available;
  }

  clientCoverageRatio(client: ClientHealth): number {
    return this.metricsForClient(client).coverageRatio;
  }

  clientDebtToLimitRatio(client: ClientHealth): number {
    return this.metricsForClient(client).debtToLimitRatio;
  }

  clientCoverageLabel(client: ClientHealth): string {
    return this.metricsForClient(client).label;
  }

  clientCoverageClass(client: ClientHealth): string {
    return toneToCoverageClass(this.metricsForClient(client).tone);
  }

  clientCoverageHint(client: ClientHealth): string {
    return this.metricsForClient(client).hint;
  }

  analysisQuery(client: ClientHealth): Record<string, string | number | null> {
    return {
      consultantId: this.selectedConsultantId(),
      clientKey: this.buildClientKey(client)
    };
  }

  rememberContext(): void {
    if (typeof window === 'undefined') {
      return;
    }
    const context: ClientsViewContext = {
      search: this.search(),
      status: this.statusFilter(),
      scrollY: window.scrollY || 0
    };
    window.sessionStorage.setItem(this.storageKey(this.selectedConsultantId()), JSON.stringify(context));
  }

  private findCreditForClient(client: ClientHealth): CreditLimitItem | null {
    return findCreditItem(
      this.creditLookup(),
      client.consultantId,
      client.customerCode,
      client.customerName
    );
  }

  private metricsForClient(client: ClientHealth) {
    return evaluateCreditMetrics({
      debtOpen: client.totalBalance,
      overdue: client.overdue,
      credit: this.findCreditForClient(client)
    });
  }

  private restoreContextForScope(consultantId: number | null): void {
    if (typeof window === 'undefined') {
      return;
    }
    const raw = window.sessionStorage.getItem(this.storageKey(consultantId));
    if (!raw) {
      this.search.set('');
      this.statusFilter.set('Todos');
      this.pendingScrollRestore = 0;
      return;
    }
    try {
      const parsed = JSON.parse(raw) as Partial<ClientsViewContext>;
      this.search.set(typeof parsed.search === 'string' ? parsed.search : '');
      this.statusFilter.set(this.normalizeStatus(parsed.status));
      this.pendingScrollRestore =
        typeof parsed.scrollY === 'number' && Number.isFinite(parsed.scrollY) && parsed.scrollY >= 0
          ? parsed.scrollY
          : 0;
    } catch {
      this.search.set('');
      this.statusFilter.set('Todos');
      this.pendingScrollRestore = 0;
    }
  }

  private restoreScrollPosition(): void {
    if (typeof window === 'undefined' || this.pendingScrollRestore === null) {
      return;
    }
    const target = this.pendingScrollRestore;
    this.pendingScrollRestore = null;
    requestAnimationFrame(() => window.scrollTo({ top: target, behavior: 'auto' }));
  }

  private storageKey(consultantId: number | null): string {
    const scope = consultantId === null ? 'all' : String(consultantId);
    return `${ClientsPage.STORAGE_PREFIX}${scope}`;
  }

  private normalizeStatus(value: unknown): 'Todos' | ClientHealth['status'] {
    if (value === 'Saudavel' || value === 'Atencao' || value === 'Critico') {
      return value;
    }
    return 'Todos';
  }
}
