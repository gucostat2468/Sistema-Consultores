import { CommonModule } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { distinctUntilChanged, forkJoin, map, switchMap } from 'rxjs';
import { environment } from '../../../../../environments/environment';
import { ClientHealth, Consultant, CreditLimitItem } from '../../../../core/models/dashboard.models';
import { AuthService } from '../../../../core/services/auth.service';
import { DashboardService } from '../../../../core/services/dashboard.service';
import { EncaminharModalComponent } from '../../../pedidos/components/encaminhar-modal/encaminhar-modal.component';
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
  imports: [CommonModule, FormsModule, RouterLink, EncaminharModalComponent],
  templateUrl: './clients.page.html',
  styleUrl: './clients.page.scss'
})
export class ClientsPage {
  private static readonly STORAGE_PREFIX = 'clients:view:';
  private readonly route = inject(ActivatedRoute);
  private readonly auth = inject(AuthService);
  private readonly dashboardService = inject(DashboardService);
  private readonly destroyRef = inject(DestroyRef);
  private pendingScrollRestore: number | null = null;

  readonly loading = signal(true);
  readonly items = signal<ClientHealth[]>([]);
  readonly creditItems = signal<CreditLimitItem[]>([]);
  readonly search = signal('');
  readonly statusFilter = signal<'Todos' | ClientHealth['status']>('Todos');
  readonly selectedConsultantId = signal<number | null>(null);
  readonly manualConsultants = signal<Consultant[]>([]);
  readonly manualConsultantId = signal<number | null>(null);
  readonly creditLookup = computed(() => buildCreditLookupMaps(this.creditItems()));
  readonly modalClient = signal<ClientHealth | null>(null);
  readonly orderFeedback = signal<string | null>(null);
  readonly addingCustomer = signal(false);
  readonly newCustomerName = signal('');
  readonly newCustomerCode = signal('');
  readonly newCustomerFeedback = signal<string | null>(null);
  readonly newCustomerError = signal<string | null>(null);
  readonly needsManualConsultantSelection = computed(
    () => this.requiresScopedConsultantForManualAdd() && this.selectedConsultantId() == null
  );
  readonly canAddCustomer = computed(
    () => !this.needsManualConsultantSelection() || this.manualConsultantId() != null
  );
  readonly canForwardOrder = computed(() => {
    const usernames = (
      (environment as { operationalUsernames?: string[] }).operationalUsernames ?? [
        'isabel',
        'isabel_dronepro',
        'marcos',
        'marcos_dronepro'
      ]
    )
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
    const username = this.auth.currentUser()?.username?.toLowerCase() ?? '';
    return usernames.includes(username);
  });

  constructor() {
    this.destroyRef.onDestroy(() => this.rememberContext());
    this.loadManualConsultantsIfNeeded();

    this.route.queryParamMap
      .pipe(
        map((params) => {
          const raw = params.get('consultantId');
          const consultantId = raw ? Number(raw) : null;
          this.selectedConsultantId.set(consultantId);
          if (consultantId != null) {
            this.manualConsultantId.set(consultantId);
          }
          this.restoreContextForScope(consultantId);
          return consultantId;
        }),
        distinctUntilChanged(),
        switchMap((consultantId) => {
          this.loading.set(true);
          return this.loadScopeData(consultantId);
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

  setNewCustomerName(value: string): void {
    this.newCustomerName.set(value);
    this.newCustomerError.set(null);
    this.newCustomerFeedback.set(null);
  }

  setNewCustomerCode(value: string): void {
    this.newCustomerCode.set(value);
    this.newCustomerError.set(null);
    this.newCustomerFeedback.set(null);
  }

  setManualConsultantId(value: string | number | null): void {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) {
      this.manualConsultantId.set(parsed);
    } else {
      this.manualConsultantId.set(null);
    }
    this.newCustomerError.set(null);
    this.newCustomerFeedback.set(null);
  }

  addCustomer(): void {
    if (this.addingCustomer()) {
      return;
    }
    if (!this.canAddCustomer()) {
      this.newCustomerError.set('Selecione um consultor para cadastrar cliente neste escopo.');
      return;
    }

    const customerName = this.newCustomerName().trim();
    const customerCode = this.newCustomerCode().trim();
    if (!customerName) {
      this.newCustomerError.set('Informe o nome do cliente.');
      return;
    }

    this.addingCustomer.set(true);
    this.newCustomerError.set(null);
    this.newCustomerFeedback.set(null);
    const currentScopeId = this.selectedConsultantId();
    const targetConsultantId = this.resolveTargetConsultantIdForManualAdd();
    this.dashboardService
      .addCustomer({
        customerName,
        customerCode: customerCode || null,
        consultantId: targetConsultantId
      })
      .pipe(
        switchMap((response) =>
          this.loadScopeData(currentScopeId).pipe(map((data) => ({ response, data })))
        ),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe({
        next: ({ response, data }) => {
          this.items.set(data.clientHealth);
          this.creditItems.set(data.creditLimits.items);
          this.search.set('');
          this.statusFilter.set('Todos');
          this.newCustomerName.set('');
          this.newCustomerCode.set('');
          this.newCustomerFeedback.set(`Cliente ${response.item.customerName} cadastrado com sucesso.`);
          this.rememberContext();
          this.addingCustomer.set(false);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao cadastrar cliente.';
          this.newCustomerError.set(String(detail));
          this.addingCustomer.set(false);
        }
      });
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
    const normalizedCode = (client.customerCode ?? '').trim();
    if (normalizedCode) {
      return `${client.consultantId}::code::${normalizedCode}`;
    }
    return `${client.consultantId}::name::${this.normalizeCustomerKey(client.customerName)}`;
  }

  buildClientTrackKey(client: ClientHealth, index: number): string {
    return `${this.buildClientKey(client)}::${index}`;
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

  hasCreditLimit(client: ClientHealth): boolean {
    return this.clientCreditLimit(client) > 0;
  }

  openForwardModal(client: ClientHealth): void {
    if (!this.canForwardOrder()) {
      this.orderFeedback.set('Encaminhamento de pedido permitido apenas para Marcos e Isabel.');
      return;
    }
    this.modalClient.set(client);
    this.orderFeedback.set(null);
  }

  closeForwardModal(): void {
    this.modalClient.set(null);
  }

  onOrderForwarded(): void {
    this.orderFeedback.set('Pedido registrado. Consulte a sessão Status para acompanhar o fluxo.');
    this.modalClient.set(null);
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

  private loadScopeData(consultantId: number | null) {
    return forkJoin({
      clientHealth: this.dashboardService.getClientHealth(consultantId),
      creditLimits: this.dashboardService.getCreditLimits(consultantId)
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

  private normalizeCustomerKey(value: string): string {
    return String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, ' ')
      .trim();
  }

  private resolveTargetConsultantIdForManualAdd(): number | null {
    return this.selectedConsultantId() ?? this.manualConsultantId();
  }

  private loadManualConsultantsIfNeeded(): void {
    if (!this.hasGlobalScopeForManualAdd()) {
      return;
    }
    this.dashboardService
      .getConsultants()
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (consultants) => {
          const sorted = [...consultants].sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));
          this.manualConsultants.set(sorted);
          if (this.selectedConsultantId() == null && this.manualConsultantId() == null && sorted.length === 1) {
            this.manualConsultantId.set(sorted[0].id);
          }
        },
        error: () => {
          this.manualConsultants.set([]);
        }
      });
  }

  private hasGlobalScopeForManualAdd(): boolean {
    const user = this.auth.currentUser();
    if (!user) {
      return false;
    }
    const username = String(user.username || '').trim().toLowerCase();
    return user.role === 'admin' || this.financialUsernames().includes(username);
  }

  private financialUsernames(): string[] {
    return ((environment as { financialUsernames?: string[] }).financialUsernames ?? [])
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
  }

  private requiresScopedConsultantForManualAdd(): boolean {
    return this.hasGlobalScopeForManualAdd() && this.selectedConsultantId() == null;
  }
}
