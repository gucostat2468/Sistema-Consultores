import { CommonModule } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { distinctUntilChanged, forkJoin, map, switchMap } from 'rxjs';
import { CreditLimitItem, ReceivableItem } from '../../../../core/models/dashboard.models';
import { DashboardService } from '../../../../core/services/dashboard.service';
import { daysUntil, toCurrency, toDate, toPercent } from '../../../../shared/utils/format';
import {
  buildCreditLookupMaps,
  evaluateCreditMetrics,
  findCreditItem,
  toneToCoverageClass
} from '../../../../shared/utils/credit-metrics';

type HorizonFilter = 'Todos' | 'Vencido' | '0-7' | '8-30' | '30+';
type ReceivablesSortBy = 'dueDate' | 'balance' | 'customer';
type SortDirection = 'asc' | 'desc';

interface ReceivableCompanyGroup {
  key: string;
  consultantId: number;
  customerName: string;
  customerCode: string;
  consultantName: string;
  titles: number;
  totalBalance: number;
  firstIssueDate: string;
  nextDueDate: string;
  overdueTitles: number;
  dueTodayTitles: number;
  minFutureDays: number | null;
}

interface ReceivablesSnapshot {
  titles: number;
  companies: number;
  totalBalance: number;
  overdueTitles: number;
  overdueBalance: number;
  due7Titles: number;
  due7Balance: number;
  largestTitleBalance: number;
  largestTitleCustomer: string;
}

@Component({
  selector: 'app-receivables-page',
  imports: [CommonModule, FormsModule],
  templateUrl: './receivables.page.html',
  styleUrl: './receivables.page.scss'
})
export class ReceivablesPage {
  private readonly route = inject(ActivatedRoute);
  private readonly dashboardService = inject(DashboardService);
  private readonly destroyRef = inject(DestroyRef);

  readonly loading = signal(true);
  readonly items = signal<ReceivableItem[]>([]);
  readonly creditItems = signal<CreditLimitItem[]>([]);
  readonly horizon = signal<HorizonFilter>('Todos');
  readonly search = signal('');
  readonly groupByCompany = signal(true);
  readonly sortBy = signal<ReceivablesSortBy>('dueDate');
  readonly sortDirection = signal<SortDirection>('asc');
  readonly debtByCompany = computed(() => {
    const mapByCompany = new Map<string, number>();
    for (const item of this.items()) {
      const key = this.buildCompanyKey(item);
      mapByCompany.set(key, (mapByCompany.get(key) ?? 0) + item.balance);
    }
    return mapByCompany;
  });
  readonly overdueByCompany = computed(() => {
    const mapByCompany = new Map<string, number>();
    for (const item of this.items()) {
      if (daysUntil(item.dueDate) >= 0) {
        continue;
      }
      const key = this.buildCompanyKey(item);
      mapByCompany.set(key, (mapByCompany.get(key) ?? 0) + item.balance);
    }
    return mapByCompany;
  });
  readonly creditLookup = computed(() => buildCreditLookupMaps(this.creditItems()));

  constructor() {
    this.route.queryParamMap
      .pipe(
        map((params) => {
          const raw = params.get('consultantId');
          return raw ? Number(raw) : null;
        }),
        distinctUntilChanged(),
        switchMap((consultantId) => {
          this.loading.set(true);
          return forkJoin({
            receivables: this.dashboardService.getReceivables(consultantId),
            creditLimits: this.dashboardService.getCreditLimits(consultantId)
          });
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(({ receivables, creditLimits }) => {
        this.items.set(receivables);
        this.creditItems.set(creditLimits.items);
        this.loading.set(false);
      });
  }

  setHorizon(value: HorizonFilter): void {
    this.horizon.set(value);
  }

  setSearch(value: string): void {
    this.search.set(value);
  }

  setGroupByCompany(value: boolean): void {
    this.groupByCompany.set(value);
  }

  setSortBy(value: ReceivablesSortBy): void {
    this.sortBy.set(value);
  }

  setSortDirection(value: SortDirection): void {
    this.sortDirection.set(value);
  }

  get filteredSorted(): ReceivableItem[] {
    const horizon = this.horizon();
    const query = this.search().trim().toLowerCase();

    const filtered = this.items().filter((item) => {
      const days = daysUntil(item.dueDate);
      const matchesHorizon =
        horizon === 'Todos' ||
        (horizon === 'Vencido' && days < 0) ||
        (horizon === '0-7' && days >= 0 && days <= 7) ||
        (horizon === '8-30' && days >= 8 && days <= 30) ||
        (horizon === '30+' && days > 30);

      const matchesSearch =
        query.length === 0 ||
        item.customerName.toLowerCase().includes(query) ||
        item.customerCode.toLowerCase().includes(query) ||
        item.documentRef.toLowerCase().includes(query);

      return matchesHorizon && matchesSearch;
    });

    return filtered.sort((a, b) => this.compareReceivables(a, b));
  }

  get groupedFiltered(): ReceivableCompanyGroup[] {
    const grouped = new Map<string, ReceivableCompanyGroup>();

    for (const item of this.filteredSorted) {
      const key = this.buildCompanyKey(item);
      const days = daysUntil(item.dueDate);
      const existing = grouped.get(key);

      if (!existing) {
        grouped.set(key, {
          key,
          consultantId: item.consultantId,
          customerName: item.customerName,
          customerCode: item.customerCode,
          consultantName: item.consultantName,
          titles: 1,
          totalBalance: item.balance,
          firstIssueDate: item.issueDate,
          nextDueDate: item.dueDate,
          overdueTitles: days < 0 ? 1 : 0,
          dueTodayTitles: days === 0 ? 1 : 0,
          minFutureDays: days > 0 ? days : null
        });
        continue;
      }

      existing.titles += 1;
      existing.totalBalance += item.balance;
      if (item.issueDate < existing.firstIssueDate) {
        existing.firstIssueDate = item.issueDate;
      }
      if (item.dueDate < existing.nextDueDate) {
        existing.nextDueDate = item.dueDate;
      }
      if (days < 0) {
        existing.overdueTitles += 1;
      } else if (days === 0) {
        existing.dueTodayTitles += 1;
      } else if (existing.minFutureDays === null || days < existing.minFutureDays) {
        existing.minFutureDays = days;
      }
    }

    return [...grouped.values()].sort((a, b) => this.compareGroups(a, b));
  }

  get snapshot(): ReceivablesSnapshot {
    const rows = this.filteredSorted;
    const totalBalance = rows.reduce((acc, item) => acc + item.balance, 0);
    const companies = new Set(rows.map((item) => this.buildCompanyKey(item))).size;

    const overdueItems = rows.filter((item) => daysUntil(item.dueDate) < 0);
    const due7Items = rows.filter((item) => {
      const days = daysUntil(item.dueDate);
      return days >= 0 && days <= 7;
    });
    const largest = rows.reduce<ReceivableItem | null>((current, item) => {
      if (!current) {
        return item;
      }
      return item.balance > current.balance ? item : current;
    }, null);

    return {
      titles: rows.length,
      companies,
      totalBalance,
      overdueTitles: overdueItems.length,
      overdueBalance: overdueItems.reduce((acc, item) => acc + item.balance, 0),
      due7Titles: due7Items.length,
      due7Balance: due7Items.reduce((acc, item) => acc + item.balance, 0),
      largestTitleBalance: largest?.balance ?? 0,
      largestTitleCustomer: largest?.customerName ?? '-'
    };
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

  dueBadge(item: ReceivableItem): string {
    const days = daysUntil(item.dueDate);
    if (days < 0) {
      return `vencido ha ${Math.abs(days)}d`;
    }
    if (days === 0) {
      return 'vence hoje';
    }
    return `vence em ${days}d`;
  }

  dueClass(item: ReceivableItem): string {
    const days = daysUntil(item.dueDate);
    if (days < 0) {
      return 'due-overdue';
    }
    if (days <= 7) {
      return 'due-near';
    }
    return 'due-safe';
  }

  groupDueBadge(item: ReceivableCompanyGroup): string {
    if (item.overdueTitles > 0) {
      return `${item.overdueTitles} vencidos`;
    }
    if (item.dueTodayTitles > 0) {
      return 'vence hoje';
    }
    if (item.minFutureDays === null) {
      return 'sem vencimento';
    }
    return `vence em ${item.minFutureDays}d`;
  }

  groupDueClass(item: ReceivableCompanyGroup): string {
    if (item.overdueTitles > 0) {
      return 'due-overdue';
    }
    if (item.dueTodayTitles > 0 || (item.minFutureDays !== null && item.minFutureDays <= 7)) {
      return 'due-near';
    }
    return 'due-safe';
  }

  groupWindowLabel(item: ReceivableCompanyGroup): string {
    if (item.overdueTitles > 0) {
      return 'Com atraso';
    }
    if (item.dueTodayTitles > 0) {
      return 'Vence hoje';
    }
    if (item.minFutureDays === null) {
      return 'Sem prazo';
    }
    if (item.minFutureDays <= 7) {
      return 'Curto prazo';
    }
    if (item.minFutureDays <= 30) {
      return 'Medio prazo';
    }
    return 'Longo prazo';
  }

  groupCreditLimit(item: ReceivableCompanyGroup): number {
    return this.metricsForGroup(item).limit;
  }

  groupDebtOpen(item: ReceivableCompanyGroup): number {
    return this.metricsForGroup(item).debtOpen;
  }

  groupCoverageRatio(item: ReceivableCompanyGroup): number {
    return this.metricsForGroup(item).coverageRatio;
  }

  groupDebtToLimitRatio(item: ReceivableCompanyGroup): number {
    return this.metricsForGroup(item).debtToLimitRatio;
  }

  groupCoverageLabel(item: ReceivableCompanyGroup): string {
    return this.metricsForGroup(item).label;
  }

  groupCoverageClass(item: ReceivableCompanyGroup): string {
    return toneToCoverageClass(this.metricsForGroup(item).tone);
  }

  groupCoverageHint(item: ReceivableCompanyGroup): string {
    return this.metricsForGroup(item).hint;
  }

  rowCreditLimit(item: ReceivableItem): number {
    return this.metricsForItem(item).limit;
  }

  rowDebtOpen(item: ReceivableItem): number {
    return this.metricsForItem(item).debtOpen;
  }

  rowCoverageRatio(item: ReceivableItem): number {
    return this.metricsForItem(item).coverageRatio;
  }

  rowDebtToLimitRatio(item: ReceivableItem): number {
    return this.metricsForItem(item).debtToLimitRatio;
  }

  rowCoverageLabel(item: ReceivableItem): string {
    return this.metricsForItem(item).label;
  }

  rowCoverageClass(item: ReceivableItem): string {
    return toneToCoverageClass(this.metricsForItem(item).tone);
  }

  rowCoverageHint(item: ReceivableItem): string {
    return this.metricsForItem(item).hint;
  }

  private compareReceivables(a: ReceivableItem, b: ReceivableItem): number {
    const sortBy = this.sortBy();
    const direction = this.sortDirection() === 'asc' ? 1 : -1;

    if (sortBy === 'balance') {
      const value = a.balance - b.balance;
      if (value !== 0) {
        return value * direction;
      }
      return a.dueDate.localeCompare(b.dueDate) * direction;
    }

    if (sortBy === 'customer') {
      const byName = a.customerName.localeCompare(b.customerName);
      if (byName !== 0) {
        return byName * direction;
      }
      return a.dueDate.localeCompare(b.dueDate) * direction;
    }

    const byDueDate = a.dueDate.localeCompare(b.dueDate);
    if (byDueDate !== 0) {
      return byDueDate * direction;
    }
    return (a.balance - b.balance) * direction;
  }

  private compareGroups(a: ReceivableCompanyGroup, b: ReceivableCompanyGroup): number {
    const sortBy = this.sortBy();
    const direction = this.sortDirection() === 'asc' ? 1 : -1;

    if (sortBy === 'balance') {
      const value = a.totalBalance - b.totalBalance;
      if (value !== 0) {
        return value * direction;
      }
      return a.nextDueDate.localeCompare(b.nextDueDate) * direction;
    }

    if (sortBy === 'customer') {
      const byName = a.customerName.localeCompare(b.customerName);
      if (byName !== 0) {
        return byName * direction;
      }
      return a.nextDueDate.localeCompare(b.nextDueDate) * direction;
    }

    const byDueDate = a.nextDueDate.localeCompare(b.nextDueDate);
    if (byDueDate !== 0) {
      return byDueDate * direction;
    }
    return (a.totalBalance - b.totalBalance) * direction;
  }

  private buildCompanyKey(item: ReceivableItem): string {
    const code = item.customerCode?.trim();
    if (code) {
      return `${item.consultantId}::${code}`;
    }
    return `${item.consultantId}::${item.customerName.toUpperCase()}`;
  }

  private creditForGroup(item: ReceivableCompanyGroup): CreditLimitItem | null {
    return findCreditItem(
      this.creditLookup(),
      item.consultantId,
      item.customerCode,
      item.customerName
    );
  }

  private creditForItem(item: ReceivableItem): CreditLimitItem | null {
    return findCreditItem(
      this.creditLookup(),
      item.consultantId,
      item.customerCode,
      item.customerName
    );
  }

  private metricsForGroup(item: ReceivableCompanyGroup) {
    return evaluateCreditMetrics({
      debtOpen: this.debtByCompany().get(item.key) ?? item.totalBalance,
      overdue: this.overdueByCompany().get(item.key) ?? 0,
      credit: this.creditForGroup(item)
    });
  }

  private metricsForItem(item: ReceivableItem) {
    const key = this.buildCompanyKey(item);
    return evaluateCreditMetrics({
      debtOpen: this.debtByCompany().get(key) ?? item.balance,
      overdue: this.overdueByCompany().get(key) ?? 0,
      credit: this.creditForItem(item)
    });
  }
}
