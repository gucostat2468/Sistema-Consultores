import { CommonModule } from '@angular/common';
import { Component, DestroyRef, computed, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, NavigationEnd, Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { filter } from 'rxjs';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { environment } from '../../../../environments/environment';
import { Consultant } from '../../../core/models/dashboard.models';
import { AuthService } from '../../../core/services/auth.service';
import { DashboardService } from '../../../core/services/dashboard.service';

@Component({
  selector: 'app-shell',
  imports: [CommonModule, FormsModule, RouterOutlet, RouterLink, RouterLinkActive],
  templateUrl: './app-shell.component.html',
  styleUrl: './app-shell.component.scss'
})
export class AppShellComponent {
  private readonly auth = inject(AuthService);
  private readonly dashboardService = inject(DashboardService);
  private readonly router = inject(Router);
  private readonly route = inject(ActivatedRoute);
  private readonly destroyRef = inject(DestroyRef);

  readonly user = this.auth.currentUser;
  readonly consultants = signal<Consultant[]>([]);
  readonly selectedConsultantId = signal<number | null>(null);
  readonly mobileNavOpen = signal(false);
  readonly isAdmin = computed(() => this.user()?.role === 'admin');
  readonly isFinancialUser = computed(() => {
    const usernames = (
      (environment as { financialUsernames?: string[] }).financialUsernames ?? ['vitor_financeiro']
    )
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
    const username = String(this.user()?.username || '').trim().toLowerCase();
    return usernames.includes(username);
  });
  readonly isOperationalUser = computed(() => {
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
    const username = String(this.user()?.username || '').trim().toLowerCase();
    return usernames.includes(username);
  });
  readonly canAccessStatus = computed(() => this.isOperationalUser());
  readonly canAccessReportUpdate = computed(() => this.isOperationalUser());
  readonly canAccessFinancialReceipts = computed(() => this.isFinancialUser());
  readonly navQueryParams = computed(() => {
    const consultantId = this.selectedConsultantId();
    return consultantId ? { consultantId } : {};
  });
  readonly routeTitle = signal('Dashboard');

  constructor() {
    this.route.queryParamMap.pipe(takeUntilDestroyed(this.destroyRef)).subscribe((params) => {
      const raw = params.get('consultantId');
      this.selectedConsultantId.set(raw ? Number(raw) : null);
    });

    if (this.isAdmin()) {
      this.dashboardService
        .getConsultants()
        .pipe(takeUntilDestroyed(this.destroyRef))
        .subscribe({
          next: (consultants) => this.consultants.set(consultants),
          error: () => {
            // Session expiry is handled by auth interceptor; keep shell stable.
            this.consultants.set([]);
          }
        });
    }

    this.router.events
      .pipe(
        filter((event): event is NavigationEnd => event instanceof NavigationEnd),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(() => {
        this.refreshRouteTitle();
        this.closeMobileNav();
      });
    this.refreshRouteTitle();
  }

  logout(): void {
    this.auth.logout();
    this.router.navigateByUrl('/login');
  }

  setScope(value: string): void {
    const consultantId = value ? Number(value) : null;
    const queryParams = consultantId ? { consultantId } : { consultantId: null };
    this.router.navigate([], {
      relativeTo: this.route,
      queryParams,
      queryParamsHandling: 'merge'
    });
  }

  toggleMobileNav(): void {
    this.mobileNavOpen.update((value) => !value);
  }

  closeMobileNav(): void {
    this.mobileNavOpen.set(false);
  }

  private refreshRouteTitle(): void {
    const url = this.router.url;
    if (url.includes('/clientes')) {
      this.routeTitle.set('Clientes');
      return;
    }
    if (url.includes('/titulos')) {
      this.routeTitle.set('Titulos');
      return;
    }
    if (url.includes('/analise-cliente')) {
      this.routeTitle.set('Analise Cliente');
      return;
    }
    if (url.includes('/status')) {
      this.routeTitle.set('Status');
      return;
    }
    if (url.includes('/comprovantes-financeiros')) {
      this.routeTitle.set('Comprovantes Financeiros');
      return;
    }
    if (url.includes('/atualizacao-report')) {
      this.routeTitle.set('Atualizacao Report');
      return;
    }
    if (url.includes('/atualizacao')) {
      this.routeTitle.set('Atualizacao');
      return;
    }
    this.routeTitle.set('Dashboard');
  }
}
