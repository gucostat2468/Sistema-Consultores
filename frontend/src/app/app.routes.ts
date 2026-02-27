import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';
import { admGuard } from './core/guards/adm.guard';
import { guestGuard } from './core/guards/guest.guard';

export const routes: Routes = [
  {
    path: 'login',
    canActivate: [guestGuard],
    loadComponent: () =>
      import('./features/auth/pages/login/login.page').then((m) => m.LoginPage)
  },
  {
    path: 'app',
    canActivate: [authGuard],
    loadComponent: () =>
      import('./layout/components/app-shell/app-shell.component').then((m) => m.AppShellComponent),
    children: [
      {
        path: 'dashboard',
        loadComponent: () =>
          import('./features/dashboard/pages/dashboard/dashboard.page').then((m) => m.DashboardPage)
      },
      {
        path: 'clientes',
        loadComponent: () =>
          import('./features/clients/pages/clients/clients.page').then((m) => m.ClientsPage)
      },
      {
        path: 'titulos',
        loadComponent: () =>
          import('./features/receivables/pages/receivables/receivables.page').then(
            (m) => m.ReceivablesPage
          )
      },
      {
        path: 'analise-cliente',
        loadComponent: () =>
          import('./features/client-analysis/pages/client-analysis/client-analysis.page').then(
            (m) => m.ClientAnalysisPage
          )
      },
      {
        path: 'atualizacao',
        canActivate: [admGuard],
        loadComponent: () =>
          import('./features/admin-import/pages/admin-import/admin-import.page').then(
            (m) => m.AdminImportPage
          )
      },
      {
        path: 'atualizacao-report',
        canActivate: [admGuard],
        loadComponent: () =>
          import('./features/report-import/pages/report-import/report-import.page').then(
            (m) => m.ReportImportPage
          )
      },
      { path: '', pathMatch: 'full', redirectTo: 'dashboard' }
    ]
  },
  { path: '', pathMatch: 'full', redirectTo: 'app/dashboard' },
  { path: '**', redirectTo: 'app/dashboard' }
];
