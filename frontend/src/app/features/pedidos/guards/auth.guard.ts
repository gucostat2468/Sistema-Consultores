import { inject } from '@angular/core';
import { CanActivateChildFn, CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../../../core/services/auth.service';
import { environment } from '../../../../environments/environment';

function getOperationalUsernames(): string[] {
  return (
    (environment as { operationalUsernames?: string[] }).operationalUsernames ?? [
      'isabel',
      'isabel_dronepro',
      'marcos',
      'marcos_dronepro'
    ]
  )
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean);
}

function getFinancialUsernames(): string[] {
  return (
    (environment as { financialUsernames?: string[] }).financialUsernames ?? ['vitor_financeiro']
  )
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean);
}

export const statusAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const operationalUsers = getOperationalUsernames();
  const isOperationalUser = operationalUsers.includes(String(user.username || '').trim().toLowerCase());
  if (isOperationalUser) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const operationalAccessGuard = statusAccessGuard;

export const financialReceiptsAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  const isFinancialUser = getFinancialUsernames().includes(username);
  if (isFinancialUser) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const financialIsolationGuard: CanActivateChildFn = (_route, state) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  const isFinancialUser = getFinancialUsernames().includes(username);
  if (!isFinancialUser) {
    return true;
  }

  if (state.url.includes('/app/comprovantes-financeiros')) {
    return true;
  }
  return router.createUrlTree(['/app/comprovantes-financeiros']);
};
