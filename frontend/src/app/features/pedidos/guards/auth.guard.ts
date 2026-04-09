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

function getCommercialDirectorUsernames(): string[] {
  return (
    (environment as { commercialDirectorUsernames?: string[] }).commercialDirectorUsernames ?? [
      'marcos',
      'marcos_dronepro'
    ]
  )
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean);
}

function getIsabelUsernames(): string[] {
  return (
    (environment as { isabelUsernames?: string[] }).isabelUsernames ?? ['isabel', 'isabel_dronepro']
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

function getStockManagerUsernames(): string[] {
  return (
    (environment as { stockManagerUsernames?: string[] }).stockManagerUsernames ?? [
      'gerente_estoque',
      'estoque'
    ]
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

  const username = String(user.username || '').trim().toLowerCase();
  const commercialUsers = getCommercialDirectorUsernames();
  if (commercialUsers.includes(username)) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const approvalsAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  const isabelUsers = getIsabelUsernames();
  if (isabelUsers.includes(username)) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const operationalAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  const operationalUsers = getOperationalUsernames();
  if (operationalUsers.includes(username)) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const financialReceiptsAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  const isAllowedUser =
    getFinancialUsernames().includes(username) || getOperationalUsernames().includes(username);
  if (isAllowedUser) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};

export const stockManagerAccessGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const username = String(user.username || '').trim().toLowerCase();
  if (getStockManagerUsernames().includes(username)) {
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

  if (state.url.includes('/app/concluidos') || state.url.includes('/app/comprovantes-financeiros')) {
    return true;
  }
  return router.createUrlTree(['/app/concluidos']);
};
