import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from '../services/auth.service';

export const admGuard: CanActivateFn = () => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const user = auth.currentUser();

  if (!user) {
    return router.createUrlTree(['/login']);
  }

  const allowed = user.role === 'admin' && user.username.toLowerCase() === 'adm';
  if (allowed) {
    return true;
  }
  return router.createUrlTree(['/app/dashboard']);
};
