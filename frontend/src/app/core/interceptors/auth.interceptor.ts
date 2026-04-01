import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';
import { AuthService } from '../services/auth.service';
import { environment } from '../../../environments/environment';

function isApiRequestUrl(url: string): boolean {
  const normalizedBase = String(environment.apiBaseUrl ?? '').trim().replace(/\/+$/, '');
  if (normalizedBase && (url === normalizedBase || url.startsWith(`${normalizedBase}/`))) {
    return true;
  }
  if (url === '/api' || url.startsWith('/api/')) {
    return true;
  }
  if (typeof window !== 'undefined') {
    try {
      const parsed = new URL(url, window.location.origin);
      return parsed.pathname === '/api' || parsed.pathname.startsWith('/api/');
    } catch {
      return false;
    }
  }
  return false;
}

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const token = auth.token();
  const isApiRequest = isApiRequestUrl(req.url);

  const request =
    token && isApiRequest
      ? req.clone({
          setHeaders: {
            Authorization: `Bearer ${token}`
          }
        })
      : req;

  return next(request).pipe(
    catchError((error: unknown) => {
      if (isApiRequest && error instanceof HttpErrorResponse && error.status === 401) {
        auth.logout();
        router.navigate(['/login'], {
          queryParams: { reason: 'session-expired' },
          replaceUrl: true
        });
      }
      return throwError(() => error);
    })
  );
};
