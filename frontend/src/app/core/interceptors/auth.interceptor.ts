import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';
import { AuthService } from '../services/auth.service';
import { environment } from '../../../environments/environment';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const auth = inject(AuthService);
  const router = inject(Router);
  const token = auth.token();
  const isApiRequest = req.url.startsWith(environment.apiBaseUrl);

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
        router.navigateByUrl('/login');
      }
      return throwError(() => error);
    })
  );
};
