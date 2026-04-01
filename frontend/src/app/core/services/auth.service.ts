import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Injectable, computed, inject, signal } from '@angular/core';
import { Observable, catchError, delay, of, tap, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';
import { AuthSession, LoginPayload, SessionUser } from '../models/auth.models';
import { buildMockSession, getMockUserByCredentials } from './mock-data';

const SESSION_KEY = 'consultores.session';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly http = inject(HttpClient);
  private readonly state = signal<AuthSession | null>(this.readStorage());

  readonly session = computed(() => this.state());
  readonly isAuthenticated = computed(() => !!this.state());
  readonly currentUser = computed<SessionUser | null>(() => this.state()?.user ?? null);
  readonly token = computed<string | null>(() => this.state()?.accessToken ?? null);

  login(payload: LoginPayload): Observable<AuthSession> {
    if (environment.useMockApi) {
      return this.loginMock(payload);
    }

    const normalizedPayload: LoginPayload = {
      username: payload.username.trim(),
      password: payload.password.trim()
    };

    return this.http
      .post<AuthSession>(`${environment.apiBaseUrl}/auth/login`, normalizedPayload)
      .pipe(
        tap((session) => this.setSession(session)),
        catchError((error) => throwError(() => this.mapLoginError(error)))
      );
  }

  logout(): void {
    this.state.set(null);
    localStorage.removeItem(SESSION_KEY);
  }

  private loginMock(payload: LoginPayload): Observable<AuthSession> {
    const user = getMockUserByCredentials(payload.username.trim(), payload.password.trim());
    if (!user) {
      return throwError(() => new Error('Usuario ou senha invalidos.'));
    }
    return of(buildMockSession(user)).pipe(
      delay(220),
      tap((session) => this.setSession(session))
    );
  }

  private setSession(session: AuthSession): void {
    this.state.set(session);
    localStorage.setItem(SESSION_KEY, JSON.stringify(session));
  }

  private readStorage(): AuthSession | null {
    try {
      const raw = localStorage.getItem(SESSION_KEY);
      if (!raw) {
        return null;
      }
      const parsed = JSON.parse(raw) as AuthSession;
      if (!parsed?.user?.username || !parsed.accessToken) {
        localStorage.removeItem(SESSION_KEY);
        return null;
      }
      if (!environment.useMockApi && !this.isStoredJwtTokenValid(parsed.accessToken)) {
        localStorage.removeItem(SESSION_KEY);
        return null;
      }
      return parsed;
    } catch {
      localStorage.removeItem(SESSION_KEY);
      return null;
    }
  }

  private isStoredJwtTokenValid(token: string): boolean {
    const payload = this.decodeJwtPayload(token);
    const exp = payload?.['exp'];
    if (typeof exp !== 'number' || !Number.isFinite(exp)) {
      return false;
    }
    return exp * 1000 > Date.now();
  }

  private decodeJwtPayload(token: string): Record<string, unknown> | null {
    const parts = String(token || '').split('.');
    if (parts.length !== 3) {
      return null;
    }
    const encodedPayload = parts[1];
    if (!encodedPayload || typeof window === 'undefined' || typeof window.atob !== 'function') {
      return null;
    }
    try {
      let normalized = encodedPayload.replace(/-/g, '+').replace(/_/g, '/');
      const remainder = normalized.length % 4;
      if (remainder > 0) {
        normalized += '='.repeat(4 - remainder);
      }
      const decoded = window.atob(normalized);
      const payload = JSON.parse(decoded) as Record<string, unknown>;
      return payload;
    } catch {
      return null;
    }
  }

  private mapLoginError(error: unknown): Error {
    if (!(error instanceof HttpErrorResponse)) {
      return error instanceof Error ? error : new Error('Falha no login. Tente novamente.');
    }

    if (error.status === 0) {
      return new Error('Nao foi possivel conectar ao backend em http://localhost:8000.');
    }
    if (error.status === 401) {
      return new Error('Usuario ou senha invalidos.');
    }

    const detail = this.extractBackendDetail(error);
    if (detail) {
      return new Error(detail);
    }
    return new Error(`Falha no login (${error.status}).`);
  }

  private extractBackendDetail(error: HttpErrorResponse): string | null {
    if (!error.error) {
      return null;
    }
    if (typeof error.error === 'string') {
      try {
        const parsed = JSON.parse(error.error) as { detail?: string };
        if (parsed?.detail) {
          return parsed.detail;
        }
      } catch {
        return error.error.trim() || null;
      }
    }
    if (typeof error.error === 'object' && 'detail' in error.error) {
      const detail = (error.error as { detail?: unknown }).detail;
      if (typeof detail === 'string' && detail.trim().length > 0) {
        return detail.trim();
      }
    }
    return null;
  }
}
