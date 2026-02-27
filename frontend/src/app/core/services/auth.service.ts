import { HttpClient } from '@angular/common/http';
import { Injectable, computed, inject, signal } from '@angular/core';
import { Observable, delay, of, tap, throwError } from 'rxjs';
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

    return this.http
      .post<AuthSession>(`${environment.apiBaseUrl}/auth/login`, payload)
      .pipe(tap((session) => this.setSession(session)));
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
        return null;
      }
      return parsed;
    } catch {
      return null;
    }
  }
}
