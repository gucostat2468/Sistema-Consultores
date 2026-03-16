import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, catchError, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';
import {
  AdminClearDataResponse,
  AdminImportRequest,
  AdminImportResponse,
  IngestionHistoryItem
} from '../models/admin-import.models';
import { AuthService } from './auth.service';

@Injectable({ providedIn: 'root' })
export class AdminImportService {
  private readonly http = inject(HttpClient);
  private readonly auth = inject(AuthService);

  execute(payload: AdminImportRequest): Observable<AdminImportResponse> {
    if (!this.isAdmUser()) {
      return throwError(() => new Error('Acesso negado. Somente Marcos e Isabel podem importar arquivos.'));
    }

    if (!payload.pdfFile && !payload.excelFile) {
      return throwError(() => new Error('Selecione ao menos um arquivo: PDF ou Excel.'));
    }

    if (environment.useMockApi) {
      return throwError(
        () =>
          new Error(
            'Modo simulado ativo (useMockApi=true). A importacao nao e real. Defina useMockApi=false e conecte a API para leitura real de PDF/Excel.'
          )
      );
    }

    const formData = new FormData();
    formData.append('mode', payload.mode);
    formData.append('strictVendors', String(payload.strictVendors));
    formData.append('appendMode', String(payload.appendMode));
    formData.append('allowSkippedLines', String(payload.allowSkippedLines));
    formData.append('allowSkippedCreditRows', String(payload.allowSkippedCreditRows));
    formData.append('inputProfile', payload.inputProfile ?? 'auto');
    formData.append('actorUsername', this.auth.currentUser()?.username ?? 'adm');

    if (payload.pdfFile) {
      formData.append('pdf', payload.pdfFile, payload.pdfFile.name);
    }
    if (payload.excelFile) {
      formData.append('excel', payload.excelFile, payload.excelFile.name);
    }

    return this.http
      .post<AdminImportResponse>(`${environment.apiBaseUrl}/admin/import`, formData)
      .pipe(catchError((error) => throwError(() => this.mapHttpError(error, 'import'))));
  }

  executeReportV1(excelFile: File, replaceBase = false): Observable<AdminImportResponse> {
    if (!this.isAdmUser()) {
      return throwError(() => new Error('Acesso negado. Somente Marcos e Isabel podem importar arquivos.'));
    }

    if (environment.useMockApi) {
      return throwError(
        () =>
          new Error(
            'Modo simulado ativo (useMockApi=true). A importacao nao e real. Defina useMockApi=false e conecte a API para leitura real de PDF/Excel.'
          )
      );
    }

    const formData = new FormData();
    formData.append('excel', excelFile, excelFile.name);
    formData.append('actorUsername', this.auth.currentUser()?.username ?? 'adm');
    formData.append('replaceBase', String(replaceBase));

    return this.http
      .post<AdminImportResponse>(`${environment.apiBaseUrl}/admin/import-report-v1`, formData)
      .pipe(
        catchError((error) => {
          // Compatibilidade com backends que ainda não expõem /admin/import-report-v1.
          if (error instanceof HttpErrorResponse && error.status === 404) {
            const fallback = new FormData();
            fallback.append('mode', 'update');
            fallback.append('strictVendors', 'false');
            fallback.append('appendMode', String(!replaceBase));
            fallback.append('allowSkippedLines', '0');
            fallback.append('allowSkippedCreditRows', '0');
            fallback.append('inputProfile', 'auto');
            fallback.append('actorUsername', this.auth.currentUser()?.username ?? 'adm');
            fallback.append('excel', excelFile, excelFile.name);

            return this.http
              .post<AdminImportResponse>(`${environment.apiBaseUrl}/admin/import`, fallback)
              .pipe(catchError((fallbackError) => throwError(() => this.mapHttpError(fallbackError, 'report'))));
          }
          return throwError(() => this.mapHttpError(error, 'report'));
        })
      );
  }

  clearData(removeConsultants = true): Observable<AdminClearDataResponse> {
    if (!this.isAdmUser()) {
      return throwError(() => new Error('Acesso negado. Somente Marcos e Isabel podem limpar dados.'));
    }

    if (environment.useMockApi) {
      return throwError(
        () =>
          new Error(
            'Modo simulado ativo (useMockApi=true). A limpeza nao e real. Defina useMockApi=false para operar no backend.'
          )
      );
    }

    const formData = new FormData();
    formData.append('actorUsername', this.auth.currentUser()?.username ?? 'adm');
    formData.append('removeConsultants', String(removeConsultants));

    return this.http
      .post<AdminClearDataResponse>(`${environment.apiBaseUrl}/admin/clear-data`, formData)
      .pipe(catchError((error) => throwError(() => this.mapHttpError(error, 'clear'))));
  }

  listHistory(limit = 60): Observable<{ items: IngestionHistoryItem[] }> {
    const user = this.auth.currentUser();
    if (!this.isOperationalUser(user?.username ?? null)) {
      return throwError(() => new Error('Acesso negado. Somente Marcos e Isabel podem ver o historico.'));
    }

    if (environment.useMockApi) {
      return throwError(
        () =>
          new Error(
            'Modo simulado ativo (useMockApi=true). O historico de importacao nao e real. Defina useMockApi=false para consultar no backend.'
          )
      );
    }

    const params = new HttpParams().set('limit', String(limit));
    return this.http
      .get<{ items: IngestionHistoryItem[] }>(`${environment.apiBaseUrl}/admin/ingestion/history`, {
        params
      })
      .pipe(catchError((error) => throwError(() => this.mapHttpError(error, 'history'))));
  }

  isAdmUser(): boolean {
    const user = this.auth.currentUser();
    return this.isOperationalUser(user?.username ?? null);
  }

  private isOperationalUser(username: string | null): boolean {
    const allowed = (
      (environment as { operationalUsernames?: string[] }).operationalUsernames ?? [
        'isabel',
        'isabel_dronepro',
        'marcos',
        'marcos_dronepro'
      ]
    )
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
    return allowed.includes(String(username ?? '').toLowerCase());
  }

  private mapHttpError(error: unknown, context: 'import' | 'report' | 'clear' | 'history'): Error {
    if (!(error instanceof HttpErrorResponse)) {
      return error instanceof Error ? error : new Error('Erro inesperado ao comunicar com a API.');
    }

    if (error.status === 0) {
      return new Error(
        'Nao foi possivel conectar ao backend. Verifique se a API FastAPI esta em execucao e acessivel.'
      );
    }

    const backendMessage = this.extractBackendMessage(error);
    if (backendMessage) {
      return new Error(backendMessage);
    }

    if (error.status === 404) {
      if (context === 'report') {
        return new Error(
          'Endpoint de importacao report nao encontrado na API atual. A instancia em localhost:8000 parece estar desatualizada. Reinicie com: python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload'
        );
      }
      if (context === 'history') {
        return new Error(
          'Endpoint de historico de importacao nao encontrado na API atual. Reinicie a API local para carregar a versao mais recente.'
        );
      }
      return new Error(
        'Endpoint administrativo nao encontrado na API atual. Confirme se a instancia correta do backend esta rodando.'
      );
    }

    if (error.status === 401 || error.status === 403) {
      return new Error('Sessao sem permissao para esta operacao. Entre novamente com Marcos ou Isabel.');
    }

    return new Error(`Falha na API (${error.status}).`);
  }

  private extractBackendMessage(error: HttpErrorResponse): string | null {
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
