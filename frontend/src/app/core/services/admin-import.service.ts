import { HttpClient } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable, throwError } from 'rxjs';
import { environment } from '../../../environments/environment';
import { AdminImportRequest, AdminImportResponse } from '../models/admin-import.models';
import { AuthService } from './auth.service';

@Injectable({ providedIn: 'root' })
export class AdminImportService {
  private readonly http = inject(HttpClient);
  private readonly auth = inject(AuthService);

  execute(payload: AdminImportRequest): Observable<AdminImportResponse> {
    if (!this.isAdmUser()) {
      return throwError(() => new Error('Acesso negado. Somente o usuário adm pode importar arquivos.'));
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
    formData.append('actorUsername', 'adm');

    if (payload.pdfFile) {
      formData.append('pdf', payload.pdfFile, payload.pdfFile.name);
    }
    if (payload.excelFile) {
      formData.append('excel', payload.excelFile, payload.excelFile.name);
    }

    return this.http.post<AdminImportResponse>(`${environment.apiBaseUrl}/admin/import`, formData);
  }

  executeReportV1(excelFile: File): Observable<AdminImportResponse> {
    if (!this.isAdmUser()) {
      return throwError(() => new Error('Acesso negado. Somente o usuário adm pode importar arquivos.'));
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
    formData.append('actorUsername', 'adm');

    return this.http.post<AdminImportResponse>(
      `${environment.apiBaseUrl}/admin/import-report-v1`,
      formData
    );
  }

  isAdmUser(): boolean {
    const user = this.auth.currentUser();
    return user?.role === 'admin' && user.username.toLowerCase() === 'adm';
  }
}
