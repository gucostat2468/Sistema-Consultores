import { CommonModule } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { finalize } from 'rxjs';
import { AdminImportResponse } from '../../../../core/models/admin-import.models';
import { AdminImportService } from '../../../../core/services/admin-import.service';

@Component({
  selector: 'app-report-import-page',
  imports: [CommonModule],
  templateUrl: './report-import.page.html',
  styleUrl: './report-import.page.scss'
})
export class ReportImportPage {
  private readonly importService = inject(AdminImportService);

  readonly loading = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly response = signal<AdminImportResponse | null>(null);
  readonly excelFile = signal<File | null>(null);

  get isAdmUser(): boolean {
    return this.importService.isAdmUser();
  }

  setReportFile(event: Event): void {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0] ?? null;
    if (!file) {
      return;
    }

    this.errorMessage.set(null);
    const name = file.name.toLowerCase();
    if (!name.endsWith('.xls') && !name.endsWith('.xlsx') && !name.endsWith('.xlsm')) {
      this.errorMessage.set('Use apenas planilha no padrao report (.xls, .xlsx ou .xlsm).');
      input.value = '';
      return;
    }

    this.excelFile.set(file);
    input.value = '';
  }

  clearFile(): void {
    this.excelFile.set(null);
  }

  execute(): void {
    if (!this.isAdmUser) {
      this.errorMessage.set('Acesso negado. Somente o usuário adm pode importar arquivos.');
      return;
    }

    const file = this.excelFile();
    if (!file) {
      this.errorMessage.set('Selecione o arquivo report para atualizar.');
      return;
    }

    this.loading.set(true);
    this.errorMessage.set(null);
    this.response.set(null);

    this.importService
      .executeReportV1(file)
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => this.response.set(response),
        error: (error: Error) =>
          this.errorMessage.set(error.message || 'Falha ao processar o report padrao.')
      });
  }

  downloadAuditLog(): void {
    const result = this.response();
    if (!result?.auditLog?.length) {
      return;
    }
    const logContent = `${result.auditLog.join('\n')}\n`;
    const blob = new Blob([logContent], { type: 'text/plain;charset=utf-8' });
    const fileName = `auditoria-report-${result.operationId ?? Date.now()}.log`;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = fileName;
    anchor.click();
    URL.revokeObjectURL(url);
  }
}
