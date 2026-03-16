import { CommonModule } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { finalize } from 'rxjs';
import { AdminImportResponse, ImportMode } from '../../../../core/models/admin-import.models';
import { AdminImportService } from '../../../../core/services/admin-import.service';

@Component({
  selector: 'app-admin-import-page',
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './admin-import.page.html',
  styleUrl: './admin-import.page.scss'
})
export class AdminImportPage {
  private readonly fb = inject(FormBuilder);
  private readonly importService = inject(AdminImportService);

  readonly loading = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly response = signal<AdminImportResponse | null>(null);

  readonly pdfFile = signal<File | null>(null);
  readonly excelFile = signal<File | null>(null);

  readonly form = this.fb.nonNullable.group({
    mode: ['update' as ImportMode, [Validators.required]],
    strictVendors: [false],
    appendMode: [false],
    allowSkippedLines: [0, [Validators.min(0), Validators.max(100)]],
    allowSkippedCreditRows: [10, [Validators.min(0), Validators.max(1000)]]
  });

  get isAdmUser(): boolean {
    return this.importService.isAdmUser();
  }

  setUpdateFiles(event: Event): void {
    const input = event.target as HTMLInputElement;
    const files = input.files ? Array.from(input.files) : [];
    if (files.length === 0) {
      return;
    }

    this.errorMessage.set(null);

    let detectedPdf: File | null = this.pdfFile();
    let detectedSheet: File | null = this.excelFile();

    const invalidFiles: string[] = [];
    for (const file of files) {
      const lowerName = file.name.toLowerCase();
      if (this.isPdf(lowerName)) {
        detectedPdf = file;
        continue;
      }
      if (this.isSpreadsheet(lowerName)) {
        detectedSheet = file;
        continue;
      }
      invalidFiles.push(file.name);
    }

    this.pdfFile.set(detectedPdf);
    this.excelFile.set(detectedSheet);

    if (invalidFiles.length > 0) {
      this.errorMessage.set(
        `Formato não suportado: ${invalidFiles.join(', ')}. Use PDF, XLSX, XLS, XLSM ou CSV.`
      );
    }

    // Permite selecionar os mesmos arquivos novamente no próximo clique.
    input.value = '';
  }

  clearPdfFile(): void {
    this.pdfFile.set(null);
  }

  clearExcelFile(): void {
    this.excelFile.set(null);
  }

  execute(): void {
    if (!this.isAdmUser) {
      this.errorMessage.set('Acesso negado. Somente Marcos e Isabel podem importar arquivos.');
      return;
    }

    if (!this.pdfFile() && !this.excelFile()) {
      this.errorMessage.set('Selecione ao menos um arquivo: PDF ou Excel.');
      return;
    }

    if (this.form.invalid || this.loading()) {
      this.form.markAllAsTouched();
      return;
    }

    this.loading.set(true);
    this.errorMessage.set(null);
    this.response.set(null);

    const values = this.form.getRawValue();
    this.importService
      .execute({
        pdfFile: this.pdfFile(),
        excelFile: this.excelFile(),
        mode: values.mode,
        strictVendors: values.strictVendors,
        appendMode: values.appendMode,
        allowSkippedLines: values.allowSkippedLines,
        allowSkippedCreditRows: values.allowSkippedCreditRows
      })
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          this.response.set(response);
          if (!response.success) {
            const firstWarning = response.warnings?.[0];
            this.errorMessage.set(firstWarning ? `${response.message} ${firstWarning}` : response.message);
          }
        },
        error: (error: Error) =>
          this.errorMessage.set(error.message || 'Falha ao processar atualização.')
      });
  }

  downloadAuditLog(): void {
    const result = this.response();
    if (!result?.auditLog?.length) {
      return;
    }

    const logContent = `${result.auditLog.join('\n')}\n`;
    const blob = new Blob([logContent], { type: 'text/plain;charset=utf-8' });
    const fileName = `auditoria-importacao-${result.operationId ?? Date.now()}.log`;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = fileName;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  getAuditOutcomeMessage(audit: NonNullable<AdminImportResponse['audit']>): string {
    const newCustomers = audit.newCustomers ?? 0;
    const newTitles = audit.newTitles ?? 0;
    const updatedTitles = audit.updatedTitles ?? 0;
    const updatedRecords = audit.updatedRecords ?? 0;

    if (newCustomers > 0 || newTitles > 0) {
      return `Novos adicionados: ${newCustomers} clientes e ${newTitles} titulos.`;
    }

    if (audit.onlyUpdates || updatedRecords > 0) {
      return `Somente atualizacao: nenhum cliente/titulo novo. Titulos atualizados: ${updatedTitles}.`;
    }

    return 'Nenhuma alteracao aplicada nesta importacao.';
  }

  isOnlyUpdateOutcome(audit: NonNullable<AdminImportResponse['audit']>): boolean {
    const newCustomers = audit.newCustomers ?? 0;
    const newTitles = audit.newTitles ?? 0;
    if (newCustomers > 0 || newTitles > 0) {
      return false;
    }
    return !!audit.onlyUpdates || (audit.updatedRecords ?? 0) > 0;
  }

  private isPdf(fileName: string): boolean {
    return fileName.endsWith('.pdf');
  }

  private isSpreadsheet(fileName: string): boolean {
    return (
      fileName.endsWith('.xlsx') ||
      fileName.endsWith('.xls') ||
      fileName.endsWith('.xlsm') ||
      fileName.endsWith('.csv')
    );
  }
}
