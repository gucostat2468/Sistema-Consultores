import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, OnChanges, Output, SimpleChanges, inject, signal } from '@angular/core';
import { FormBuilder, ReactiveFormsModule, Validators } from '@angular/forms';
import { finalize } from 'rxjs';
import { ClientHealth } from '../../../../core/models/dashboard.models';
import { PedidoService } from '../../services/pedido.service';

const MAX_PDF_BYTES = 10 * 1024 * 1024;
const EMAIL_REGEX = /^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$/;

@Component({
  selector: 'app-encaminhar-modal',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule],
  templateUrl: './encaminhar-modal.component.html',
  styleUrl: './encaminhar-modal.component.scss'
})
export class EncaminharModalComponent implements OnChanges {
  private readonly fb = inject(FormBuilder);
  private readonly pedidoService = inject(PedidoService);

  @Input({ required: true }) client!: ClientHealth;
  @Input() visible = false;
  @Output() closed = new EventEmitter<void>();
  @Output() forwarded = new EventEmitter<void>();

  readonly loading = signal(false);
  readonly extracting = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly successMessage = signal<string | null>(null);
  readonly selectedFile = signal<File | null>(null);

  readonly form = this.fb.group({
    orderNumber: ['', [Validators.maxLength(80)]],
    customerIdDoc: ['', [Validators.maxLength(80)]],
    customerName: ['', [Validators.required, Validators.maxLength(220)]],
    orderValue: [null as number | null, [Validators.required, Validators.min(0.01)]],
    recipientEmails: [''],
    attachClientAnalysis: [true]
  });

  ngOnChanges(changes: SimpleChanges): void {
    if ((changes['client'] || changes['visible']) && this.visible && this.client) {
      this.resetForm();
    }
    if (changes['visible'] && !this.visible) {
      this.errorMessage.set(null);
      this.successMessage.set(null);
      this.selectedFile.set(null);
    }
  }

  onFileSelected(event: Event): void {
    this.errorMessage.set(null);
    this.successMessage.set(null);
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0] ?? null;
    if (!file) {
      this.selectedFile.set(null);
      return;
    }
    const isPdf = file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf');
    if (!isPdf) {
      this.errorMessage.set('Selecione somente arquivo PDF.');
      input.value = '';
      this.selectedFile.set(null);
      return;
    }
    if (file.size > MAX_PDF_BYTES) {
      this.errorMessage.set('Arquivo excede 10MB.');
      input.value = '';
      this.selectedFile.set(null);
      return;
    }
    this.selectedFile.set(file);
  }

  extractFromPdf(): void {
    const file = this.selectedFile();
    if (!file || this.extracting()) {
      return;
    }
    this.extracting.set(true);
    this.errorMessage.set(null);
    this.successMessage.set(null);
    this.pedidoService
      .extractFromPdf(file)
      .pipe(finalize(() => this.extracting.set(false)))
      .subscribe({
        next: (response) => {
          const extracted = response.extracted;
          const extractedCustomerName = String(extracted.customerName ?? '').trim();
          const useExtractedCustomerName =
            extractedCustomerName.length > 0 &&
            !this.isLikelyPlaceholderCustomerName(extractedCustomerName);
          this.form.patchValue({
            orderNumber: extracted.orderNumber ?? this.form.controls.orderNumber.value,
            customerIdDoc: extracted.customerIdDoc ?? this.form.controls.customerIdDoc.value,
            customerName: useExtractedCustomerName
              ? extractedCustomerName
              : this.form.controls.customerName.value,
            orderValue: extracted.orderValue ?? this.form.controls.orderValue.value
          });
          if (extractedCustomerName && !useExtractedCustomerName) {
            this.errorMessage.set(
              'Nome extraído do PDF parece genérico. Mantivemos o nome do cliente selecionado para evitar divergência.'
            );
          }
          this.successMessage.set('Campos extraídos do PDF. Revise antes de encaminhar.');
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao extrair dados do PDF.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  submit(): void {
    this.errorMessage.set(null);
    this.successMessage.set(null);
    if (this.loading()) {
      return;
    }
    if (this.form.invalid) {
      this.form.markAllAsTouched();
      const orderValueControl = this.form.controls.orderValue;
      if (orderValueControl.hasError('required') || orderValueControl.hasError('min')) {
        this.errorMessage.set('Informe valor do pedido maior que zero ou use "Extrair campos do PDF".');
      } else {
        this.errorMessage.set('Revise os campos obrigatorios antes de encaminhar.');
      }
      return;
    }
    const file = this.selectedFile();
    if (!file) {
      this.errorMessage.set('Selecione o PDF do pedido antes de encaminhar.');
      return;
    }

    const payload = this.form.getRawValue();
    const recipientEmailsRaw = String(payload.recipientEmails ?? '').trim();
    const recipientEmails = this.normalizeRecipientEmails(recipientEmailsRaw);
    const routeByEmail = recipientEmails.length > 0;
    if (recipientEmailsRaw.length > 0 && !this.areAllEmailsValid(recipientEmails)) {
      this.errorMessage.set('Revise os e-mails informados. Use formato nome@dominio.com.');
      return;
    }

    const orderValue = Number(payload.orderValue ?? 0);
    this.loading.set(true);
    this.pedidoService
      .forwardOrder({
        file,
        consultantId: this.client.consultantId,
        customerCode: this.client.customerCode || null,
        customerName: String(payload.customerName ?? '').trim(),
        lookupCustomerName: this.client.customerName,
        orderValue,
        orderNumber: String(payload.orderNumber ?? '').trim() || null,
        customerIdDoc: String(payload.customerIdDoc ?? '').trim() || null,
        routeByEmail,
        recipientEmails: routeByEmail ? recipientEmails.join(', ') : null,
        attachClientAnalysis: payload.attachClientAnalysis !== false
      })
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          const warningText =
            response.warnings && response.warnings.length > 0
              ? ` Atenção: ${response.warnings.join(' | ')}`
              : '';
          if (response.credit.approved) {
            this.successMessage.set(`Pedido encaminhado e aguardando assinatura da Isabel.${warningText}`);
          } else {
            this.successMessage.set(
              `Pedido registrado como NEGADO_SEM_LIMITE e notificado para revisão com Isabel.${warningText}`
            );
          }
          this.forwarded.emit();
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao encaminhar pedido.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  close(): void {
    this.closed.emit();
  }

  private resetForm(): void {
    this.form.reset({
      orderNumber: '',
      customerIdDoc: '',
      customerName: this.client.customerName,
      orderValue: null,
      recipientEmails: '',
      attachClientAnalysis: true
    });
    this.selectedFile.set(null);
    this.errorMessage.set(null);
    this.successMessage.set(null);
  }

  orderValueError(): string | null {
    const control = this.form.controls.orderValue;
    if (!control || !(control.touched || control.dirty)) {
      return null;
    }
    if (control.hasError('required')) {
      return 'Informe o valor do pedido.';
    }
    if (control.hasError('min')) {
      return 'O valor precisa ser maior que zero.';
    }
    return null;
  }

  recipientEmailsError(): string | null {
    const raw = String(this.form.controls.recipientEmails.value ?? '').trim();
    if (!raw) {
      return null;
    }
    const recipients = this.normalizeRecipientEmails(raw);
    if (!recipients.length) {
      return null;
    }
    if (!this.areAllEmailsValid(recipients)) {
      return 'Existe e-mail invalido na lista.';
    }
    return null;
  }

  private normalizeRecipientEmails(raw: string): string[] {
    return raw
      .split(/[;,]/)
      .map((item) => item.trim().toLowerCase())
      .filter(Boolean);
  }

  private areAllEmailsValid(items: string[]): boolean {
    return items.every((item) => EMAIL_REGEX.test(item));
  }

  private isLikelyPlaceholderCustomerName(value: string): boolean {
    const normalized = String(value ?? '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toUpperCase()
      .replace(/[^A-Z0-9 ]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const placeholders = new Set([
      'ENDERECO',
      'ENDERECO DO CLIENTE',
      'CLIENTE',
      'NOME',
      'NOME DO CLIENTE',
      'RAZAO SOCIAL',
      'RAZAO SOCIAL DO CLIENTE'
    ]);
    return placeholders.has(normalized) || normalized.startsWith('ENDERECO ');
  }
}
