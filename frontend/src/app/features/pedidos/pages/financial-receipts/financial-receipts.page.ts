import { CommonModule } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { finalize } from 'rxjs/operators';
import { ApprovalOrderItem, PedidoService } from '../../services/pedido.service';

@Component({
  selector: 'app-financial-receipts-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './financial-receipts.page.html',
  styleUrl: './financial-receipts.page.scss'
})
export class FinancialReceiptsPage {
  private readonly pedidoService = inject(PedidoService);

  readonly loading = signal(true);
  readonly errorMessage = signal<string | null>(null);
  readonly items = signal<ApprovalOrderItem[]>([]);

  readonly customerFilter = signal('');
  readonly dateFrom = signal('');
  readonly dateTo = signal('');

  constructor() {
    this.load();
  }

  applyFilters(): void {
    this.load();
  }

  clearFilters(): void {
    this.customerFilter.set('');
    this.dateFrom.set('');
    this.dateTo.set('');
    this.load();
  }

  download(order: ApprovalOrderItem): void {
    this.pedidoService.downloadOrderPdf(order.id).subscribe({
      next: (blob) => {
        this.downloadBlob(blob, `${order.orderNumber}-assinado.pdf`);
      },
      error: () => {
        this.errorMessage.set('Nao foi possivel baixar o comprovante assinado.');
      }
    });
  }

  signatureResponsibleLabel(order: ApprovalOrderItem): string {
    const approvals = Array.isArray(order.distribution?.approvals) ? order.distribution.approvals : [];
    const names: string[] = [];
    const seen = new Set<string>();
    for (const approval of approvals) {
      const name = String(approval.signedByName || '').trim();
      const normalized = name.toLowerCase();
      if (name && !seen.has(normalized)) {
        seen.add(normalized);
        names.push(name);
      }
    }
    if (names.length >= 2) {
      return names.join('/');
    }
    if (names.length === 1) {
      return names[0];
    }
    return order.signedByName || '-';
  }

  statusLabel(status: ApprovalOrderItem['status'] | string): string {
    if (status === 'CONCLUIDO' || status === 'FATURADO') {
      return 'Concluído';
    }
    if (status === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL') {
      return 'Aguardando Diretor Comercial';
    }
    if (status === 'AGUARDANDO_ASSINATURA_ISABEL') {
      return 'Aguardando Isabel';
    }
    if (status === 'NEGADO_SEM_LIMITE') {
      return 'Negado sem limite';
    }
    if (status === 'DEVOLVIDO_REVISAO') {
      return 'Devolvido para revisão';
    }
    return String(status);
  }

  private load(): void {
    this.loading.set(true);
    this.errorMessage.set(null);
    this.pedidoService
      .listFinancialReceipts({
        customer: this.customerFilter().trim(),
        dateFrom: this.dateFrom(),
        dateTo: this.dateTo(),
        limit: 500
      })
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          this.items.set(response.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar comprovantes financeiros.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  private downloadBlob(blob: Blob, filename: string): void {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
  }
}
