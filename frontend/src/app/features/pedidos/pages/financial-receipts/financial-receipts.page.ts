import { CommonModule } from '@angular/common';
import { Component, DestroyRef, OnDestroy, computed, inject, signal } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { finalize } from 'rxjs/operators';
import { ApprovalOrderItem, OrderStatusDetail, PedidoService } from '../../services/pedido.service';

const LIVE_REFRESH_INTERVAL_MS = 2500;

@Component({
  selector: 'app-financial-receipts-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './financial-receipts.page.html',
  styleUrl: './financial-receipts.page.scss'
})
export class FinancialReceiptsPage implements OnDestroy {
  private readonly pedidoService = inject(PedidoService);
  private readonly sanitizer = inject(DomSanitizer);
  private readonly destroyRef = inject(DestroyRef);

  readonly loading = signal(true);
  readonly actionLoading = signal(false);
  readonly previewLoading = signal(false);
  readonly analysisPreviewLoading = signal(false);
  readonly auditLoading = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly toastMessage = signal<string | null>(null);
  readonly items = signal<ApprovalOrderItem[]>([]);
  readonly selectedOrder = signal<ApprovalOrderItem | null>(null);
  readonly decisionModalOpen = signal(false);
  readonly previewTab = signal<'pedido' | 'analise'>('pedido');
  readonly previewUrl = signal<SafeResourceUrl | null>(null);
  readonly analysisPreviewUrl = signal<SafeResourceUrl | null>(null);
  readonly decisionEvents = signal<OrderStatusDetail['events']>([]);

  readonly customerFilter = signal('');
  readonly dateFrom = signal('');
  readonly dateTo = signal('');

  readonly signedToday = computed(() => {
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = today.getMonth();
    const dd = today.getDate();
    return this.items().filter((item) => {
      const raw = String(item.signedAt || '').trim();
      if (!raw) {
        return false;
      }
      const stamp = new Date(raw);
      return (
        stamp.getFullYear() === yyyy &&
        stamp.getMonth() === mm &&
        stamp.getDate() === dd
      );
    }).length;
  });

  private previewObjectUrl: string | null = null;
  private analysisPreviewObjectUrl: string | null = null;
  private liveRefreshHandle: ReturnType<typeof setInterval> | null = null;
  private liveRefreshBusy = false;

  constructor() {
    this.load();
    this.startLiveRefresh();
  }

  ngOnDestroy(): void {
    this.stopLiveRefresh();
    this.revokePreview();
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

  openDetails(order: ApprovalOrderItem): void {
    this.selectedOrder.set(order);
    this.decisionModalOpen.set(true);
    this.previewTab.set('pedido');
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.decisionEvents.set([]);
    this.loadPreview(order.id);
    this.loadAnalysisPreview(order.id);
    this.loadOrderAudit(order.id);
  }

  closeDetails(): void {
    this.decisionModalOpen.set(false);
    this.selectedOrder.set(null);
    this.decisionEvents.set([]);
    this.revokePreview();
  }

  setPreviewTab(tab: 'pedido' | 'analise'): void {
    this.previewTab.set(tab);
  }

  download(order: ApprovalOrderItem): void {
    if (this.actionLoading()) {
      return;
    }
    this.actionLoading.set(true);
    this.pedidoService
      .downloadOrderPdf(order.id)
      .pipe(finalize(() => this.actionLoading.set(false)))
      .subscribe({
        next: (blob) => {
          this.downloadBlob(blob, `${order.orderNumber}-assinado.pdf`);
          this.toastMessage.set(`PDF do pedido ${order.orderNumber} salvo com sucesso.`);
        },
        error: () => {
          this.errorMessage.set('Nao foi possivel salvar o PDF assinado do pedido.');
        }
      });
  }

  print(order: ApprovalOrderItem): void {
    if (this.actionLoading()) {
      return;
    }
    this.actionLoading.set(true);
    this.pedidoService
      .downloadOrderPdf(order.id)
      .pipe(finalize(() => this.actionLoading.set(false)))
      .subscribe({
        next: (blob) => {
          this.printBlob(blob, `${order.orderNumber}-assinado.pdf`);
        },
        error: () => {
          this.errorMessage.set('Nao foi possivel carregar o PDF para impressao.');
        }
      });
  }

  downloadAnalysis(order: ApprovalOrderItem): void {
    this.pedidoService.downloadOrderAnalysisPdf(order.id).subscribe({
      next: (blob) => {
        this.downloadBlob(blob, `${order.orderNumber}-analise-cliente.pdf`);
      },
      error: () => {
        this.errorMessage.set('Nao foi possivel salvar o PDF de analise deste pedido.');
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
        limit: 1000
      })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          this.items.set(response.items);
          this.syncSelectedOrderFromLiveData(response.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar histórico de concluídos.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  private startLiveRefresh(): void {
    this.stopLiveRefresh();
    this.liveRefreshHandle = setInterval(() => this.refreshLiveData(), LIVE_REFRESH_INTERVAL_MS);
  }

  private stopLiveRefresh(): void {
    if (this.liveRefreshHandle) {
      clearInterval(this.liveRefreshHandle);
      this.liveRefreshHandle = null;
    }
  }

  private refreshLiveData(): void {
    if (this.liveRefreshBusy || this.loading() || this.actionLoading()) {
      return;
    }
    if (typeof document !== 'undefined' && document.hidden) {
      return;
    }

    this.liveRefreshBusy = true;
    this.pedidoService
      .listFinancialReceipts({
        customer: this.customerFilter().trim(),
        dateFrom: this.dateFrom(),
        dateTo: this.dateTo(),
        limit: 1000
      })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => (this.liveRefreshBusy = false)))
      .subscribe({
        next: (response) => {
          this.items.set(response.items);
          this.syncSelectedOrderFromLiveData(response.items);
        },
        error: (error: { status?: number }) => {
          if ((error?.status ?? 0) === 401) {
            this.stopLiveRefresh();
            this.errorMessage.set('Sessão expirada. Faça login novamente para continuar.');
          }
        }
      });
  }

  private syncSelectedOrderFromLiveData(items: ApprovalOrderItem[]): void {
    const selected = this.selectedOrder();
    if (!selected) {
      return;
    }
    const updated = items.find((item) => item.id === selected.id);
    if (!updated) {
      this.closeDetails();
      this.toastMessage.set(`Pedido ${selected.orderNumber} não está mais na lista de concluídos.`);
      return;
    }
    this.selectedOrder.set(updated);
  }

  private loadPreview(orderId: number): void {
    this.previewLoading.set(true);
    if (this.previewObjectUrl) {
      URL.revokeObjectURL(this.previewObjectUrl);
      this.previewObjectUrl = null;
    }
    this.previewUrl.set(null);
    this.pedidoService
      .downloadOrderPdf(orderId)
      .pipe(finalize(() => this.previewLoading.set(false)))
      .subscribe({
        next: (blob) => {
          this.previewObjectUrl = URL.createObjectURL(blob);
          this.previewUrl.set(this.sanitizer.bypassSecurityTrustResourceUrl(this.previewObjectUrl));
        },
        error: () => {
          this.previewUrl.set(null);
        }
      });
  }

  private loadAnalysisPreview(orderId: number): void {
    this.analysisPreviewLoading.set(true);
    if (this.analysisPreviewObjectUrl) {
      URL.revokeObjectURL(this.analysisPreviewObjectUrl);
      this.analysisPreviewObjectUrl = null;
    }
    this.analysisPreviewUrl.set(null);
    this.pedidoService
      .downloadOrderAnalysisPdf(orderId)
      .pipe(finalize(() => this.analysisPreviewLoading.set(false)))
      .subscribe({
        next: (blob) => {
          this.analysisPreviewObjectUrl = URL.createObjectURL(blob);
          this.analysisPreviewUrl.set(this.sanitizer.bypassSecurityTrustResourceUrl(this.analysisPreviewObjectUrl));
        },
        error: () => {
          this.analysisPreviewUrl.set(null);
        }
      });
  }

  private loadOrderAudit(orderId: number): void {
    this.auditLoading.set(true);
    this.decisionEvents.set([]);
    this.pedidoService
      .getOrderStatus(orderId)
      .pipe(finalize(() => this.auditLoading.set(false)))
      .subscribe({
        next: (response) => {
          this.decisionEvents.set(response.events || []);
        },
        error: () => {
          this.decisionEvents.set([]);
        }
      });
  }

  private revokePreview(): void {
    if (this.previewObjectUrl) {
      URL.revokeObjectURL(this.previewObjectUrl);
      this.previewObjectUrl = null;
    }
    if (this.analysisPreviewObjectUrl) {
      URL.revokeObjectURL(this.analysisPreviewObjectUrl);
      this.analysisPreviewObjectUrl = null;
    }
    this.previewUrl.set(null);
    this.analysisPreviewUrl.set(null);
  }

  private downloadBlob(blob: Blob, filename: string): void {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  private printBlob(blob: Blob, fallbackFilename: string): void {
    const url = URL.createObjectURL(blob);
    const printWindow = window.open(url, '_blank');
    if (!printWindow) {
      URL.revokeObjectURL(url);
      this.downloadBlob(blob, fallbackFilename);
      this.toastMessage.set(
        'Navegador bloqueou a janela de impressão. PDF salvo para impressão manual.'
      );
      return;
    }

    const triggerPrint = () => {
      try {
        printWindow.focus();
        printWindow.print();
        this.toastMessage.set('PDF aberto para impressão.');
      } catch {
        this.downloadBlob(blob, fallbackFilename);
      }
    };

    const fallbackTimer = setTimeout(triggerPrint, 700);
    printWindow.addEventListener(
      'load',
      () => {
        clearTimeout(fallbackTimer);
        triggerPrint();
      },
      { once: true }
    );
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  }
}

