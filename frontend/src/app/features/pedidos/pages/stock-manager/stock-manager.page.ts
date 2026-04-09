import { CommonModule } from '@angular/common';
import { Component, DestroyRef, ElementRef, OnDestroy, ViewChild, computed, inject, signal } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { FormBuilder, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { finalize } from 'rxjs/operators';
import { environment } from '../../../../../environments/environment';
import { AuthService } from '../../../../core/services/auth.service';
import { ApprovalOrderItem, ApprovalOrderStatus, OrderStatusDetail, PedidoService } from '../../services/pedido.service';

const LIVE_REFRESH_INTERVAL_MS = 2500;
const ELAPSED_TICK_MS = 1000;

@Component({
  selector: 'app-stock-manager-page',
  standalone: true,
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './stock-manager.page.html',
  styleUrl: './stock-manager.page.scss'
})
export class StockManagerPage implements OnDestroy {
  private readonly pedidoService = inject(PedidoService);
  private readonly fb = inject(FormBuilder);
  private readonly sanitizer = inject(DomSanitizer);
  private readonly destroyRef = inject(DestroyRef);
  private readonly auth = inject(AuthService);

  @ViewChild('signaturePad') signaturePad?: ElementRef<HTMLCanvasElement>;
  readonly user = this.auth.currentUser;

  readonly loading = signal(true);
  readonly actionLoading = signal(false);
  readonly previewLoading = signal(false);
  readonly analysisPreviewLoading = signal(false);
  readonly auditLoading = signal(false);
  readonly manualSigning = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly toastMessage = signal<string | null>(null);
  readonly items = signal<ApprovalOrderItem[]>([]);
  readonly selectedOrder = signal<ApprovalOrderItem | null>(null);
  readonly decisionModalOpen = signal(false);
  readonly hasSignature = signal(false);
  readonly hasSavedSignature = signal(false);
  readonly previewUrl = signal<SafeResourceUrl | null>(null);
  readonly analysisPreviewUrl = signal<SafeResourceUrl | null>(null);
  readonly decisionEvents = signal<OrderStatusDetail['events']>([]);
  readonly elapsedNow = signal(Date.now());

  readonly signatureMode = signal<'canvas' | 'hash'>(
    ((environment as { isabelSignatureMode?: string }).isabelSignatureMode ?? 'canvas') === 'hash'
      ? 'hash'
      : 'canvas'
  );

  readonly filtersForm = this.fb.group({
    customer: [''],
    dateFrom: [''],
    dateTo: ['']
  });

  readonly pendingItems = computed(() =>
    this.items().filter((item) => item.status === 'AGUARDANDO_ASSINATURA_GERENTE_ESTOQUE')
  );

  readonly completedItems = computed(() =>
    this.items().filter((item) => item.status === 'CONCLUIDO' || item.status === 'FATURADO')
  );

  private drawing = false;
  private previewObjectUrl: string | null = null;
  private analysisPreviewObjectUrl: string | null = null;
  private liveRefreshHandle: ReturnType<typeof setInterval> | null = null;
  private elapsedClockHandle: ReturnType<typeof setInterval> | null = null;
  private liveRefreshBusy = false;

  constructor() {
    this.refreshSavedSignatureState();
    this.loadData();
    this.startLiveRefresh();
    this.startElapsedClock();
  }

  ngOnDestroy(): void {
    this.stopLiveRefresh();
    this.stopElapsedClock();
    this.revokePreview();
  }

  applyFilters(): void {
    this.loadData();
  }

  clearFilters(): void {
    this.filtersForm.reset({
      customer: '',
      dateFrom: '',
      dateTo: ''
    });
    this.loadData();
  }

  openDecisionModal(order: ApprovalOrderItem): void {
    this.selectedOrder.set(order);
    this.decisionModalOpen.set(true);
    this.hasSignature.set(false);
    this.decisionEvents.set([]);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.loadPreview(order.id);
    this.loadAnalysisPreview(order.id);
    this.loadOrderAudit(order.id);
    setTimeout(() => {
      this.resetCanvas();
      this.loadSavedSignature({ silent: true });
    }, 0);
  }

  closeDecisionModal(): void {
    this.decisionModalOpen.set(false);
    this.selectedOrder.set(null);
    this.decisionEvents.set([]);
    this.hasSignature.set(false);
    this.revokePreview();
  }

  signSelectedOrder(): void {
    const order = this.selectedOrder();
    if (!order || order.status !== 'AGUARDANDO_ASSINATURA_GERENTE_ESTOQUE' || this.actionLoading()) {
      return;
    }

    const mode = this.signatureMode();
    let signatureCanvasBase64: string | null = null;
    if (mode === 'canvas') {
      if (!this.hasSignature()) {
        this.errorMessage.set('Assinatura manuscrita obrigatoria para concluir.');
        return;
      }
      signatureCanvasBase64 = this.signaturePad?.nativeElement.toDataURL('image/png') ?? null;
    }

    this.actionLoading.set(true);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.pedidoService
      .signOrder(order.id, {
        signatureMode: mode,
        signatureCanvasBase64
      })
      .pipe(finalize(() => this.actionLoading.set(false)))
      .subscribe({
        next: (response) => {
          const isDone = response.order.status === 'CONCLUIDO' || response.order.status === 'FATURADO';
          const warning =
            response.failedEmails > 0
              ? ` ${response.failedEmails} falha(s) de e-mail na distribuição.`
              : '';
          this.toastMessage.set(
            isDone
              ? `Pedido ${order.orderNumber} finalizado pelo estoque e liberado para entrega.` + warning
              : `Etapa assinada. Pedido agora está em ${this.statusLabel(response.order.status)}.` + warning
          );
          if (isDone) {
            this.downloadOrderById(order.id, `${order.orderNumber}-concluido-estoque.pdf`);
          }
          this.closeDecisionModal();
          this.loadData();
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao assinar pedido.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  downloadOrder(order: ApprovalOrderItem): void {
    this.downloadOrderById(order.id, `${order.orderNumber}.pdf`);
  }

  downloadOrderAnalysis(order: ApprovalOrderItem): void {
    this.pedidoService.downloadOrderAnalysisPdf(order.id).subscribe({
      next: (blob) => {
        this.downloadBlob(blob, `${order.orderNumber}-analise-cliente.pdf`);
      },
      error: () => {
        this.errorMessage.set('Nao foi possivel baixar o PDF de saude financeira deste pedido.');
      }
    });
  }

  downloadManualSignedPreview(order: ApprovalOrderItem): void {
    if (this.manualSigning()) {
      return;
    }
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas || !this.hasSignature()) {
      this.errorMessage.set('Desenhe a assinatura na caneta virtual antes de baixar.');
      return;
    }
    const signatureCanvasBase64 = canvas.toDataURL('image/png');
    this.manualSigning.set(true);
    this.errorMessage.set(null);
    this.pedidoService
      .downloadManualSignedPreview(order.id, signatureCanvasBase64)
      .pipe(finalize(() => this.manualSigning.set(false)))
      .subscribe({
        next: (blob) => {
          this.downloadBlob(blob, `${order.orderNumber}-assinado-manual.pdf`);
          this.toastMessage.set('PDF de pré-visualização gerado com assinatura manual.');
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao gerar PDF com assinatura manual.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  cycleElapsedLabel(order: ApprovalOrderItem): string {
    const startedAt = this.parseIsoDate(order.createdAt);
    if (!startedAt) {
      return '-';
    }
    const end = this.resolveCycleEndDate(order) ?? this.elapsedNow();
    const elapsedMs = Math.max(0, end - startedAt.getTime());
    return this.formatDuration(elapsedMs);
  }

  statusLabel(status: ApprovalOrderStatus | string): string {
    if (status === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL') {
      return 'Aguardando Diretor Comercial';
    }
    if (status === 'AGUARDANDO_ASSINATURA_ISABEL') {
      return 'Aguardando Isabel';
    }
    if (status === 'AGUARDANDO_ASSINATURA_GERENTE_ESTOQUE') {
      return 'Aguardando Gerente de Estoque';
    }
    if (status === 'NEGADO_SEM_LIMITE') {
      return 'Negado sem limite';
    }
    if (status === 'DEVOLVIDO_REVISAO') {
      return 'Devolvido para revisão';
    }
    if (status === 'CONCLUIDO') {
      return 'Concluído';
    }
    if (status === 'FATURADO') {
      return 'Faturado';
    }
    return String(status);
  }

  onSignaturePointerDown(event: PointerEvent): void {
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext('2d');
    if (!context) {
      return;
    }
    const point = this.pointerPoint(canvas, event);
    this.drawing = true;
    canvas.setPointerCapture(event.pointerId);
    context.beginPath();
    context.moveTo(point.x, point.y);
  }

  onSignaturePointerMove(event: PointerEvent): void {
    if (!this.drawing) {
      return;
    }
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext('2d');
    if (!context) {
      return;
    }
    const point = this.pointerPoint(canvas, event);
    context.lineTo(point.x, point.y);
    context.stroke();
    this.hasSignature.set(true);
  }

  onSignaturePointerUp(event: PointerEvent): void {
    const canvas = this.signaturePad?.nativeElement;
    if (canvas && canvas.hasPointerCapture(event.pointerId)) {
      canvas.releasePointerCapture(event.pointerId);
    }
    this.drawing = false;
  }

  clearSignature(): void {
    this.resetCanvas();
    this.hasSignature.set(false);
  }

  saveCurrentSignature(): void {
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas || !this.hasSignature()) {
      this.errorMessage.set('Desenhe a assinatura antes de salvar.');
      return;
    }
    const storageKey = this.signatureStorageKey();
    if (!storageKey) {
      this.errorMessage.set('Usuário não identificado para salvar assinatura.');
      return;
    }
    try {
      localStorage.setItem(storageKey, canvas.toDataURL('image/png'));
      this.hasSavedSignature.set(true);
      this.toastMessage.set('Assinatura salva para reutilização neste navegador.');
    } catch {
      this.errorMessage.set('Falha ao salvar assinatura no navegador.');
    }
  }

  loadSavedSignature(options?: { silent?: boolean }): void {
    const storageKey = this.signatureStorageKey();
    if (!storageKey) {
      return;
    }
    const saved = localStorage.getItem(storageKey);
    if (!saved) {
      this.hasSavedSignature.set(false);
      if (!options?.silent) {
        this.errorMessage.set('Nenhuma assinatura salva encontrada neste navegador.');
      }
      return;
    }
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext('2d');
    if (!context) {
      return;
    }

    const image = new Image();
    image.onload = () => {
      this.resetCanvas();
      context.drawImage(image, 0, 0, canvas.width, canvas.height);
      this.hasSignature.set(true);
      this.hasSavedSignature.set(true);
      this.errorMessage.set(null);
      if (!options?.silent) {
        this.toastMessage.set('Assinatura salva carregada com sucesso.');
      }
    };
    image.onerror = () => {
      this.errorMessage.set('Assinatura salva está inválida. Salve novamente.');
    };
    image.src = saved;
  }

  deleteSavedSignature(): void {
    const storageKey = this.signatureStorageKey();
    if (!storageKey) {
      return;
    }
    localStorage.removeItem(storageKey);
    this.hasSavedSignature.set(false);
    this.toastMessage.set('Assinatura salva removida deste navegador.');
  }

  private loadData(): void {
    this.loading.set(true);
    this.errorMessage.set(null);
    const filters = this.filtersForm.getRawValue();
    this.pedidoService
      .listStockQueue({
        customer: filters.customer || '',
        dateFrom: filters.dateFrom || '',
        dateTo: filters.dateTo || '',
        limit: 500
      })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          this.items.set(response.items);
          this.syncSelectedOrderFromLiveData(response.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar fila de estoque.';
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

  private startElapsedClock(): void {
    this.stopElapsedClock();
    this.elapsedClockHandle = setInterval(() => this.elapsedNow.set(Date.now()), ELAPSED_TICK_MS);
  }

  private stopElapsedClock(): void {
    if (this.elapsedClockHandle) {
      clearInterval(this.elapsedClockHandle);
      this.elapsedClockHandle = null;
    }
  }

  private refreshLiveData(): void {
    if (this.liveRefreshBusy || this.loading() || this.actionLoading() || this.decisionModalOpen()) {
      return;
    }
    if (typeof document !== 'undefined' && document.hidden) {
      return;
    }
    this.liveRefreshBusy = true;
    const filters = this.filtersForm.getRawValue();
    this.pedidoService
      .listStockQueue({
        customer: filters.customer || '',
        dateFrom: filters.dateFrom || '',
        dateTo: filters.dateTo || '',
        limit: 500
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
          // Keep screen responsive and retry automatically on next cycle for transient errors.
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
      this.closeDecisionModal();
      this.toastMessage.set(`Pedido ${selected.orderNumber} saiu da fila de estoque.`);
      return;
    }
    this.selectedOrder.set(updated);
    if (this.decisionModalOpen() && updated.status !== 'AGUARDANDO_ASSINATURA_GERENTE_ESTOQUE') {
      this.closeDecisionModal();
      this.toastMessage.set(
        `Pedido ${updated.orderNumber} atualizado para ${this.statusLabel(updated.status)}.`
      );
    }
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

  private resolveCycleEndDate(order: ApprovalOrderItem): number | null {
    const isClosed = order.status === 'CONCLUIDO' || order.status === 'FATURADO';
    if (!isClosed) {
      return null;
    }
    const finalStamp = this.parseIsoDate(order.signedAt);
    return finalStamp ? finalStamp.getTime() : null;
  }

  private parseIsoDate(value: string | null | undefined): Date | null {
    const raw = String(value || '').trim();
    if (!raw) {
      return null;
    }
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) {
      return null;
    }
    return parsed;
  }

  private formatDuration(totalMs: number): string {
    const totalSeconds = Math.floor(totalMs / 1000);
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;
    if (days > 0) {
      return `${days}d ${String(hours).padStart(2, '0')}h ${String(minutes).padStart(2, '0')}m`;
    }
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  }

  private resetCanvas(): void {
    const canvas = this.signaturePad?.nativeElement;
    if (!canvas) {
      return;
    }
    const context = canvas.getContext('2d');
    if (!context) {
      return;
    }
    context.clearRect(0, 0, canvas.width, canvas.height);
    context.fillStyle = '#ffffff';
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.strokeStyle = '#143a48';
    context.lineWidth = 2;
    context.lineCap = 'round';
    context.lineJoin = 'round';
  }

  private pointerPoint(canvas: HTMLCanvasElement, event: PointerEvent): { x: number; y: number } {
    const rect = canvas.getBoundingClientRect();
    return {
      x: event.clientX - rect.left,
      y: event.clientY - rect.top
    };
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

  private downloadOrderById(orderId: number, filename: string): void {
    this.pedidoService.downloadOrderPdf(orderId).subscribe({
      next: (blob) => {
        this.downloadBlob(blob, filename);
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

  private signatureStorageKey(): string | null {
    const user = this.auth.currentUser();
    if (!user) {
      return null;
    }
    return `pedidos.signature.${user.id}.${String(user.username || '').toLowerCase()}`;
  }

  private refreshSavedSignatureState(): void {
    const storageKey = this.signatureStorageKey();
    if (!storageKey) {
      this.hasSavedSignature.set(false);
      return;
    }
    this.hasSavedSignature.set(!!localStorage.getItem(storageKey));
  }
}

