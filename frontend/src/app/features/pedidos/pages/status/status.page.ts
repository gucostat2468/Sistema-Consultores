import { CommonModule } from '@angular/common';
import { Component, DestroyRef, ElementRef, OnDestroy, ViewChild, computed, inject, signal } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';
import { FormBuilder, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { forkJoin } from 'rxjs';
import { finalize } from 'rxjs/operators';
import { environment } from '../../../../../environments/environment';
import { IngestionHistoryFile, IngestionHistoryItem } from '../../../../core/models/admin-import.models';
import { AdminImportService } from '../../../../core/services/admin-import.service';
import { AuthService } from '../../../../core/services/auth.service';
import {
  AdminEmailItem,
  ApprovalOrderItem,
  ApprovalOrderStatus,
  ApprovalSummary,
  OrderStatusDetail,
  PedidoService
} from '../../services/pedido.service';

type StatusTab = 'pendentes' | 'negados' | 'importacoes' | 'config';

const DEFAULT_SUMMARY: ApprovalSummary = {
  total: 0,
  pendingSignature: 0,
  negativeNoLimit: 0,
  returnedForReview: 0,
  done: 0,
  billed: 0,
  signedToday: 0
};

const PENDING_SIGNATURE_STATUSES: ApprovalOrderStatus[] = [
  'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL'
];

const FROZEN_SIGNATURE_STATUSES: ApprovalOrderStatus[] = [
  'ASSINADO_AGUARDANDO_DISTRIBUICAO',
  'CONCLUIDO',
  'FATURADO'
];

const LIVE_REFRESH_INTERVAL_MS = 2500;

@Component({
  selector: 'app-status-page',
  standalone: true,
  imports: [CommonModule, FormsModule, ReactiveFormsModule],
  templateUrl: './status.page.html',
  styleUrl: './status.page.scss'
})
export class StatusPage implements OnDestroy {
  private readonly pedidoService = inject(PedidoService);
  private readonly adminImportService = inject(AdminImportService);
  private readonly auth = inject(AuthService);
  private readonly fb = inject(FormBuilder);
  private readonly sanitizer = inject(DomSanitizer);
  private readonly destroyRef = inject(DestroyRef);

  @ViewChild('signaturePad') signaturePad?: ElementRef<HTMLCanvasElement>;

  readonly user = this.auth.currentUser;
  readonly isOperationalUser = computed(() => {
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
    const username = String(this.user()?.username || '').trim().toLowerCase();
    return allowed.includes(username);
  });
  readonly isCommercialDirector = computed(() => {
    const allowed = (
      (environment as { commercialDirectorUsernames?: string[] }).commercialDirectorUsernames ?? [
        'marcos',
        'marcos_dronepro'
      ]
    )
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
    const username = String(this.user()?.username || '').trim().toLowerCase();
    return allowed.includes(username);
  });
  readonly isAdmin = computed(() => this.isCommercialDirector());
  readonly canAccessStatus = computed(() => this.isCommercialDirector());

  readonly loading = signal(true);
  readonly actionLoading = signal(false);
  readonly previewLoading = signal(false);
  readonly analysisPreviewLoading = signal(false);
  readonly auditLoading = signal(false);
  readonly savingConfig = signal(false);
  readonly loadingConfig = signal(false);
  readonly importHistoryLoading = signal(false);
  readonly manualSigning = signal(false);
  readonly errorMessage = signal<string | null>(null);
  readonly toastMessage = signal<string | null>(null);
  readonly importHistoryError = signal<string | null>(null);

  readonly tab = signal<StatusTab>('pendentes');
  readonly summary = signal<ApprovalSummary>(DEFAULT_SUMMARY);
  readonly items = signal<ApprovalOrderItem[]>([]);
  readonly importHistoryItems = signal<IngestionHistoryItem[]>([]);
  readonly selectedOrder = signal<ApprovalOrderItem | null>(null);
  readonly decisionModalOpen = signal(false);
  readonly previewTab = signal<'pedido' | 'analise'>('pedido');
  readonly previewUrl = signal<SafeResourceUrl | null>(null);
  readonly analysisPreviewUrl = signal<SafeResourceUrl | null>(null);
  readonly orderSignatureUrl = signal<SafeResourceUrl | null>(null);
  readonly orderSignatureLoading = signal(false);
  readonly decisionEvents = signal<OrderStatusDetail['events']>([]);
  readonly adminEmails = signal<AdminEmailItem[]>([]);
  readonly adminDraftEmail = signal<Record<number, string>>({});
  readonly hasSignature = signal(false);
  readonly hasSavedSignature = signal(false);

  readonly signatureMode = signal<'canvas' | 'hash'>(
    ((environment as { isabelSignatureMode?: string }).isabelSignatureMode ?? 'canvas') === 'hash'
      ? 'hash'
      : 'canvas'
  );

  readonly filtersForm = this.fb.group({
    status: ['' as '' | ApprovalOrderStatus],
    customer: [''],
    dateFrom: [''],
    dateTo: ['']
  });

  readonly configForm = this.fb.nonNullable.group({
    isabelEmails: ['', [Validators.required]],
    vitorEmails: ['', [Validators.required]],
    marcosEmails: ['', [Validators.required]]
  });

  readonly pendingItems = computed(() =>
    this.items().filter((item) => PENDING_SIGNATURE_STATUSES.includes(item.status))
  );

  readonly negativeItems = computed(() =>
    this.items().filter((item) => item.status === 'NEGADO_SEM_LIMITE')
  );

  readonly latestSpreadsheetImport = computed(() => {
    for (const item of this.importHistoryItems()) {
      const spreadsheet = item.files.find((file) => this.isSpreadsheetFile(file.fileType));
      if (spreadsheet) {
        return {
          item,
          file: spreadsheet,
          processedAt: item.completedAt || item.createdAt
        };
      }
    }
    return null;
  });

  private drawing = false;
  private previewObjectUrl: string | null = null;
  private analysisPreviewObjectUrl: string | null = null;
  private orderSignatureObjectUrl: string | null = null;
  private liveRefreshHandle: ReturnType<typeof setInterval> | null = null;
  private liveRefreshBusy = false;

  constructor() {
    this.refreshSavedSignatureState();
    this.loadMainData();
    this.startLiveRefresh();
  }

  ngOnDestroy(): void {
    this.stopLiveRefresh();
    this.revokePreview();
  }

  changeTab(next: StatusTab): void {
    this.tab.set(next);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    if (next === 'config' && this.isAdmin()) {
      this.loadConfigData();
    }
    if (next === 'importacoes' && this.isAdmin() && this.importHistoryItems().length === 0) {
      this.loadImportHistory();
    }
  }

  setPreviewTab(tab: 'pedido' | 'analise'): void {
    this.previewTab.set(tab);
  }

  refreshImportHistory(): void {
    this.loadImportHistory(true);
  }

  applyFilters(): void {
    this.loading.set(true);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    const filters = this.filtersForm.getRawValue();
    this.pedidoService
      .listStatus({
        status: filters.status || '',
        customer: filters.customer || '',
        dateFrom: filters.dateFrom || '',
        dateTo: filters.dateTo || '',
        limit: 500
      })
      .pipe(finalize(() => this.loading.set(false)))
      .subscribe({
        next: (response) => {
          this.items.set(response.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao filtrar pedidos.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  clearFilters(): void {
    this.filtersForm.reset({
      status: '',
      customer: '',
      dateFrom: '',
      dateTo: ''
    });
    this.loadMainData();
  }

  openDecisionModal(order: ApprovalOrderItem): void {
    this.selectedOrder.set(order);
    this.decisionModalOpen.set(true);
    this.previewTab.set('pedido');
    this.hasSignature.set(false);
    this.manualSigning.set(false);
    this.decisionEvents.set([]);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.loadPreview(order.id);
    this.loadAnalysisPreview(order.id);
    this.loadOrderAudit(order.id);
    if (this.isSignatureFrozen(order)) {
      this.loadOrderSignature(order.id);
    } else {
      this.clearOrderSignature();
    }
    setTimeout(() => {
      this.resetCanvas();
      if (!this.isSignatureFrozen(order)) {
        this.loadSavedSignature({ silent: true });
      }
    }, 0);
  }

  closeDecisionModal(): void {
    this.decisionModalOpen.set(false);
    this.selectedOrder.set(null);
    this.decisionEvents.set([]);
    this.hasSignature.set(false);
    this.manualSigning.set(false);
    this.revokePreview();
    this.clearOrderSignature();
  }

  downloadOrder(order: ApprovalOrderItem): void {
    this.pedidoService.downloadOrderPdf(order.id).subscribe({
      next: (blob) => {
        this.downloadBlob(blob, `${order.orderNumber}.pdf`);
      }
    });
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

  canDelete(order: ApprovalOrderItem): boolean {
    if (!this.isAdmin()) {
      return false;
    }
    if (order.status === 'EXCLUIDO') {
      return false;
    }
    if (
      order.status === 'AGUARDANDO_ASSINATURA_ISABEL' ||
      order.status === 'ASSINADO_AGUARDANDO_DISTRIBUICAO' ||
      order.status === 'CONCLUIDO' ||
      order.status === 'FATURADO' ||
      !!order.signedAt
    ) {
      return false;
    }
    return true;
  }

  deleteOrder(order: ApprovalOrderItem): void {
    if (!this.isAdmin()) {
      this.errorMessage.set('Somente Marcos e Isabel podem excluir solicitações.');
      return;
    }
    if (this.actionLoading()) {
      return;
    }

    const accepted = window.confirm(
      `Excluir a solicitação ${order.orderNumber} de ${order.customerName}? Esta ação remove o item da fila de Status.`
    );
    if (!accepted) {
      return;
    }

    this.actionLoading.set(true);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.pedidoService
      .deleteOrder(order.id)
      .pipe(finalize(() => this.actionLoading.set(false)))
      .subscribe({
        next: () => {
          if (this.selectedOrder()?.id === order.id) {
            this.closeDecisionModal();
          }
          this.toastMessage.set(`Solicitação ${order.orderNumber} excluída com sucesso.`);
          this.loadMainData();
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao excluir solicitação.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  signSelectedOrder(): void {
    const order = this.selectedOrder();
    if (!order || this.actionLoading()) {
      return;
    }
    if (!this.canCurrentUserSign(order)) {
      this.errorMessage.set(
        `Assinatura desta etapa permitida somente para ${this.statusOwnerLabel(order.status)}.`
      );
      return;
    }

    const mode = this.signatureMode();
    let signatureCanvasBase64: string | null = null;
    if (mode === 'canvas') {
      if (!this.hasSignature()) {
        this.errorMessage.set('Assinatura manuscrita obrigatoria para avançar a etapa.');
        return;
      }
      signatureCanvasBase64 = this.signaturePad?.nativeElement.toDataURL('image/png') ?? null;
    }

    this.actionLoading.set(true);
    this.errorMessage.set(null);
    this.pedidoService
      .signOrder(order.id, {
        signatureMode: mode,
        signatureCanvasBase64
      })
      .pipe(finalize(() => this.actionLoading.set(false)))
      .subscribe({
        next: (response) => {
          const isForwardedToIsabel = response.order.status === 'AGUARDANDO_ASSINATURA_ISABEL';
          const warning =
            response.failedEmails > 0
              ? ` Houve ${response.failedEmails} falha(s) de e-mail na distribuição.`
              : '';
          this.toastMessage.set(
            isForwardedToIsabel
              ? 'Pedido assinado e encaminhado para Aprovações da Isabel.' + warning
              : `Etapa concluída. Pedido agora está em ${this.statusLabel(response.order.status)}.` + warning
          );
          this.closeDecisionModal();
          this.loadMainData();
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao assinar pedido.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  isSignatureFrozen(order: ApprovalOrderItem): boolean {
    return FROZEN_SIGNATURE_STATUSES.includes(order.status);
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
          this.toastMessage.set('PDF gerado com assinatura manual da pessoa logada.');
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao gerar PDF com assinatura manual.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  saveConfig(): void {
    if (!this.isAdmin()) {
      return;
    }
    if (this.configForm.invalid || this.savingConfig()) {
      this.configForm.markAllAsTouched();
      return;
    }
    const payload = this.configForm.getRawValue();
    this.savingConfig.set(true);
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.pedidoService
      .updateConfig({
        isabelEmails: this.parseEmailList(payload.isabelEmails),
        vitorEmails: this.parseEmailList(payload.vitorEmails),
        marcosEmails: this.parseEmailList(payload.marcosEmails)
      })
      .pipe(finalize(() => this.savingConfig.set(false)))
      .subscribe({
        next: ({ config }) => {
          this.configForm.patchValue(
            {
              isabelEmails: config.isabelEmails.join(', '),
              vitorEmails: config.vitorEmails.join(', '),
              marcosEmails: config.marcosEmails.join(', ')
            },
            { emitEvent: false }
          );
          this.toastMessage.set('Configuração de e-mails salva com sucesso.');
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao salvar configuração.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  saveAdminEmail(admin: AdminEmailItem): void {
    if (!this.isAdmin()) {
      return;
    }
    const email = String(this.adminDraftEmail()[admin.id] ?? '').trim();
    if (!email) {
      this.errorMessage.set(`Informe e-mail válido para ${admin.name}.`);
      return;
    }
    this.errorMessage.set(null);
    this.toastMessage.set(null);
    this.pedidoService.updateAdminEmail(admin.id, email).subscribe({
      next: ({ item }) => {
        this.adminEmails.set(
          this.adminEmails().map((current) => (current.id === item.id ? item : current))
        );
        this.adminDraftEmail.update((current) => ({ ...current, [item.id]: item.email ?? '' }));
        this.toastMessage.set(`E-mail de ${item.name} atualizado.`);
      },
      error: (error: { error?: { detail?: string }; message?: string }) => {
        const detail = error.error?.detail ?? error.message ?? 'Falha ao atualizar e-mail do admin.';
        this.errorMessage.set(String(detail));
      }
    });
  }

  setAdminDraftEmail(adminId: number, value: string): void {
    this.adminDraftEmail.update((current) => ({ ...current, [adminId]: value }));
  }

  importModeLabel(mode: string): string {
    return mode === 'validate' ? 'Validação' : 'Atualização';
  }

  importStatusLabel(status: string): string {
    const key = status.trim().toLowerCase();
    if (key === 'completed') {
      return 'Concluído';
    }
    if (key === 'failed') {
      return 'Falhou';
    }
    if (key === 'validated_ok') {
      return 'Validado OK';
    }
    if (key === 'validated_error') {
      return 'Validado com erro';
    }
    if (key === 'started') {
      return 'Em execução';
    }
    if (key === 'faturado') {
      return 'Faturado';
    }
    return status;
  }

  statusLabel(status: ApprovalOrderStatus | string): string {
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
    if (status === 'ASSINADO_AGUARDANDO_DISTRIBUICAO') {
      return 'Assinado aguardando distribuição';
    }
    if (status === 'CONCLUIDO') {
      return 'Concluído';
    }
    if (status === 'FATURADO') {
      return 'Concluído';
    }
    if (status === 'EXCLUIDO') {
      return 'Excluído';
    }
    return String(status);
  }

  statusOwnerLabel(status: ApprovalOrderStatus | string): string {
    if (
      status === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL' ||
      status === 'NEGADO_SEM_LIMITE' ||
      status === 'DEVOLVIDO_REVISAO'
    ) {
      return 'Diretor Comercial';
    }
    return 'responsável da etapa';
  }

  canCurrentUserSign(order: ApprovalOrderItem): boolean {
    if (
      order.status === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL' ||
      order.status === 'NEGADO_SEM_LIMITE' ||
      order.status === 'DEVOLVIDO_REVISAO'
    ) {
      return this.isCommercialDirector();
    }
    return false;
  }

  canSignOrder(order: ApprovalOrderItem): boolean {
    return this.canCurrentUserSign(order);
  }

  signatureActionLabel(order: ApprovalOrderItem): string {
    if (
      order.status === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL' ||
      order.status === 'NEGADO_SEM_LIMITE' ||
      order.status === 'DEVOLVIDO_REVISAO'
    ) {
      return 'Assinar e encaminhar para Isabel';
    }
    return 'Assinar etapa';
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

  importFileLabel(file: IngestionHistoryFile): string {
    return this.isSpreadsheetFile(file.fileType) ? 'Planilha' : 'PDF';
  }

  importAuditSummary(item: IngestionHistoryItem): string {
    const audit = item.audit ?? {};
    const created = audit.newRecords ?? 0;
    const updated = audit.updatedRecords ?? 0;
    const errors = audit.errors ?? 0;
    return `novos ${created} | atualizados ${updated} | erros ${errors}`;
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

  private loadMainData(): void {
    this.loading.set(true);
    this.errorMessage.set(null);
    forkJoin({
      summary: this.pedidoService.getSummary(),
      list: this.pedidoService.listStatus({ limit: 500 })
    })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => this.loading.set(false)))
      .subscribe({
        next: ({ summary, list }) => {
          this.summary.set(summary);
          this.items.set(list.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar status dos pedidos.';
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
    if (
      !this.canAccessStatus() ||
      this.liveRefreshBusy ||
      this.loading() ||
      this.actionLoading() ||
      this.decisionModalOpen()
    ) {
      return;
    }
    if (typeof document !== 'undefined' && document.hidden) {
      return;
    }
    this.liveRefreshBusy = true;
    const filters = this.filtersForm.getRawValue();
    forkJoin({
      summary: this.pedidoService.getSummary(),
      list: this.pedidoService.listStatus({
        status: filters.status || '',
        customer: filters.customer || '',
        dateFrom: filters.dateFrom || '',
        dateTo: filters.dateTo || '',
        limit: 500
      })
    })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => (this.liveRefreshBusy = false)))
      .subscribe({
        next: ({ summary, list }) => {
          this.summary.set(summary);
          this.items.set(list.items);
          this.syncSelectedOrderFromLiveData(list.items);
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
    const previousStatus = selected.status;
    const updated = items.find((item) => item.id === selected.id);
    if (!updated) {
      this.closeDecisionModal();
      this.toastMessage.set(`Pedido ${selected.orderNumber} saiu da visualização atual.`);
      return;
    }
    this.selectedOrder.set(updated);
    const wasCommercialSigningStage =
      previousStatus === 'AGUARDANDO_ASSINATURA_DIRETOR_COMERCIAL' ||
      previousStatus === 'NEGADO_SEM_LIMITE' ||
      previousStatus === 'DEVOLVIDO_REVISAO';
    if (
      this.decisionModalOpen() &&
      wasCommercialSigningStage &&
      updated.status !== previousStatus
    ) {
      this.closeDecisionModal();
      if (updated.status === 'AGUARDANDO_ASSINATURA_ISABEL') {
        this.toastMessage.set(`Pedido ${updated.orderNumber} assinado e encaminhado para Isabel.`);
      } else {
        this.toastMessage.set(
          `Pedido ${updated.orderNumber} atualizado para ${this.statusLabel(updated.status)}.`
        );
      }
    }
  }

  private loadConfigData(): void {
    if (!this.isAdmin()) {
      return;
    }
    this.loadingConfig.set(true);
    forkJoin({
      config: this.pedidoService.getConfig(),
      admins: this.pedidoService.getAdminEmails()
    })
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => this.loadingConfig.set(false)))
      .subscribe({
        next: ({ config, admins }) => {
          this.configForm.patchValue(
            {
              isabelEmails: config.config.isabelEmails.join(', '),
              vitorEmails: config.config.vitorEmails.join(', '),
              marcosEmails: config.config.marcosEmails.join(', ')
            },
            { emitEvent: false }
          );
          this.adminEmails.set(admins.items);
          this.adminDraftEmail.set(
            admins.items.reduce<Record<number, string>>((acc, item) => {
              acc[item.id] = item.email ?? '';
              return acc;
            }, {})
          );
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar configuração.';
          this.errorMessage.set(String(detail));
        }
      });
  }

  private loadImportHistory(force = false): void {
    if (!this.isAdmin()) {
      return;
    }
    if (this.importHistoryLoading()) {
      return;
    }
    if (!force && this.importHistoryItems().length > 0) {
      return;
    }

    this.importHistoryLoading.set(true);
    this.importHistoryError.set(null);
    this.adminImportService
      .listHistory(120)
      .pipe(takeUntilDestroyed(this.destroyRef), finalize(() => this.importHistoryLoading.set(false)))
      .subscribe({
        next: (response) => {
          this.importHistoryItems.set(response.items);
        },
        error: (error: { error?: { detail?: string }; message?: string }) => {
          const detail = error.error?.detail ?? error.message ?? 'Falha ao carregar histórico de importação.';
          this.importHistoryError.set(String(detail));
        }
      });
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

  private loadOrderSignature(orderId: number): void {
    this.orderSignatureLoading.set(true);
    this.clearOrderSignature();
    this.pedidoService
      .downloadOrderSignatureImage(orderId)
      .pipe(finalize(() => this.orderSignatureLoading.set(false)))
      .subscribe({
        next: (blob) => {
          this.orderSignatureObjectUrl = URL.createObjectURL(blob);
          this.orderSignatureUrl.set(this.sanitizer.bypassSecurityTrustResourceUrl(this.orderSignatureObjectUrl));
        },
        error: () => {
          this.orderSignatureUrl.set(null);
        }
      });
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
    if (this.orderSignatureObjectUrl) {
      URL.revokeObjectURL(this.orderSignatureObjectUrl);
      this.orderSignatureObjectUrl = null;
    }
    this.previewUrl.set(null);
    this.analysisPreviewUrl.set(null);
    this.orderSignatureUrl.set(null);
  }

  private clearOrderSignature(): void {
    if (this.orderSignatureObjectUrl) {
      URL.revokeObjectURL(this.orderSignatureObjectUrl);
      this.orderSignatureObjectUrl = null;
    }
    this.orderSignatureUrl.set(null);
  }

  private downloadBlob(blob: Blob, filename: string): void {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  private parseEmailList(raw: string): string[] {
    return raw
      .split(/[;,]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  private isSpreadsheetFile(fileType: string): boolean {
    return String(fileType).trim().toLowerCase() === 'excel';
  }

  private signatureStorageKey(): string | null {
    const user = this.user();
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
