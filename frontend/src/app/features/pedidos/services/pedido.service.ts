import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from '../../../../environments/environment';

export type ApprovalOrderStatus =
  | 'AGUARDANDO_ASSINATURA_ISABEL'
  | 'NEGADO_SEM_LIMITE'
  | 'DEVOLVIDO_REVISAO'
  | 'ASSINADO_AGUARDANDO_DISTRIBUICAO'
  | 'CONCLUIDO'
  | 'FATURADO'
  | 'EXCLUIDO';

export interface ApprovalOrderItem {
  id: number;
  externalId: string;
  orderNumber: string;
  consultantId: number;
  consultantName?: string;
  requestedByUserId: number;
  requestedByName?: string;
  customerCode: string | null;
  customerName: string;
  customerIdDoc: string | null;
  orderValue: number;
  openBalance: number;
  creditLimit: number;
  overLimit: number;
  status: ApprovalOrderStatus;
  statusReason: string | null;
  extracted: Record<string, unknown>;
  distribution: {
    notifications?: Array<{
      kind: string;
      to: string;
      success: boolean;
      error: string | null;
    }>;
  };
  originalPdfPath: string;
  signedPdfPath: string | null;
  analysisPdfPath?: string | null;
  packagePdfPath?: string | null;
  signatureMode: 'canvas' | 'hash' | null;
  signatureHash: string | null;
  signedByUserId: number | null;
  signedByName?: string | null;
  signedAt: string | null;
  returnedReason: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ApprovalSummary {
  total: number;
  pendingSignature: number;
  negativeNoLimit: number;
  returnedForReview: number;
  done: number;
  billed: number;
  signedToday: number;
}

export interface OrderStatusDetail {
  order: ApprovalOrderItem;
  events: Array<{
    eventType: string;
    actorName: string | null;
    fromStatus: string | null;
    toStatus: string | null;
    message: string | null;
    payload: Record<string, unknown>;
    createdAt: string;
  }>;
}

export interface ExtractOrderResponse {
  extracted: {
    orderNumber: string | null;
    customerIdDoc: string | null;
    customerName: string | null;
    orderValue: number | null;
  };
  warnings: string[];
}

export interface ForwardOrderResponse {
  order: ApprovalOrderItem;
  credit: {
    approved: boolean;
    openBalance: number;
    creditLimit: number;
    overLimit: number;
    reason: string;
  };
  warnings?: string[];
}

export interface OrderConfigResponse {
  config: {
    isabelEmails: string[];
    vitorEmails: string[];
    marcosEmails: string[];
    updatedBy: string | null;
    updatedAt: string | null;
    createdAt: string | null;
  };
}

export interface AdminEmailItem {
  id: number;
  name: string;
  username: string;
  email: string | null;
}

@Injectable({ providedIn: 'root' })
export class PedidoService {
  private readonly http = inject(HttpClient);
  private readonly baseUrl = `${environment.apiBaseUrl}/pedidos`;

  extractFromPdf(file: File): Observable<ExtractOrderResponse> {
    const formData = new FormData();
    formData.append('pdf', file, file.name);
    return this.http.post<ExtractOrderResponse>(`${this.baseUrl}/extrair`, formData);
  }

  forwardOrder(payload: {
    file: File;
    consultantId: number;
    customerCode: string | null;
    customerName: string;
    lookupCustomerName?: string | null;
    orderValue: number;
    orderNumber?: string | null;
    customerIdDoc?: string | null;
    routeByEmail?: boolean;
    recipientEmails?: string | null;
    attachClientAnalysis?: boolean;
  }): Observable<ForwardOrderResponse> {
    const formData = new FormData();
    formData.append('pdf', payload.file, payload.file.name);
    formData.append('consultantId', String(payload.consultantId));
    formData.append('customerName', payload.customerName);
    formData.append('orderValue', String(payload.orderValue));
    formData.append('routeByEmail', payload.routeByEmail ? 'true' : 'false');
    formData.append('attachClientAnalysis', payload.attachClientAnalysis === false ? 'false' : 'true');
    if (payload.customerCode) {
      formData.append('customerCode', payload.customerCode);
    }
    if (payload.lookupCustomerName) {
      formData.append('lookupCustomerName', payload.lookupCustomerName);
    }
    if (payload.orderNumber) {
      formData.append('orderNumber', payload.orderNumber);
    }
    if (payload.customerIdDoc) {
      formData.append('customerIdDoc', payload.customerIdDoc);
    }
    if (payload.routeByEmail && payload.recipientEmails) {
      formData.append('recipientEmails', payload.recipientEmails);
    }
    return this.http.post<ForwardOrderResponse>(`${this.baseUrl}/encaminhar`, formData);
  }

  listStatus(filters: {
    status?: ApprovalOrderStatus | '';
    customer?: string;
    dateFrom?: string;
    dateTo?: string;
    limit?: number;
  } = {}): Observable<{ items: ApprovalOrderItem[] }> {
    let params = new HttpParams();
    if (filters.status) {
      params = params.set('status', filters.status);
    }
    if (filters.customer) {
      params = params.set('customer', filters.customer);
    }
    if (filters.dateFrom) {
      params = params.set('dateFrom', filters.dateFrom);
    }
    if (filters.dateTo) {
      params = params.set('dateTo', filters.dateTo);
    }
    if (filters.limit != null) {
      params = params.set('limit', String(filters.limit));
    }
    return this.http.get<{ items: ApprovalOrderItem[] }>(`${this.baseUrl}/status`, { params });
  }

  listFinancialReceipts(filters: {
    customer?: string;
    dateFrom?: string;
    dateTo?: string;
    limit?: number;
  } = {}): Observable<{ items: ApprovalOrderItem[] }> {
    let params = new HttpParams();
    if (filters.customer) {
      params = params.set('customer', filters.customer);
    }
    if (filters.dateFrom) {
      params = params.set('dateFrom', filters.dateFrom);
    }
    if (filters.dateTo) {
      params = params.set('dateTo', filters.dateTo);
    }
    if (filters.limit != null) {
      params = params.set('limit', String(filters.limit));
    }
    return this.http.get<{ items: ApprovalOrderItem[] }>(`${this.baseUrl}/comprovantes`, { params });
  }

  getSummary(): Observable<ApprovalSummary> {
    return this.http.get<ApprovalSummary>(`${this.baseUrl}/resumo`);
  }

  signOrder(
    orderId: number,
    payload: {
      signatureMode: 'canvas' | 'hash';
      signatureCanvasBase64?: string | null;
    }
  ): Observable<{
    order: ApprovalOrderItem;
    downloadUrl: string;
    signatureMode: 'canvas' | 'hash';
    failedEmails: number;
  }> {
    return this.http.post<{
      order: ApprovalOrderItem;
      downloadUrl: string;
      signatureMode: 'canvas' | 'hash';
      failedEmails: number;
    }>(`${this.baseUrl}/${orderId}/assinar`, payload);
  }

  signAndFinalizeOrder(
    orderId: number,
    payload: {
      signatureMode: 'canvas' | 'hash';
      signatureCanvasBase64?: string | null;
      billingNote?: string | null;
    }
  ): Observable<{
    order: ApprovalOrderItem;
    downloadUrl: string;
    signatureMode: 'canvas' | 'hash';
    failedEmails: number;
    billed: boolean;
  }> {
    return this.http.post<{
      order: ApprovalOrderItem;
      downloadUrl: string;
      signatureMode: 'canvas' | 'hash';
      failedEmails: number;
      billed: boolean;
    }>(`${this.baseUrl}/${orderId}/assinar-concluir`, payload);
  }

  downloadManualSignedPreview(orderId: number, signatureCanvasBase64: string): Observable<Blob> {
    return this.http.post(`${this.baseUrl}/${orderId}/assinar-preview`, {
      signatureCanvasBase64
    }, {
      responseType: 'blob'
    });
  }

  markAsBilled(orderId: number, note?: string | null): Observable<{ order: ApprovalOrderItem }> {
    return this.http.post<{ order: ApprovalOrderItem }>(`${this.baseUrl}/${orderId}/faturar`, {
      note: note ?? null
    });
  }

  returnOrder(orderId: number, reason: string): Observable<{ order: ApprovalOrderItem }> {
    return this.http.post<{ order: ApprovalOrderItem }>(`${this.baseUrl}/${orderId}/devolver`, {
      reason
    });
  }

  deleteOrder(orderId: number): Observable<{ order: ApprovalOrderItem }> {
    return this.http.delete<{ order: ApprovalOrderItem }>(`${this.baseUrl}/${orderId}`);
  }

  getOrderStatus(orderId: number): Observable<OrderStatusDetail> {
    return this.http.get<OrderStatusDetail>(`${this.baseUrl}/${orderId}/status`);
  }

  downloadOrderPdf(orderId: number): Observable<Blob> {
    return this.http.get(`${this.baseUrl}/${orderId}/download`, {
      responseType: 'blob'
    });
  }

  downloadOrderAnalysisPdf(orderId: number): Observable<Blob> {
    return this.http.get(`${this.baseUrl}/${orderId}/download-analise`, {
      responseType: 'blob'
    });
  }

  downloadOrderSignatureImage(orderId: number): Observable<Blob> {
    return this.http.get(`${this.baseUrl}/${orderId}/assinatura`, {
      responseType: 'blob'
    });
  }

  getConfig(): Observable<OrderConfigResponse> {
    return this.http.get<OrderConfigResponse>(`${this.baseUrl}/config`);
  }

  updateConfig(payload: {
    isabelEmails: string[];
    vitorEmails: string[];
    marcosEmails: string[];
  }): Observable<OrderConfigResponse> {
    return this.http.put<OrderConfigResponse>(`${this.baseUrl}/config`, payload);
  }

  getAdminEmails(): Observable<{ items: AdminEmailItem[] }> {
    return this.http.get<{ items: AdminEmailItem[] }>(`${this.baseUrl}/admin-emails`);
  }

  updateAdminEmail(adminId: number, email: string): Observable<{ item: AdminEmailItem }> {
    return this.http.put<{ item: AdminEmailItem }>(`${this.baseUrl}/admin-emails/${adminId}`, {
      email
    });
  }
}
