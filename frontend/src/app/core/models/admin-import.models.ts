export type ImportMode = 'validate' | 'update';

export interface AdminImportRequest {
  pdfFile: File | null;
  excelFile: File | null;
  mode: ImportMode;
  strictVendors: boolean;
  appendMode: boolean;
  allowSkippedLines: number;
  allowSkippedCreditRows: number;
  inputProfile?: 'auto' | 'report_v1';
}

export interface FileImportResult {
  fileName: string;
  fileType: 'pdf' | 'excel';
  status: 'processed' | 'skipped';
  details: string;
  summaryLines?: string[];
}

export interface AdminImportResponse {
  success: boolean;
  mode: ImportMode;
  message: string;
  warnings: string[];
  files: FileImportResult[];
  audit?: {
    newRecords: number;
    updatedRecords: number;
    ignoredDuplicates: number;
    errors: number;
    stagedRecords?: number;
    stagedReceivables?: number;
    stagedCreditLimits?: number;
    newCustomers?: number;
    newTitles?: number;
    updatedTitles?: number;
    newCreditLimits?: number;
    updatedCreditLimits?: number;
    onlyUpdates?: boolean;
  };
  auditLog?: string[];
  operationId?: string;
  processedAt?: string;
}

export interface AdminClearDataResponse {
  success: boolean;
  message: string;
  operationId?: string;
  processedAt?: string;
  removed: {
    receivables: number;
    creditLimits: number;
    customers: number;
    consultantLinks: number;
    ingestionBatches: number;
    importProfiles: number;
    consultantsRemoved: number;
  };
}
