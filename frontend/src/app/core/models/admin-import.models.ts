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
  backupSnapshot?: {
    fileName: string;
    filePath: string;
    metadataFile: string;
    createdAt: string;
  } | null;
}

export interface IngestionHistoryFile {
  id: number;
  fileName: string;
  fileType: 'pdf' | 'excel' | string;
  sourceKind: string;
  recordCount: number;
  meta: Record<string, unknown>;
  createdAt: string;
}

export interface IngestionHistoryItem {
  id: number;
  operationId: string;
  mode: ImportMode | string;
  actorUsername: string;
  strictVendors: boolean;
  appendMode: boolean;
  status: string;
  message: string;
  audit: {
    newRecords?: number;
    updatedRecords?: number;
    ignoredDuplicates?: number;
    errors?: number;
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
  warnings: string[];
  files: IngestionHistoryFile[];
  createdAt: string;
  completedAt: string | null;
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
