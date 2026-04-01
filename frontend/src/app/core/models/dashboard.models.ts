export interface Consultant {
  id: number;
  name: string;
  username: string;
  role: 'consultor' | 'admin';
}

export interface PortfolioSummary {
  totalBalance: number;
  due7: number;
  due30: number;
  overdue: number;
  clients: number;
  titles: number;
  overdueRatio: number;
  due7Ratio: number;
  guidance: string;
}

export type FinancialStatus = 'Saudavel' | 'Atencao' | 'Critico';

export interface ClientHealth {
  consultantId: number;
  consultantName: string;
  customerName: string;
  customerCode: string;
  totalBalance: number;
  overdue: number;
  due7: number;
  due30: number;
  titles: number;
  score: number;
  status: FinancialStatus;
  action: string;
}

export interface DashboardCustomerItem {
  consultantId: number;
  consultantName: string;
  customerName: string;
  customerCode: string;
  created: boolean;
}

export interface ReceivableItem {
  id: number;
  consultantId: number;
  consultantName: string;
  customerName: string;
  customerCode: string;
  status: 'A Vencer' | 'Vencido';
  documentId: string;
  documentRef: string;
  installment: string;
  issueDate: string;
  dueDate: string;
  balance: number;
  installmentValue: number;
}

export type CreditAlert =
  | 'Acima do limite'
  | 'Sem limite livre'
  | 'Atencao'
  | 'Controlado'
  | 'Sem exposicao'
  | 'Sem dados';

export interface CreditLimitItem {
  consultantId: number;
  consultantName: string;
  customerName: string;
  customerCode: string | null;
  cnpj: string | null;
  creditLimit: number;
  creditUsed: number;
  creditAvailable: number;
  exposure: number;
  overdue: number;
  due7: number;
  usageRatio: number;
  exposureToAvailableRatio: number;
  alert: CreditAlert;
  updatedAt: string | null;
}

export interface CreditSummary {
  customersWithLimit: number;
  customersWithoutLimit: number;
  totalLimit: number;
  totalUsed: number;
  totalAvailable: number;
  totalExposure: number;
  totalOverdue: number;
  totalDue7: number;
  uncoveredExposure: number;
  portfolioExposure: number;
  usageRatio: number;
  exposureToAvailableRatio: number;
  coverageRatio: number;
  statusCounts: {
    acimaLimite: number;
    semLimiteLivre: number;
    atencao: number;
    controlado: number;
    semExposicao: number;
  };
}

export interface CreditLimitsResponse {
  summary: CreditSummary;
  items: CreditLimitItem[];
}
