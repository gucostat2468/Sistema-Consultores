import { AuthSession, SessionUser } from '../models/auth.models';
import { Consultant, ReceivableItem } from '../models/dashboard.models';

interface ReceivableSeed {
  consultantId: number;
  consultantName: string;
  customerName: string;
  customerCode: string;
  documentId: string;
  documentRef: string;
  installment: string;
  dueOffset: number;
  issueOffset: number;
  balance: number;
}

const now = new Date();

const toIso = (offsetInDays: number): string => {
  const date = new Date(now);
  date.setDate(date.getDate() + offsetInDays);
  return date.toISOString().slice(0, 10);
};

export const MOCK_CONSULTANTS: Consultant[] = [
  { id: 1, name: 'Luiz Carlos Abranches', username: 'luiz.carlos.abranches', role: 'consultor' },
  { id: 2, name: 'Alisson Rocha Alencar', username: 'alisson.rocha.alencar', role: 'consultor' },
  { id: 3, name: 'Dustin Akihiro Maeno', username: 'dustin.akihiro.maeno', role: 'consultor' },
  { id: 4, name: 'Thiago Gomes', username: 'thiago.gomes', role: 'consultor' },
  { id: 5, name: 'Gabriel Moura de Almeida', username: 'gabriel.moura.de.almeida', role: 'consultor' },
  { id: 6, name: 'Kallebe Gomes da Silva', username: 'kallebe.gomes.da.silva', role: 'consultor' },
  { id: 7, name: 'Guilherme Aparecido Vanzella', username: 'guilherme.aparecido.vanzella', role: 'consultor' }
];

const mockUsers: Array<{ username: string; password: string; user: SessionUser }> = [
  { username: 'admin', password: 'Admin@123', user: { id: 9000, name: 'Administrador', username: 'admin', role: 'admin' } },
  { username: 'adm', password: 'Admin@123', user: { id: 9001, name: 'Administrador', username: 'adm', role: 'admin' } },
  { username: 'luiz.carlos.abranches', password: 'Consultor@123', user: { id: 1, name: 'Luiz Carlos Abranches', username: 'luiz.carlos.abranches', role: 'consultor' } },
  { username: 'alisson.rocha.alencar', password: 'Consultor@123', user: { id: 2, name: 'Alisson Rocha Alencar', username: 'alisson.rocha.alencar', role: 'consultor' } },
  { username: 'dustin.akihiro.maeno', password: 'Consultor@123', user: { id: 3, name: 'Dustin Akihiro Maeno', username: 'dustin.akihiro.maeno', role: 'consultor' } },
  { username: 'thiago.gomes', password: 'Consultor@123', user: { id: 4, name: 'Thiago Gomes', username: 'thiago.gomes', role: 'consultor' } },
  { username: 'gabriel.moura.de.almeida', password: 'Consultor@123', user: { id: 5, name: 'Gabriel Moura de Almeida', username: 'gabriel.moura.de.almeida', role: 'consultor' } },
  { username: 'kallebe.gomes.da.silva', password: 'Consultor@123', user: { id: 6, name: 'Kallebe Gomes da Silva', username: 'kallebe.gomes.da.silva', role: 'consultor' } },
  { username: 'guilherme.aparecido.vanzella', password: 'Consultor@123', user: { id: 7, name: 'Guilherme Aparecido Vanzella', username: 'guilherme.aparecido.vanzella', role: 'consultor' } }
];

export const getMockUserByCredentials = (
  username: string,
  password: string
): SessionUser | null => {
  const found = mockUsers.find((item) => item.username === username && item.password === password);
  return found?.user ?? null;
};

const seeds: ReceivableSeed[] = [
  { consultantId: 1, consultantName: 'Luiz Carlos Abranches', customerName: 'LCA Drones Ltda', customerCode: '692520', documentId: '848701', documentRef: 'NT:59218', installment: '3/5', dueOffset: 8, issueOffset: -28, balance: 30880 },
  { consultantId: 1, consultantName: 'Luiz Carlos Abranches', customerName: 'Rural Drone Agricultura Ltda', customerCode: '697080', documentId: '855501', documentRef: 'NT:59258', installment: '3/5', dueOffset: 13, issueOffset: -20, balance: 47475 },
  { consultantId: 1, consultantName: 'Luiz Carlos Abranches', customerCode: '696210', customerName: 'VRJ Comercio de Drones Ltda', documentId: '859901', documentRef: 'NT:59304', installment: '3/3', dueOffset: 26, issueOffset: -11, balance: 51816 },
  { consultantId: 1, consultantName: 'Luiz Carlos Abranches', customerCode: '696070', customerName: 'Agro Santana Distribuidora Ltda', documentId: '859001', documentRef: 'NT:59298', installment: '3/4', dueOffset: -6, issueOffset: -14, balance: 62823.04 },
  { consultantId: 1, consultantName: 'Luiz Carlos Abranches', customerCode: '696540', customerName: 'Sousa & Lustosa Ltda', documentId: '851701', documentRef: 'NT:59231', installment: '2/3', dueOffset: -2, issueOffset: -26, balance: 34550 },

  { consultantId: 2, consultantName: 'Alisson Rocha Alencar', customerCode: '696670', customerName: 'Agro Santana Distribuidora Xingu Ltda', documentId: '854001', documentRef: 'NT:59243', installment: '3/4', dueOffset: 6, issueOffset: -29, balance: 89619.6 },
  { consultantId: 2, consultantName: 'Alisson Rocha Alencar', customerCode: '696070', customerName: 'Agro Santana Distribuidora Ltda', documentId: '854101', documentRef: 'NT:59244', installment: '3/4', dueOffset: 6, issueOffset: -29, balance: 70468.9 },
  { consultantId: 2, consultantName: 'Alisson Rocha Alencar', customerCode: '696430', customerName: 'WR Drones Comercio e Servico Ltda', documentId: '858301', documentRef: 'NT:59287', installment: '3/4', dueOffset: 20, issueOffset: -15, balance: 60885.5 },
  { consultantId: 2, consultantName: 'Alisson Rocha Alencar', customerCode: '697080', customerName: 'Rural Drone Agricultura Ltda', documentId: '859401', documentRef: 'NT:59300', installment: '2/4', dueOffset: -4, issueOffset: -13, balance: 134073 },

  { consultantId: 3, consultantName: 'Dustin Akihiro Maeno', customerCode: '696410', customerName: 'Pulveriza Drones Ltda', documentId: '855401', documentRef: 'NT:59253', installment: '2/3', dueOffset: 12, issueOffset: -21, balance: 196885.63 },
  { consultantId: 3, consultantName: 'Dustin Akihiro Maeno', customerCode: '696310', customerName: 'Agrominas Produtos Agropecuarios Ltda', documentId: '853601', documentRef: 'NT:59240', installment: '4/4', dueOffset: 35, issueOffset: -27, balance: 28817.6 },
  { consultantId: 3, consultantName: 'Dustin Akihiro Maeno', customerCode: '696450', customerName: 'Alfa Drone Comercio & Servicos Ltda', documentId: '860101', documentRef: 'NT:59306', installment: '3/3', dueOffset: -1, issueOffset: -10, balance: 14000 },
  { consultantId: 3, consultantName: 'Dustin Akihiro Maeno', customerCode: '697430', customerName: 'TM Drones e Maquinas Ltda', documentId: '861101', documentRef: 'NT:59313', installment: '4/4', dueOffset: 28, issueOffset: -9, balance: 60809.53 },

  { consultantId: 4, consultantName: 'Thiago Gomes', customerCode: '696270', customerName: 'Pulveriza Drones Ltda', documentId: '857301', documentRef: 'NT:59279', installment: '2/3', dueOffset: 13, issueOffset: -15, balance: 185385.73 },
  { consultantId: 4, consultantName: 'Thiago Gomes', customerCode: '696560', customerName: 'Agrosul Maquinas & Equipamentos Agricolas Ltda', documentId: '854301', documentRef: 'NT:59245', installment: '4/4', dueOffset: -3, issueOffset: -29, balance: 73982.91 },
  { consultantId: 4, consultantName: 'Thiago Gomes', customerCode: '696340', customerName: 'Terra Boa Drones Ltda', documentId: '855001', documentRef: 'NT:59249', installment: '4/4', dueOffset: 5, issueOffset: -27, balance: 109460 },
  { consultantId: 4, consultantName: 'Thiago Gomes', customerCode: '696250', customerName: 'Mar Auto Pecas Ltda EPP', documentId: '856301', documentRef: 'NT:59265', installment: '3/3', dueOffset: 11, issueOffset: -20, balance: 52754.66 },

  { consultantId: 5, consultantName: 'Gabriel Moura de Almeida', customerCode: '696080', customerName: 'Eletro Energia Solar Ltda', documentId: '851901', documentRef: 'NT:59537', installment: '3/3', dueOffset: 4, issueOffset: -26, balance: 5360.2 },
  { consultantId: 5, consultantName: 'Gabriel Moura de Almeida', customerCode: '693500', customerName: 'Leite & Silva Eletroeletronicos Ltda', documentId: '857201', documentRef: 'NT:59623', installment: '3/3', dueOffset: 18, issueOffset: -13, balance: 11782.33 },
  { consultantId: 5, consultantName: 'Gabriel Moura de Almeida', customerCode: '697290', customerName: 'VR Solar e Compressores Ltda', documentId: '859201', documentRef: 'NT:59644', installment: '2/2', dueOffset: -5, issueOffset: -8, balance: 12140.49 },

  { consultantId: 6, consultantName: 'Kallebe Gomes da Silva', customerCode: '696600', customerName: 'Agropecuaria Amigos do Campo Ltda', documentId: '860301', documentRef: 'NT:59308', installment: '3/5', dueOffset: 20, issueOffset: -8, balance: 79907.95 },
  { consultantId: 6, consultantName: 'Kallebe Gomes da Silva', customerCode: '696600', customerName: 'Agropecuaria Amigos do Campo Ltda', documentId: '860301', documentRef: 'NT:59308', installment: '4/5', dueOffset: 50, issueOffset: -8, balance: 79907.8 },
  { consultantId: 6, consultantName: 'Kallebe Gomes da Silva', customerCode: '696810', customerName: 'R L M de Miranda Ltda', documentId: '849201', documentRef: 'NT:59221', installment: '3/3', dueOffset: -2, issueOffset: -24, balance: 14770.65 },
  { consultantId: 6, consultantName: 'Kallebe Gomes da Silva', customerCode: '695860', customerName: 'M & E Produtos Agropecuarios Ltda', documentId: '860001', documentRef: 'NT:59305', installment: '1/1', dueOffset: 2, issueOffset: -6, balance: 12400 },

  { consultantId: 7, consultantName: 'Guilherme Aparecido Vanzella', customerCode: '695850', customerName: 'Agrodrone Solucoes Agricolas Ltda', documentId: '856901', documentRef: 'NT:59272', installment: '3/4', dueOffset: 11, issueOffset: -18, balance: 91460 },
  { consultantId: 7, consultantName: 'Guilherme Aparecido Vanzella', customerCode: '695850', customerName: 'Agrodrone Solucoes Agricolas Ltda', documentId: '856901', documentRef: 'NT:59272', installment: '4/4', dueOffset: 41, issueOffset: -18, balance: 91460 },
  { consultantId: 7, consultantName: 'Guilherme Aparecido Vanzella', customerCode: '695850', customerName: 'Agrodrone Solucoes Agricolas Ltda', documentId: '857401', documentRef: 'NT:5', installment: '1/1', dueOffset: -8, issueOffset: -13, balance: 76006.24 }
];

export const MOCK_RECEIVABLES: ReceivableItem[] = seeds.map((seed, index) => {
  const dueDate = toIso(seed.dueOffset);
  return {
    id: index + 1,
    consultantId: seed.consultantId,
    consultantName: seed.consultantName,
    customerName: seed.customerName,
    customerCode: seed.customerCode,
    status: seed.dueOffset < 0 ? 'Vencido' : 'A Vencer',
    documentId: seed.documentId,
    documentRef: seed.documentRef,
    installment: seed.installment,
    issueDate: toIso(seed.issueOffset),
    dueDate,
    balance: seed.balance,
    installmentValue: seed.balance
  };
});

export const buildMockSession = (user: SessionUser): AuthSession => ({
  accessToken: `mock-${user.username}-${Date.now()}`,
  user
});
