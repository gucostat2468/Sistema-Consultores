import { CreditLimitItem } from '../../core/models/dashboard.models';
import { toPercent } from './format';

export type CreditMetricTone = 'ok' | 'attention' | 'critical' | 'no-limit';

export interface CreditMetrics {
  limit: number;
  available: number;
  debtOpen: number;
  debtToLimitRatio: number;
  coverageRatio: number;
  label: string;
  hint: string;
  tone: CreditMetricTone;
}

export interface CreditLookupMaps {
  byCode: Map<string, CreditLimitItem>;
  byName: Map<string, CreditLimitItem>;
}

export function buildCreditLookupMaps(items: CreditLimitItem[]): CreditLookupMaps {
  const byCode = new Map<string, CreditLimitItem>();
  const byName = new Map<string, CreditLimitItem>();

  for (const item of items) {
    const code = (item.customerCode ?? '').trim();
    if (code) {
      byCode.set(`${item.consultantId}::${code}`, item);
    }
    byName.set(`${item.consultantId}::${normalizeCustomerKey(item.customerName)}`, item);
  }

  return { byCode, byName };
}

export function findCreditItem(
  lookup: CreditLookupMaps,
  consultantId: number,
  customerCode: string | null | undefined,
  customerName: string
): CreditLimitItem | null {
  const code = (customerCode ?? '').trim();
  if (code) {
    const byCode = lookup.byCode.get(`${consultantId}::${code}`);
    if (byCode) {
      return byCode;
    }
  }

  const byName = lookup.byName.get(`${consultantId}::${normalizeCustomerKey(customerName)}`);
  return byName ?? null;
}

export function evaluateCreditMetrics(args: {
  debtOpen: number;
  overdue: number;
  credit: CreditLimitItem | null;
}): CreditMetrics {
  const debtOpen = Math.max(Number(args.debtOpen) || 0, 0);
  const overdue = Math.max(Number(args.overdue) || 0, 0);
  const limit = Math.max(Number(args.credit?.creditLimit) || 0, 0);
  const rawAvailable = Math.max(Number(args.credit?.creditAvailable) || 0, 0);
  const inferredAvailable = limit > 0 ? Math.max(limit - debtOpen, 0) : rawAvailable;
  const available = Math.max(rawAvailable, inferredAvailable);
  const debtToLimitRatio = limit > 0 ? debtOpen / limit : 0;
  const coverageRatio = debtOpen > 0 ? available / debtOpen : 1;

  if (!args.credit) {
    return {
      limit,
      available,
      debtOpen,
      debtToLimitRatio,
      coverageRatio,
      label: 'Sem limite',
      hint: 'Sem limite cadastrado',
      tone: 'no-limit'
    };
  }

  const overdueRatio = debtOpen > 0 ? overdue / debtOpen : 0;
  let tone: CreditMetricTone = 'ok';
  if (debtToLimitRatio > 1.03 || overdueRatio >= 0.25) {
    tone = 'critical';
  } else if (overdue > 0 || debtToLimitRatio >= 0.9 || (available <= 0 && debtToLimitRatio >= 1.0)) {
    tone = 'attention';
  }

  return {
    limit,
    available,
    debtOpen,
    debtToLimitRatio,
    coverageRatio,
    label: toPercent(coverageRatio),
    hint: `Div/Lim ${toPercent(debtToLimitRatio)} | Vencido ${toPercent(overdueRatio)}`,
    tone
  };
}

export function toneToCoverageClass(tone: CreditMetricTone): string {
  if (tone === 'critical') {
    return 'coverage coverage-critical';
  }
  if (tone === 'attention') {
    return 'coverage coverage-attention';
  }
  if (tone === 'ok') {
    return 'coverage coverage-ok';
  }
  return 'coverage coverage-no-limit';
}

export function normalizeCustomerKey(value: string): string {
  return value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase()
    .replaceAll('_', ' ')
    .trim()
    .replace(/\s+/g, ' ');
}
