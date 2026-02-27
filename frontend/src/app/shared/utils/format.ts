const currency = new Intl.NumberFormat('pt-BR', {
  style: 'currency',
  currency: 'BRL',
  maximumFractionDigits: 2
});

const percent = new Intl.NumberFormat('pt-BR', {
  style: 'percent',
  maximumFractionDigits: 1
});

const shortDate = new Intl.DateTimeFormat('pt-BR', {
  day: '2-digit',
  month: '2-digit',
  year: 'numeric'
});

const parseIsoLocalDate = (value: string): Date => {
  const isoOnlyDate = /^(\d{4})-(\d{2})-(\d{2})$/;
  const match = isoOnlyDate.exec(String(value ?? '').trim());
  if (match) {
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    return new Date(year, month - 1, day);
  }
  return new Date(value);
};

export const toCurrency = (value: number): string => currency.format(value ?? 0);
export const toPercent = (value: number): string => percent.format(value ?? 0);
export const toDate = (isoDate: string): string => shortDate.format(parseIsoLocalDate(isoDate));

export const daysUntil = (isoDate: string): number => {
  const today = new Date();
  const a = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
  const d = parseIsoLocalDate(isoDate);
  const b = new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  return Math.floor((b - a) / 86_400_000);
};
