import { fetchJson, toNullableNumber } from '@/services/dataClient'
import type { ABCSkuRow } from '@/types/abc'

const DATA_PATH = '/data/abc/abc_sku.json'

export const loadABCData = async (): Promise<ABCSkuRow[]> => {
  const rows = await fetchJson<Array<Record<string, unknown>>>(DATA_PATH).catch(() => [])

  return rows.map((row) => ({
    rank: Number(row.rank ?? 0),
    sku: String(row.sku ?? ''),
    sku_description: row.sku_description ? String(row.sku_description) : null,
    lines: Number(row.lines ?? 0),
    units: Number(row.units ?? 0),
    stock_quantity: toNullableNumber(row.stock_quantity),
    percentage: Number(row.percentage ?? 0),
    cumulative_percentage: Number(row.cumulative_percentage ?? 0),
    abc_category: String(row.abc_category ?? 'C') as ABCSkuRow['abc_category'],
    last_pi_date: row.last_pi_date ? String(row.last_pi_date) : null,
    cr_lines: Number(row.cr_lines ?? 0),
    last_cr_date: row.last_cr_date ? String(row.last_cr_date) : null,
  }))
}
