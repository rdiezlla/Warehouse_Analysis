import { fetchJson } from '@/services/dataClient'
import type {
  AssociationRuleRow,
  FrequentPairRow,
  MarketBasketDashboardData,
} from '@/types/marketBasket'

const DATA_BASE_PATH = '/data/market_basket'

const toNumber = (value: unknown): number => {
  const numericValue = typeof value === 'number' ? value : Number(value ?? 0)
  return Number.isFinite(numericValue) ? numericValue : 0
}

const mapPair = (row: Record<string, unknown>): FrequentPairRow => ({
  article_a: String(row.article_a ?? ''),
  article_b: String(row.article_b ?? ''),
  shared_transactions: toNumber(row.shared_transactions),
  support: toNumber(row.support),
  confidence_a_to_b: toNumber(row.confidence_a_to_b),
  confidence_b_to_a: toNumber(row.confidence_b_to_a),
  lift: toNumber(row.lift),
})

const mapRule = (row: Record<string, unknown>): AssociationRuleRow => ({
  antecedent: String(row.antecedent ?? ''),
  consequent: String(row.consequent ?? ''),
  support: toNumber(row.support),
  confidence: toNumber(row.confidence),
  lift: toNumber(row.lift),
  shared_transactions: toNumber(row.shared_transactions),
})

export const loadMarketBasketData = async (): Promise<MarketBasketDashboardData> => {
  const [pairsRaw, rulesRaw] = await Promise.all([
    fetchJson<Array<Record<string, unknown>>>(`${DATA_BASE_PATH}/pares_frecuentes.json`).catch(
      () => [],
    ),
    fetchJson<Array<Record<string, unknown>>>(`${DATA_BASE_PATH}/reglas_asociacion.json`).catch(
      () => [],
    ),
  ])

  return {
    pairs: pairsRaw.map(mapPair),
    rules: rulesRaw.map(mapRule),
  }
}
