export interface ABCSkuRow {
  rank: number
  sku: string
  sku_description: string | null
  lines: number
  units: number
  stock_quantity: number | null
  percentage: number
  cumulative_percentage: number
  abc_category: 'A' | 'B' | 'C'
  last_pi_date: string | null
  cr_lines: number
  last_cr_date: string | null
}
