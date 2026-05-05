export interface FrequentPairRow {
  article_a: string
  article_b: string
  shared_transactions: number
  support: number
  confidence_a_to_b: number
  confidence_b_to_a: number
  lift: number
}

export interface AssociationRuleRow {
  antecedent: string
  consequent: string
  support: number
  confidence: number
  lift: number
  shared_transactions: number
}

export interface MarketBasketDashboardData {
  pairs: FrequentPairRow[]
  rules: AssociationRuleRow[]
}
