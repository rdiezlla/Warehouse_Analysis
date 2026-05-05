import { useEffect, useState } from 'react'
import { AlertTriangle } from 'lucide-react'

import { Panel } from '@/components/ui/Panel'
import { loadMarketBasketData } from '@/services/marketBasketDataService'
import type { MarketBasketDashboardData } from '@/types/marketBasket'
import { formatNumber } from '@/utils/formatters'

export const MarketBasketModule = () => {
  const [data, setData] = useState<MarketBasketDashboardData>({ pairs: [], rules: [] })
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let isMounted = true
    loadMarketBasketData()
      .then((nextData) => {
        if (isMounted) {
          setData(nextData)
          setError(null)
        }
      })
      .catch((caughtError) => {
        if (isMounted) {
          setError(caughtError instanceof Error ? caughtError.message : 'Error desconocido')
        }
      })
      .finally(() => {
        if (isMounted) {
          setIsLoading(false)
        }
      })
    return () => {
      isMounted = false
    }
  }, [])

  if (isLoading) {
    return <Panel className="min-h-[220px] animate-rise text-sm text-slate-500">Cargando Market Basket...</Panel>
  }

  if (error) {
    return (
      <Panel className="min-h-[220px] border-rose-200/70 bg-rose-50/40">
        <div className="flex items-start gap-3 text-rose-700">
          <AlertTriangle size={18} className="mt-0.5 shrink-0" />
          <p className="text-sm">{error}</p>
        </div>
      </Panel>
    )
  }

  return (
    <div className="space-y-5">
      <div className="grid gap-4 md:grid-cols-2">
        <Panel className="animate-rise">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Pares</p>
          <p className="mt-2 text-2xl font-semibold text-slate-950">{formatNumber(data.pairs.length)}</p>
        </Panel>
        <Panel className="animate-rise">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Reglas</p>
          <p className="mt-2 text-2xl font-semibold text-slate-950">{formatNumber(data.rules.length)}</p>
        </Panel>
      </div>

      <Panel className="animate-rise overflow-hidden">
        <div className="mb-4 flex items-center justify-between gap-3">
          <h2 className="text-base font-semibold text-slate-950">Pares frecuentes</h2>
          <span className="text-xs text-slate-500">Top 200</span>
        </div>
        <div className="max-h-[620px] overflow-auto">
          <table className="w-full min-w-[860px] border-separate border-spacing-0 text-sm">
            <thead className="sticky top-0 bg-white text-left text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="border-b border-slate-200 py-2 pr-4">SKU A</th>
                <th className="border-b border-slate-200 py-2 pr-4">SKU B</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Transacciones</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Soporte</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Conf. A-B</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Lift</th>
              </tr>
            </thead>
            <tbody>
              {data.pairs.slice(0, 200).map((row) => (
                <tr key={`${row.article_a}-${row.article_b}`}>
                  <td className="border-b border-slate-100 py-2 pr-4 font-medium text-slate-900">{row.article_a}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 font-medium text-slate-900">{row.article_b}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{formatNumber(row.shared_transactions)}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{(row.support * 100).toFixed(2)}%</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{(row.confidence_a_to_b * 100).toFixed(1)}%</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{row.lift.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  )
}
