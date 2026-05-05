import { useEffect, useMemo, useState } from 'react'
import { AlertTriangle } from 'lucide-react'

import { Panel } from '@/components/ui/Panel'
import { loadABCData } from '@/services/abcDataService'
import type { ABCSkuRow } from '@/types/abc'
import { formatNumber } from '@/utils/formatters'

export const ABCModule = () => {
  const [rows, setRows] = useState<ABCSkuRow[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let isMounted = true
    loadABCData()
      .then((data) => {
        if (isMounted) {
          setRows(data)
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

  const summary = useMemo(() => {
    const byCategory = rows.reduce<Record<string, number>>((accumulator, row) => {
      accumulator[row.abc_category] = (accumulator[row.abc_category] ?? 0) + 1
      return accumulator
    }, {})
    return {
      totalSkus: rows.length,
      aSkus: byCategory.A ?? 0,
      bSkus: byCategory.B ?? 0,
      cSkus: byCategory.C ?? 0,
    }
  }, [rows])

  if (isLoading) {
    return <Panel className="min-h-[220px] animate-rise text-sm text-slate-500">Cargando ABC...</Panel>
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
      <div className="grid gap-4 md:grid-cols-4">
        {[
          ['SKUs', summary.totalSkus],
          ['A', summary.aSkus],
          ['B', summary.bSkus],
          ['C', summary.cSkus],
        ].map(([label, value]) => (
          <Panel key={label} className="animate-rise">
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
            <p className="mt-2 text-2xl font-semibold text-slate-950">{formatNumber(Number(value))}</p>
          </Panel>
        ))}
      </div>

      <Panel className="animate-rise overflow-hidden">
        <div className="mb-4 flex items-center justify-between gap-3">
          <h2 className="text-base font-semibold text-slate-950">Ranking ABC por SKU</h2>
          <span className="text-xs text-slate-500">{formatNumber(rows.length)} registros</span>
        </div>
        <div className="max-h-[620px] overflow-auto">
          <table className="w-full min-w-[760px] border-separate border-spacing-0 text-sm">
            <thead className="sticky top-0 bg-white text-left text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="border-b border-slate-200 py-2 pr-4">Rank</th>
                <th className="border-b border-slate-200 py-2 pr-4">SKU</th>
                <th className="border-b border-slate-200 py-2 pr-4">Clase</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Lineas</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">Unidades</th>
                <th className="border-b border-slate-200 py-2 pr-4 text-right">% Acum.</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 200).map((row) => (
                <tr key={`${row.rank}-${row.sku}`} className="border-b border-slate-100">
                  <td className="border-b border-slate-100 py-2 pr-4 text-slate-500">{row.rank}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 font-medium text-slate-900">{row.sku}</td>
                  <td className="border-b border-slate-100 py-2 pr-4">
                    <span className="rounded-full bg-slate-900 px-2 py-0.5 text-xs font-semibold text-white">
                      {row.abc_category}
                    </span>
                  </td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{formatNumber(row.lines)}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">{formatNumber(row.units)}</td>
                  <td className="border-b border-slate-100 py-2 pr-4 text-right">
                    {(row.cumulative_percentage * 100).toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>
    </div>
  )
}
