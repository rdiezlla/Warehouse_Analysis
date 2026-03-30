import { useEffect, useState } from 'react'

import { loadForecastDashboardData } from '@/services/forecastDataService'
import type { ForecastDashboardData } from '@/types/forecast'

interface ForecastDataState {
  data: ForecastDashboardData | null
  isLoading: boolean
  error: string | null
}

export const useForecastData = (): ForecastDataState => {
  const [state, setState] = useState<ForecastDataState>({
    data: null,
    isLoading: true,
    error: null,
  })

  useEffect(() => {
    let isMounted = true

    const run = async () => {
      try {
        const data = await loadForecastDashboardData()
        if (!isMounted) {
          return
        }
        setState({
          data,
          isLoading: false,
          error: null,
        })
      } catch (error) {
        if (!isMounted) {
          return
        }
        setState({
          data: null,
          isLoading: false,
          error: error instanceof Error ? error.message : 'Error desconocido',
        })
      }
    }

    run()
    return () => {
      isMounted = false
    }
  }, [])

  return state
}
