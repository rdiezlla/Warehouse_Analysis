import { useCallback, useEffect, useState } from 'react'

import { WAREHOUSE_BAYS } from '@/features/warehouse3d/layout/warehouseLayout'
import type {
  RackBayType,
  RackBayTypeOverrides,
} from '@/features/warehouse3d/types'

export const RACK_BAY_TYPE_OVERRIDES_STORAGE_KEY =
  'warehouse3d:rack-bay-type-overrides'

const validBayIds = new Set(WAREHOUSE_BAYS.map((bay) => bay.id))

const readStoredOverrides = (): RackBayTypeOverrides => {
  if (typeof window === 'undefined') {
    return {}
  }

  try {
    const storedValue = window.localStorage.getItem(
      RACK_BAY_TYPE_OVERRIDES_STORAGE_KEY,
    )

    if (!storedValue) {
      return {}
    }

    const parsedValue = JSON.parse(storedValue) as Record<string, unknown>

    return Object.fromEntries(
      Object.entries(parsedValue).filter(
        ([bayId, type]) => validBayIds.has(bayId) && type === 'split-6eu',
      ),
    ) as RackBayTypeOverrides
  } catch {
    return {}
  }
}

export const useRackBayTypeOverrides = () => {
  const [overrides, setOverrides] =
    useState<RackBayTypeOverrides>(readStoredOverrides)

  useEffect(() => {
    try {
      if (Object.keys(overrides).length === 0) {
        window.localStorage.removeItem(RACK_BAY_TYPE_OVERRIDES_STORAGE_KEY)
      } else {
        window.localStorage.setItem(
          RACK_BAY_TYPE_OVERRIDES_STORAGE_KEY,
          JSON.stringify(overrides),
        )
      }
    } catch {
      // The in-memory editor remains usable when storage is unavailable.
    }
  }, [overrides])

  const setBayType = useCallback((bayId: string, type: RackBayType) => {
    if (!validBayIds.has(bayId)) {
      return
    }

    setOverrides((currentOverrides) => {
      const nextOverrides = { ...currentOverrides }

      if (type === 'standard-3eu') {
        delete nextOverrides[bayId]
      } else {
        nextOverrides[bayId] = type
      }

      return nextOverrides
    })
  }, [])

  const resetBayTypes = useCallback(() => setOverrides({}), [])

  return { overrides, setBayType, resetBayTypes }
}
