export const fetchJson = async <T>(path: string): Promise<T> => {
  const response = await fetch(path, {
    headers: {
      'Cache-Control': 'no-cache',
    },
  })

  if (!response.ok) {
    throw new Error(`No se pudo cargar ${path}: ${response.status}`)
  }

  return (await response.json()) as T
}

export const toNullableNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') {
    return null
  }

  const numericValue = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(numericValue) ? numericValue : null
}
