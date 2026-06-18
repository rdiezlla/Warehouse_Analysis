const legendItems = [
  { label: 'Ubicacion vacia', className: 'bg-slate-300' },
  { label: 'Ubicacion seleccionada', className: 'bg-amber-400' },
  { label: 'Pilares rack', className: 'bg-blue-700' },
  { label: 'Largueros rack', className: 'bg-red-600' },
  { label: 'Zonas suelo', className: 'bg-slate-300' },
]

export const WarehouseLegend = () => (
  <div className="grid gap-2 text-xs text-slate-600 sm:grid-cols-2">
    {legendItems.map((item) => (
      <div key={item.label} className="flex items-center gap-2">
        <span className={`h-3 w-3 rounded-sm ${item.className}`} />
        <span>{item.label}</span>
      </div>
    ))}
  </div>
)
