interface WarehouseControlsProps {
  showLabels: boolean
  showReferenceZones: boolean
  onShowLabelsChange: (showLabels: boolean) => void
  onShowReferenceZonesChange: (showReferenceZones: boolean) => void
}

export const WarehouseControls = ({
  showLabels,
  showReferenceZones,
  onShowLabelsChange,
  onShowReferenceZonesChange,
}: WarehouseControlsProps) => (
  <div className="space-y-5">
    <label className="flex items-center gap-3 text-sm font-medium text-slate-700">
      <input
        type="checkbox"
        checked={showLabels}
        onChange={(event) => onShowLabelsChange(event.target.checked)}
        className="h-4 w-4 rounded border-slate-300 text-cyan-600 focus:ring-cyan-500"
      />
      Mostrar etiquetas
    </label>

    <label className="flex items-center gap-3 text-sm font-medium text-slate-700">
      <input
        type="checkbox"
        checked={showReferenceZones}
        onChange={(event) => onShowReferenceZonesChange(event.target.checked)}
        className="h-4 w-4 rounded border-slate-300 text-cyan-600 focus:ring-cyan-500"
      />
      Mostrar zonas de referencia
    </label>
  </div>
)
