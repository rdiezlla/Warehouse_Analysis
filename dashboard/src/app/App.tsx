import { useState } from 'react'

import { AppShell } from '@/app/AppShell'
import type { ModuleId } from '@/layouts/Sidebar'
import { ABCPage } from '@/pages/ABCPage'
import { ForecastPage } from '@/pages/ForecastPage'
import { MarketBasketPage } from '@/pages/MarketBasketPage'
import { Warehouse3DPage } from '@/pages/Warehouse3DPage'

export const App = () => {
  const [activeModule, setActiveModule] = useState<ModuleId>('forecast')

  return (
    <AppShell activeModule={activeModule} onModuleChange={setActiveModule}>
      {activeModule === 'forecast' && <ForecastPage />}
      {activeModule === 'abc' && <ABCPage />}
      {activeModule === 'marketBasket' && <MarketBasketPage />}
      {activeModule === 'warehouse3d' && <Warehouse3DPage />}
    </AppShell>
  )
}
