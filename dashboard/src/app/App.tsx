import { useState } from 'react'

import { AppShell } from '@/app/AppShell'
import type { ModuleId } from '@/layouts/Sidebar'
import { ABCPage } from '@/pages/ABCPage'
import { ForecastPage } from '@/pages/ForecastPage'
import { MarketBasketPage } from '@/pages/MarketBasketPage'

export const App = () => {
  const [activeModule, setActiveModule] = useState<ModuleId>('forecast')

  return (
    <AppShell activeModule={activeModule} onModuleChange={setActiveModule}>
      {activeModule === 'forecast' && <ForecastPage />}
      {activeModule === 'abc' && <ABCPage />}
      {activeModule === 'marketBasket' && <MarketBasketPage />}
    </AppShell>
  )
}
