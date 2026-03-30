import type { PropsWithChildren } from 'react'

import { Sidebar } from '@/layouts/Sidebar'

export const AppShell = ({ children }: PropsWithChildren) => (
  <div className="relative min-h-screen overflow-hidden bg-[var(--app-bg)] text-slate-900">
    <div className="pointer-events-none absolute -left-40 top-10 h-80 w-80 rounded-full bg-cyan-100/70 blur-3xl" />
    <div className="pointer-events-none absolute right-0 top-0 h-72 w-72 rounded-full bg-blue-100/50 blur-3xl" />

    <div className="relative flex min-h-screen flex-col lg:flex-row">
      <Sidebar />
      <main className="flex-1 p-4 md:p-6 lg:p-8">{children}</main>
    </div>
  </div>
)
