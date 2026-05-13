import type { PropsWithChildren } from 'react'
import clsx from 'clsx'

interface PanelProps extends PropsWithChildren {
  className?: string
}

export const Panel = ({ children, className }: PanelProps) => (
  <section
    className={clsx(
      'rounded-2xl border border-white/60 bg-white/85 p-5 shadow-[0_10px_30px_-18px_rgba(16,24,40,0.35)] backdrop-blur',
      className,
    )}
  >
    {children}
  </section>
)
