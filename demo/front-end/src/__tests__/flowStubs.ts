// Browser APIs Vue Flow needs that jsdom lacks: it measures nodes with a ResizeObserver and reads
// the pane zoom from a DOMMatrix. Nodes stay 0×0 in jsdom, which is fine for DOM assertions.
import { vi } from 'vitest'

export function stubFlowGlobals() {
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  vi.stubGlobal(
    'DOMMatrixReadOnly',
    class {
      m22 = 1
    },
  )
}
