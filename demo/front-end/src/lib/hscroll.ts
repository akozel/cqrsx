// A horizontal strip under the mouse wheel: vertical wheel movement scrolls it sideways while it
// has somewhere to go; at an edge (or when everything fits) the page scrolls as usual.
export function wheelToHorizontal(e: WheelEvent) {
  const el = e.currentTarget as HTMLElement | null
  if (!el) return
  const delta = e.deltaY !== 0 ? e.deltaY : e.deltaX
  if (delta === 0 || el.scrollWidth <= el.clientWidth) return
  const atEnd = delta > 0 && el.scrollLeft + el.clientWidth >= el.scrollWidth - 1
  const atStart = delta < 0 && el.scrollLeft <= 0
  if (atEnd || atStart) return
  e.preventDefault()
  el.scrollLeft += delta
}
