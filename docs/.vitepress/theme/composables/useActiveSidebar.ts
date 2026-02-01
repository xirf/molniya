import { useData, useRoute } from 'vitepress'
import { computed } from 'vue'

export function useActiveSidebar() {
  const { theme } = useData()
  const route = useRoute()

  return computed(() => {
    const sidebar = theme.value.sidebar
    const path = route.path.replace(/\.html$/, '')

    let items = []
    if (Array.isArray(sidebar)) {
      items = sidebar
    } else if (typeof sidebar === 'object') {
      const key = Object.keys(sidebar)
        .filter(k => path.startsWith(k))
        .sort((a, b) => b.length - a.length)[0]
      
      if (key) items = sidebar[key]
    }

    function containsActive(list: any[]): boolean {
      if (!list) return false
      for (const item of list) {
        if (item.link && item.link.replace(/\.html$/, '') === path) return true
        if (item.items && containsActive(item.items)) return true
      }
      return false
    }

    for (const group of items) {
      if (group.link && group.link.replace(/\.html$/, '') === path) return group
      if (group.items && containsActive(group.items)) return group
    }

    return null
  })
}
