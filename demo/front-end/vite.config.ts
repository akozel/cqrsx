/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'

// Dev proxy: one origin for the UI, the back-end (/api) and the sidecar (/inspect), so no CORS
// is needed and SSE streams work through the proxy.
export default defineConfig({
  plugins: [vue(), tailwindcss()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': { target: 'http://127.0.0.1:8080', changeOrigin: false },
      '/inspect': { target: 'http://127.0.0.1:8090', changeOrigin: false },
    },
  },
  test: {
    environment: 'node',
    include: ['src/__tests__/**/*.spec.ts'],
  },
})
