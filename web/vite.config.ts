import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// The gateway serves /api and /ws. Proxying keeps the browser on one origin,
// the same shape as the public deployment behind a reverse proxy.
const gateway = process.env.GATEWAY_URL ?? 'http://localhost:8080'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': gateway,
      '/ws': { target: gateway, ws: true },
    },
  },
})
