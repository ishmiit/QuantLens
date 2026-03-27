import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react' // Fixed: plugin-react instead of react-plugin
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  envDir: '../',
})