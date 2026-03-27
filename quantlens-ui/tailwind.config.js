/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Let's add your QuantLens terminal colors here!
        terminalBg: '#0d1117',
        cardBg: '#161b22',
        accentGreen: '#39ff14',
        accentRed: '#ff3131',
        borderGray: '#30363d',
      },
      keyframes: {
        'flash-green': {
          '0%': { backgroundColor: 'rgba(57, 255, 20, 0.5)' },
          '100%': { backgroundColor: 'transparent' },
        },
        'flash-red': {
          '0%': { backgroundColor: 'rgba(255, 49, 49, 0.5)' },
          '100%': { backgroundColor: 'transparent' },
        },
        'flash-bg-green': {
          '0%': { backgroundColor: 'rgba(57, 255, 20, 0.15)' },
          '100%': { backgroundColor: 'transparent' },
        },
        'flash-bg-red': {
          '0%': { backgroundColor: 'rgba(255, 49, 49, 0.15)' },
          '100%': { backgroundColor: 'transparent' },
        }
      },
      animation: {
        'flash-green': 'flash-green 800ms ease-out forwards',
        'flash-red': 'flash-red 800ms ease-out forwards',
        'flash-bg-green': 'flash-bg-green 800ms ease-out forwards',
        'flash-bg-red': 'flash-bg-red 800ms ease-out forwards',
      }
    },
  },
  plugins: [],
}