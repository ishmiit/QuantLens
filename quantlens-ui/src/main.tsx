import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css' // <--- THIS LINE MUST BE HERE
import { ClerkProvider } from '@clerk/clerk-react'

// Import your publishable key
const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY

if (!PUBLISHABLE_KEY) {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <div className="min-h-screen bg-black flex items-center justify-center p-4">
      <div className="border border-red-500/30 bg-red-900/10 p-6 rounded-lg max-w-md text-center">
        <h1 className="text-red-500 font-bold text-xl mb-2">SYSTEM HALT: Missing Auth Key</h1>
        <p className="text-gray-400 text-sm">
          The VITE_CLERK_PUBLISHABLE_KEY environment variable is missing from the production build environment. 
          Please add it to your hosting dashboard and trigger a re-deploy.
        </p>
      </div>
    </div>
  );
  // We return here so the rest of the file doesn't attempt to execute and crash
} else {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      <ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
        <App />
      </ClerkProvider>
    </React.StrictMode>,
  )
}