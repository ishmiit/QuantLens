import { useState, useEffect, useRef, useMemo } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { SignedIn, SignedOut, SignInButton, UserButton } from "@clerk/clerk-react";

// @ts-ignore
import { API_URL, WS_URL } from './config';

interface Stock {
  symbol: string;
  price: number;
  change_percent?: number;
  pct_change?: number;
  changePercent?: number;
  rvol?: number;
  rvol_ratio?: number;
  clusterId?: number;
  signal?: 'BUY' | 'SELL';
  ai_signal?: 'BUY' | 'SELL';
  entryPrice?: number;
  stop_loss?: number;
  stopLoss?: number;
  targetPrice?: number;
  target_price?: number;
  isConviction?: boolean;
  lastDirection?: 'up' | 'down';
  // --- NEW FIELDS FROM BACKEND ---
  aiMode?: string;
  ai_mode?: string;
  newTrigger?: boolean;
  probability?: number;
  confidence?: number;
  mc_win_rate?: number;
}

interface Alert {
  id: string;
  symbol: string;
  signal: string;
  mode: string;
}

interface Trade {
  id: number;
  symbol: string;
  entry_price: number;
  stop_loss: number;
  target: number;
  quantity: number;
  trade_type: string;
  status: string;
  timestamp: string;
}

const PriceDisplay = ({ price, className = '', prefix = '' }: { price: number, className?: string, prefix?: string }) => {
  const prevPriceRef = useRef(price);
  const [tick, setTick] = useState<'up' | 'down' | null>(null);

  useEffect(() => {
    if (price > prevPriceRef.current) {
      setTick('up');
      const clear = setTimeout(() => setTick(null), 1500);
      prevPriceRef.current = price;
      return () => clearTimeout(clear);
    } else if (price < prevPriceRef.current) {
      setTick('down');
      const clear = setTimeout(() => setTick(null), 1500);
      prevPriceRef.current = price;
      return () => clearTimeout(clear);
    }
  }, [price]);

  // Use explicit string literals tailored to Tailwind's parser
  // !text-emerald-400 guarantees it overrides `text-white` or `text-slate-100` from className
  const colorClass = tick === 'up'
    ? '!text-emerald-400 drop-shadow-[0_0_8px_rgba(52,211,153,0.8)] transition-none'
    : tick === 'down'
      ? '!text-red-400 drop-shadow-[0_0_8px_rgba(248,113,113,0.8)] transition-none'
      : 'transition-colors duration-1000';

  return (
    <span className={`${className} ${colorClass} font-mono inline-block`}>
      {prefix}{price.toLocaleString('en-IN', { minimumFractionDigits: 2 })}
    </span>
  );
};

function App() {
  const [view, setView] = useState<'dashboard' | 'convictions' | 'forge'>('dashboard');
  const [search, setSearch] = useState('');
  const [sortOrder, setSortOrder] = useState<'performance' | 'a-z' | 'conviction'>('performance');
  const [stocks, setStocks] = useState<Stock[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [activeTrades, setActiveTrades] = useState<Trade[]>([]);

  // --- FORGE STATE ---
  const [forgeSearch, setForgeSearch] = useState('');
  const [forgeEntry, setForgeEntry] = useState<number | ''>('');
  const [forgeQty, setForgeQty] = useState<number | ''>(100);
  const [aiSlPercent, setAiSlPercent] = useState(2.0);
  const [aiTargetPercent, setAiTargetPercent] = useState(5.0);
  const [isAuditing, setIsAuditing] = useState(false); // New: Tracks scanning state
  const [forgeSignal, setForgeSignal] = useState<'BUY' | 'SELL'>('BUY');
  const [isServerAwake, setIsServerAwake] = useState(false);


  // --- NEW: ML AUDIT ENGINE (CONNECTED TO BACKEND) ---
  const [forgeMetrics, setForgeMetrics] = useState<any>(null); // State to hold real feature data

  // --- WEEKEND MARKET QUOTES REST FALLBACK ---
  useEffect(() => {
    fetch(`${API_URL}/api/market-quotes`)
      .then(res => res.json())
      .then(data => {
        if (Array.isArray(data) && data.length > 0) {
          setStocks(data);
        }
      })
      .catch(err => console.error("REST Fallback Error:", err));
  }, []);

  const handleCloseTrade = async (symbol: string, tradeId?: number) => {
    try {
      // Hit the new persistent deletion route
      const response = await fetch(`${API_URL}/api/trades/${encodeURIComponent(symbol)}`, {
        method: 'DELETE'
      });

      const resJSON = await response.json();
      if (response.ok && resJSON.status === 'success') {
        // Only remove from UI after DB confirmation
        setActiveTrades(prev => prev.filter(t => t.symbol !== symbol && t.id !== tradeId));
      } else {
        console.error("Failed to delete trade from DB:", resJSON.message);
      }
    } catch (err) {
      console.error("Network error during trade deletion:", err);
    }
  };

  const handleClearAllTrades = async () => {
    if (!window.confirm("CRITICAL: This will permanently erase ALL trades from PostgreSQL. Proceed?")) return;
    try {
      const response = await fetch(`${API_URL}/api/trades/clear`, {
        method: 'DELETE'
      });
      const resJSON = await response.json();
      if (response.ok && resJSON.status === 'success') {
        setActiveTrades([]);
      }
    } catch (err) {
      console.error("Failed to clear trades:", err);
    }
  };


  const executeLiveTrade = async () => {
    if (!forgeSearch || !forgeEntry || !forgeMetrics) {
      alert("Run Analysis first!");
      return;
    }

    const tradePayload = {
      symbol: forgeSearch.toUpperCase(),
      entryPrice: Number(parseFloat(String(forgeEntry)).toFixed(2)),
      quantity: Number(forgeQty) || 1,
      stop_loss: Number(calculatedSL.toFixed(2)),
      target_price: Number(calculatedTarget.toFixed(2)),
      probability: Number(forgeMetrics.probability || 0),
      signal: forgeSignal
    };

    try {
      // 1. Update live ticker to conviction status
      const response1 = await fetch(`${API_URL}/execute-forge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tradePayload),
      });

      if (!response1.ok) {
        throw new Error("Failed to set Conviction status");
      }

      // 2. Persist to active_trades database
      const persistPayload = {
        symbol: tradePayload.symbol,
        entry_price: tradePayload.entryPrice,
        stop_loss: tradePayload.stop_loss,
        target: tradePayload.target_price,
        quantity: tradePayload.quantity,
        trade_type: 'LIVE'
      };

      const response2 = await fetch(`${API_URL}/api/trades`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(persistPayload),
      });

      const result = await response2.json();

      if (response2.ok && result.status === 'success') {
        setView('convictions');
      } else {
        alert("Trade persistence failed: " + result.message);
      }
    } catch (err) {
      console.error("Execution failed:", err);
      alert("Execution failed, check console.");
    }
  };

  const runAudit = async (symbol: string) => {
    if (!symbol) return;
    setIsAuditing(true);

    try {
      const response = await fetch(`${API_URL}/audit/${symbol.toUpperCase()}`);
      const data = await response.json();

      if (data.error) {
        console.error("Audit error:", data.error);
        setIsAuditing(false);
        return;
      }

      // Only overwrite forgeEntry if the backend returned a real price.
      // handleSendToForge already set it from the live WebSocket tick —
      // we must NOT clobber it with 0 if the audit response is missing price.
      const backendPrice = Number(data.price) || 0;
      if (backendPrice > 0) {
        setForgeEntry(backendPrice);
      }

      // Always update forgeMetrics — any response (even probability=0) clears
      // the "AWAITING INPUT" state which is caused by forgeMetrics being null.
      setForgeMetrics(data);

      // Direct State Integration from Python Backend
      if (data.sl_pct) {
        setAiSlPercent(Number(data.sl_pct) || 2.0);
        setAiTargetPercent(Number(data.tp_pct) || 5.0);
        setForgeSignal((data.signal || 'BUY') as 'BUY' | 'SELL');
      } else {
        console.warn("Backend sent 0 for SL/TP. Using 2%/5% defaults.");
        setAiSlPercent(2.0);
        setAiTargetPercent(5.0);
        setForgeSignal('BUY');
      }

    } catch (err) {
      console.error("Network error during audit:", err);
    } finally {
      setTimeout(() => setIsAuditing(false), 500);
    }
  };

  // --- DYNAMIC CALCULATIONS ---
  const entryNum = Number(forgeEntry) || 0;
  const qtyNum = Number(forgeQty) || 0;
  const slPercNum = Number(aiSlPercent) || 0;
  const tpPercNum = Number(aiTargetPercent) || 0;

  const isShort = forgeSignal === 'SELL';

  const calculatedSL = entryNum > 0
    ? (isShort ? entryNum * (1 + slPercNum / 100) : entryNum * (1 - slPercNum / 100))
    : 0;

  const calculatedTarget = entryNum > 0
    ? (isShort ? entryNum * (1 - tpPercNum / 100) : entryNum * (1 + tpPercNum / 100))
    : 0;

  const totalUsed = entryNum * qtyNum;
  const totalRisk = Math.abs(entryNum - calculatedSL) * qtyNum;

  const getIndexData = (searchTerms: string[]) => {
    return stocks.find(s => 
      searchTerms.some(term => s.symbol.toUpperCase().includes(term.toUpperCase()))
    );
  };

  const handleSendToForge = (stock: any) => {
    const symbol = cleanSymbol(stock.symbol).toUpperCase();
    setForgeSearch(symbol);
    setForgeEntry(Number(stock.price) || 0);
    runAudit(symbol);
    setView('forge');
  };

  const filteredStocks = useMemo(() => {
    const mainList = stocks.filter(s =>
      s.symbol !== 'NIFTY 50' &&
      s.symbol !== 'SENSEX' &&
      s.symbol !== 'BANK NIFTY'
    );

    let result = mainList.filter(s => s.symbol.toLowerCase().includes(search.toLowerCase()));

    if (sortOrder === 'a-z') {
      result.sort((a, b) => a.symbol.localeCompare(b.symbol));
    } else if (sortOrder === 'conviction') {
      // Sort by absolute ML probability, forcing missing/0 values strictly to the bottom
      result.sort((a, b) => {
        const probA = a.probability || a.confidence || 0;
        const probB = b.probability || b.confidence || 0;
        return probB - probA;
      });
    } else {
      result.sort((a, b) => (b.change_percent ?? 0) - (a.change_percent ?? 0));
    }
    return result;
  }, [stocks, search, sortOrder]);

  const parentRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer({
    count: filteredStocks.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 56,
  });

  const socketRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let reconnectTimeout: ReturnType<typeof setTimeout>;
    let isMounted = true;
    let retryCount = 0;

    const wakeUpServer = async () => {
      if (!isMounted) return;
      try {
        const res = await fetch(`${API_URL}/health`);
        if (res.ok) {
          setIsServerAwake(true);
          connectToWebsocket();
        } else {
          throw new Error("Server not ready");
        }
      } catch (error) {
        console.log("Server waking up or unreachable, retrying in 3s...");
        setTimeout(wakeUpServer, 3000);
      }
    };

    const connectToWebsocket = () => {
      if (!isMounted) return;

      console.log("🔌 Attempting WebSocket Connection...");
      const socket = new WebSocket(`${WS_URL}/ws`);
      socketRef.current = socket;

      socket.onopen = () => {
        console.log("✅ WebSocket Connected!");
        retryCount = 0;
      };

      socket.onmessage = (event) => {
        if (!isMounted) return;
        try {
          // 1. Parse the incoming string into a JavaScript object
          const rawData = JSON.parse(event.data);
          console.log("🟢 Raw WS Data Received:", rawData); 

          // 2. Find the array inside the payload
          let dataArray = [];
          if (Array.isArray(rawData)) {
            dataArray = rawData; // The backend sent a pure array
          } else if (rawData && Array.isArray(rawData.data)) {
            dataArray = rawData.data; // The array is nested under 'data'
          } else if (rawData && Array.isArray(rawData.ticks)) {
            dataArray = rawData.ticks; // The array is nested under 'ticks'
          } else {
            console.warn("🟡 WS Data is not an array, skipping forEach:", rawData);
            return; 
          }

          const liveData: Stock[] = dataArray;
          
          // ... (Inner message logic stays the same)
          liveData.forEach(item => {
            if (item.newTrigger && item.isConviction) {
              const id = Math.random().toString(36).substr(2, 9);
              const newAlert = {
                id,
                symbol: item.symbol.split(':').pop() || item.symbol,
                signal: item.signal || 'ALERT',
                mode: item.aiMode || 'SYSTEM'
              };
              setAlerts(prev => [newAlert, ...prev].slice(0, 5));
              setTimeout(() => {
                if (isMounted) setAlerts(prev => prev.filter(a => a.id !== id));
              }, 5000);
            }
          });

          setStocks((prevStocks) => {
            const stockMap = new Map(prevStocks.map(s => [s.symbol, s]));
            return liveData.map((update: any) => {
              const existing = stockMap.get(update.symbol);
              let nextDirection = existing?.lastDirection;
              if (existing && update.price > existing.price) nextDirection = 'up';
              if (existing && update.price < existing.price) nextDirection = 'down';

              return {
                ...existing,
                ...update,
                pct_change: (update.pct_change || update.changePercent) || (existing?.pct_change || existing?.changePercent) || 0,
                change_percent: (update.changePercent || update.pct_change) || (existing?.change_percent || existing?.pct_change) || 0,
                changePercent: (update.changePercent || update.pct_change) || (existing?.changePercent || existing?.pct_change) || 0,
                rvol: (update.rvol || update.rvol_ratio) || (existing?.rvol || existing?.rvol_ratio) || 0,
                rvol_ratio: (update.rvol_ratio || update.rvol) || (existing?.rvol_ratio || existing?.rvol) || 0,
                lastDirection: nextDirection,
                isConviction: update.isConviction ?? existing?.isConviction ?? false,
                entryPrice: update.entryPrice ?? existing?.entryPrice ?? update.price,
                stop_loss: update.stop_loss ?? update.stopLoss ?? existing?.stop_loss ?? existing?.stopLoss ?? 0,
                stopLoss: update.stopLoss ?? update.stop_loss ?? existing?.stopLoss ?? existing?.stop_loss ?? 0,
                target_price: update.target_price ?? update.targetPrice ?? existing?.target_price ?? existing?.targetPrice ?? 0,
                targetPrice: update.targetPrice ?? update.target_price ?? existing?.targetPrice ?? existing?.target_price ?? 0,
                signal: update.signal ?? update.ai_signal ?? existing?.signal ?? existing?.ai_signal,
                aiMode: update.aiMode ?? update.ai_mode ?? existing?.aiMode ?? existing?.ai_mode,
                probability: update.probability ?? update.confidence ?? existing?.probability ?? existing?.confidence ?? 0,
              };
            });
          });
        } catch (err) {
          console.error("Data Sync Error:", err);
        }
      };

      socket.onclose = (event) => {
        if (!isMounted) return;
        
        console.log(`🔌 WebSocket Closed: ${event.reason || 'No reason'}. Retrying...`);
        const delay = Math.min(5000, 1000 * Math.pow(2, retryCount));
        retryCount++;
        reconnectTimeout = setTimeout(connectToWebsocket, delay);
      };

      socket.onerror = (error) => {
        console.error("❌ WebSocket Error:", error);
      };
    };

    wakeUpServer();

    return () => {
      isMounted = false;
      clearTimeout(reconnectTimeout);
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  // --- PERSISTENCE: FETCH TRADES ON CONVICTIONS MOUNT ---
  useEffect(() => {
    if (view === 'convictions') {
      fetch(`${API_URL}/api/trades`)
        .then(res => res.json())
        .then(data => {
          if (Array.isArray(data)) {
            setActiveTrades(data);
          }
        })
        .catch(err => console.error("Failed to fetch active trades:", err));
    }
  }, [view]);

  const handleVirtualForge = async () => {
    if (!forgeSearch || !forgeEntry) return;

    const persistPayload = {
      symbol: forgeSearch.toUpperCase(),
      entry_price: Number(parseFloat(String(forgeEntry)).toFixed(2)),
      stop_loss: Number(calculatedSL.toFixed(2)),
      target: Number(calculatedTarget.toFixed(2)),
      quantity: Number(forgeQty) || 1,
      trade_type: 'VIRTUAL'
    };

    try {
      const response = await fetch(`${API_URL}/api/trades`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(persistPayload),
      });

      const result = await response.json();

      if (response.ok && result.status === 'success') {
        console.log("🛠️ VIRTUAL FORGE TRADE PERSISTED:", persistPayload);
        const id = Math.random().toString(36).substr(2, 9);
        setAlerts(prev => [{ id, symbol: forgeSearch, signal: 'LOGGED', mode: 'VIRTUAL FORGE' }, ...prev].slice(0, 5));
        setTimeout(() => setAlerts(prev => prev.filter(a => a.id !== id)), 5000);

        setView('convictions');
      } else {
        alert("Virtual Trade failed: " + result.message);
      }
    } catch (err) {
      console.error("Virtual Trade Error:", err);
      alert("Virtual Trade Error, check console.");
    }
  };

  const cleanSymbol = (sym: string) => sym.split(':').pop()?.split('.')[0] || sym;

  if (!isServerAwake) {
    return (
      <div className="flex flex-col items-center justify-center h-screen bg-black text-white font-mono">
        <div className="w-12 h-12 border-4 border-[#39ff14] border-t-transparent rounded-full animate-spin mb-4" />
        <h1 className="text-xl font-black tracking-widest text-[#39ff14] mb-2">QUANT<span className="text-white">LENS</span></h1>
        <p className="text-zinc-500 text-xs tracking-[0.2em] animate-pulse">WAKING UP SERVER...</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-screen bg-black text-white font-mono overflow-hidden">

      <SignedOut>
        <div className="flex-1 flex flex-col justify-center items-center bg-gradient-to-br from-black via-zinc-950 to-black">
          <div className="flex flex-col items-center bg-zinc-950 p-10 rounded-2xl border border-accent-green/20 shadow-[0_0_40px_rgba(57,255,20,0.1)]">
            <h1 className="text-3xl font-black tracking-tighter text-white mb-2">
              QUANT<span className="text-accent-green">LENS</span>
            </h1>
            <p className="text-xs text-zinc-500 tracking-[0.4em] mb-8 font-bold">SECURE TERMINAL</p>
            <SignInButton mode="modal">
              <button className="px-8 py-3 bg-accent-green text-black font-black hover:bg-white transition-all uppercase tracking-widest text-sm rounded-md shadow-[0_0_20px_rgba(57,255,20,0.3)]">
                Sign In to QuantLens
              </button>
            </SignInButton>
          </div>
        </div>
      </SignedOut>

      <SignedIn>
        {/* --- ALERT TOAST CONTAINER --- */}
        <div className="fixed bottom-24 md:bottom-6 right-4 md:right-6 z-50 flex flex-col gap-3 max-w-[calc(100vw-2rem)]">
          {alerts.map((alert) => (
            <div
              key={alert.id}
              className={`animate-slide-in p-4 rounded-lg border-2 shadow-[0_0_30px_rgba(0,0,0,0.5)] flex flex-col min-w-[240px] backdrop-blur-md ${alert.signal === 'BUY' ? 'border-accent-green bg-black/90' :
                alert.signal === 'LOGGED' ? 'border-blue-500 bg-black/90' :
                  'border-accent-red bg-black/90'
                }`}
            >
              <div className="flex justify-between items-center mb-1">
                <span className="text-[10px] font-black text-zinc-400 tracking-[0.2em]">{alert.mode}</span>
                <span className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${alert.signal === 'BUY' ? 'bg-accent-green text-black' :
                  alert.signal === 'LOGGED' ? 'bg-blue-500 text-white' :
                    'bg-accent-red text-white'
                  }`}>
                  {alert.signal}
                </span>
              </div>
              <span className="text-xl font-black text-white">{alert.symbol}</span>
              <div className="mt-2 text-[8px] text-zinc-500 font-bold border-t border-white/10 pt-2">
                SIGNAL TIME: {new Date().toLocaleTimeString()}
              </div>
            </div>
          ))}
        </div>

        <nav className="hidden md:flex items-center justify-between h-16 bg-gradient-to-r from-black via-zinc-950 to-black border-b border-accent-green/20 px-8 shrink-0 shadow-[0_4px_24px_rgba(57,255,20,0.1)]">
          <div className="flex items-center w-1/4">
            <div className="flex flex-col">
              <span className="text-xl font-black tracking-tighter text-white">
                QUANT<span className="text-accent-green">LENS</span>
              </span>
              <span className="text-[7px] tracking-[0.6em] text-accent-green opacity-70 italic">ADVANCED ANALYTICS</span>
            </div>
          </div>

          <div className="flex-1 flex justify-center">
            <div className="flex bg-zinc-900/80 backdrop-blur-sm rounded-lg p-1 border border-accent-green/10">
              {['dashboard', 'convictions', 'forge'].map((v) => (
                <button
                  key={v}
                  onClick={() => setView(v as any)}
                  className={`px-8 py-1.5 rounded-md text-[10px] font-bold uppercase tracking-widest transition-all duration-300 ${view === v
                    ? 'bg-accent-green text-black shadow-[0_0_20px_rgba(57,255,20,0.3)]'
                    : 'text-zinc-500 hover:text-accent-green hover:bg-white/5'
                    }`}
                >
                  {v}
                </button>
              ))}
            </div>
          </div>

          <div className="w-1/4 flex justify-end">
            <div className="text-[9px] font-bold text-zinc-500 flex items-center gap-4">
              <span className="text-accent-green flex items-center gap-2">
                <span className="w-1.5 h-1.5 bg-accent-green rounded-full shadow-[0_0_8px_#39ff14] animate-pulse" />
                {filteredStocks.length} ASSETS LIVE
              </span>
              <UserButton appearance={{ elements: { userButtonAvatarBox: "w-8 h-8 rounded-md border-2 border-accent-green/50 hover:border-accent-green transition-colors" } }} />
            </div>
          </div>
        </nav>

        <main className="flex-1 overflow-y-auto p-4 md:p-6 pb-28 md:pb-24 space-y-4 md:space-y-6 flex flex-col bg-gradient-to-b from-black via-zinc-950 to-black">
        {/* DESKTOP INDICES VIEW */}
        <div className="hidden md:grid grid-cols-3 gap-6 shrink-0">
          {[
            { label: 'NIFTY 50', searchTerms: ["NIFTY 50", "NIFTY50"] },
            { label: 'SENSEX', searchTerms: ["SENSEX"] },
            { label: 'BANK NIFTY', searchTerms: ["BANK NIFTY", "NIFTY BANK", "BANKNIFTY"] },
          ].map((idx) => {
            const data = getIndexData(idx.searchTerms);
            return (
              <div
                key={idx.label}
                className="bg-black border border-gray-800 p-5 rounded-2xl shadow-sm"
              >
                <span className="text-[10px] text-gray-500 uppercase tracking-widest font-bold">{idx.label}</span>
                <div className="flex justify-between items-end mt-1">
                  <span className="text-2xl font-bold tabular-nums">
                    {data ? <PriceDisplay price={data.price ?? 0} className="text-white" /> : <span className="text-white">---</span>}
                  </span>
                  <span className={`text-xs font-black ${data && (data.change_percent ?? 0) >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                    {data ? `${(data.change_percent ?? 0) >= 0 ? '+' : ''}${(data.change_percent ?? 0).toFixed(2)}%` : '0.00%'}
                  </span>
                </div>
              </div>
            );
          })}
        </div>

        {/* MOBILE INDICES STRIP — prominent numbers, easier to skim */}
        <div className="flex md:hidden flex-row items-center justify-between w-full px-4 py-2 bg-black border-b border-zinc-900 shrink-0">
          {[
            { label: 'NF50',  searchTerms: ["NIFTY 50", "NIFTY50"] },
            { label: 'SNX',   searchTerms: ["SENSEX"] },
            { label: 'BNF',   searchTerms: ["BANK NIFTY", "NIFTY BANK", "BANKNIFTY"] },
          ].map((idx) => {
            const data = getIndexData(idx.searchTerms);
            const pct  = data?.change_percent ?? 0;
            return (
              <div key={idx.label} className="flex flex-col items-center gap-0.5">
                <span className="text-[9px] text-zinc-500 font-black uppercase tracking-widest">{idx.label}</span>
                <span className="text-sm font-bold font-mono text-white tabular-nums">
                  {data ? data.price?.toLocaleString('en-IN', { maximumFractionDigits: 0 }) : '---'}
                </span>
                <span className={`text-[10px] font-bold ${pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {pct >= 0 ? '+' : ''}{pct.toFixed(2)}%
                </span>
              </div>
            );
          })}
        </div>

        {view === 'dashboard' ? (
          <section className="flex-1 min-h-0 bg-gradient-to-br from-[#0d1117] to-black border border-accent-green/10 rounded-2xl overflow-hidden flex flex-col shadow-[0_8px_32px_rgba(57,255,20,0.05)]">
            <div className="flex justify-between items-center p-4 bg-white/[0.02] border-b border-accent-green/10 backdrop-blur-sm">
              <div className="flex bg-black/60 border border-accent-green/20 rounded-lg items-center px-4 py-2 w-full max-w-md hover:border-accent-green/40 transition-colors">
                <svg className="w-4 h-4 text-accent-green mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                </svg>
                <input
                  type="text"
                  className="bg-transparent border-none outline-none text-xs text-white w-full placeholder:text-zinc-700 font-mono"
                  placeholder="SEARCH SYMBOL..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
              </div>
              <div className="flex gap-2 ml-4">
                {['performance', 'a-z', 'conviction'].map((m) => (
                  <button
                    key={m}
                    onClick={() => setSortOrder(m as any)}
                    className={`px-5 py-2 text-[10px] font-bold uppercase tracking-widest rounded transition-all duration-300 ${sortOrder === m
                      ? 'bg-white text-black shadow-[0_0_12px_rgba(255,255,255,0.3)]'
                      : 'text-zinc-500 hover:text-white border border-white/5 hover:border-white/20'
                      }`}
                  >
                    {m === 'conviction' ? 'AI CONVICTION' : m}
                  </button>
                ))}
              </div>
            </div>

            {/* Column headers: RVOL hidden on mobile */}
            <div className="flex items-center px-4 md:px-10 py-3 text-[10px] text-zinc-500 uppercase font-black tracking-[0.2em] bg-gradient-to-r from-white/[0.02] to-transparent border-b border-accent-green/5">
              <span className="flex-1 md:w-2/4">Identifier</span>
              <span className="w-1/3 md:w-1/4 text-right">LTP (INR)</span>
              <span className="w-1/3 md:w-1/4 text-right">Day Change</span>
              <span className="hidden md:block w-1/4 text-right pr-6">RVOL Index</span>
            </div>

            <div ref={parentRef} className="flex-1 overflow-auto custom-scrollbar">
              <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, width: '100%', position: 'relative' }}>
                {rowVirtualizer.getVirtualItems().map((vRow) => {
                  const s = filteredStocks[vRow.index];
                  if (!s) return null;
                  return (
                    <div
                      key={vRow.key}
                      style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        width: '100%',
                        height: `${vRow.size}px`,
                        transform: `translateY(${vRow.start}px)`
                      }}
                      className="flex items-center px-10 border-b border-white/[0.03] hover:bg-gradient-to-r hover:from-accent-green/5 hover:to-transparent transition-all duration-200 group"
                    >
                      {/* Identifier: expands on mobile since RVOL is hidden */}
                      <div className="flex-1 md:w-2/4 flex items-center gap-2 md:gap-3 min-w-0">
                        <div className={`w-1 h-5 rounded-full transition-all duration-300 shrink-0 ${(s.change_percent ?? 0) >= 0
                          ? 'bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.6)]'
                          : 'bg-red-400 shadow-[0_0_8px_rgba(248,113,113,0.6)]'
                          }`} />
                        <span className="font-black text-white text-[11px] md:text-[12px] tracking-tight group-hover:text-accent-green transition-colors truncate">
                          {cleanSymbol(s.symbol)}
                        </span>
                        <button
                          onClick={(e) => { e.stopPropagation(); handleSendToForge(s); }}
                          className="opacity-0 group-hover:opacity-100 px-1.5 py-0.5 border border-accent-green/50 text-accent-green text-[7px] rounded hover:bg-accent-green hover:text-black transition-all shrink-0"
                        >
                          FORGE
                        </button>
                      </div>
                      <div className="w-1/3 md:w-1/4 text-right">
                        <PriceDisplay
                          price={s.price ?? 0}
                          prefix="₹"
                          className="text-[11px] font-bold tabular-nums text-slate-100"
                        />
                      </div>
                      <div className="w-1/3 md:w-1/4 text-right">
                        <span className={`inline-block px-1.5 md:px-2 py-0.5 rounded text-[10px] font-black transition-all ${(s.change_percent ?? 0) >= 0
                          ? 'text-emerald-400 bg-emerald-400/10'
                          : 'text-red-400 bg-red-400/10'
                          }`}>
                          {(s.change_percent ?? 0) >= 0 ? '+' : ''}{(s.change_percent ?? 0).toFixed(2)}%
                        </span>
                      </div>
                      {/* RVOL bar — hidden on mobile */}
                      <div className="hidden md:flex w-1/4 items-center justify-end gap-2 pr-4">
                        <div className="w-16 h-1 bg-zinc-800 rounded-full overflow-hidden">
                          <div
                            className="h-full bg-accent-green/70 transition-all duration-300"
                            style={{ width: `${Math.min(((s.rvol ?? 0) / 5) * 100, 100)}%` }}
                          />
                        </div>
                        <span className="text-[10px] font-bold text-zinc-500 w-7 tabular-nums text-right">{(s.rvol ?? 0).toFixed(1)}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </section>
        ) : view === 'convictions' ? (
          <section className="flex-1 bg-gradient-to-br from-[#0d1117] to-black border border-accent-green/10 rounded-2xl overflow-hidden flex flex-col shadow-[0_8px_32px_rgba(57,255,20,0.05)]">
            <div className="p-4 md:p-6 border-b border-accent-green/10 bg-gradient-to-r from-white/[0.02] to-transparent flex flex-col gap-3">
              {/* LIVE AI CONVICTION RADAR */}
              {(() => {
                const activeConvictions = stocks.filter(s => s.isConviction && (s.signal === 'BUY' || s.ai_signal === 'BUY'));
                if (activeConvictions.length === 0) return null;
                
                return (
                  <div className="mb-4 flex flex-col gap-2">
                    <div className="flex items-center gap-2">
                      <span className="w-1.5 h-1.5 bg-accent-green rounded-full shadow-[0_0_8px_#39ff14] animate-pulse" />
                      <span className="text-[10px] text-zinc-500 font-black tracking-widest uppercase">Live AI Signals Detected ({activeConvictions.length})</span>
                    </div>
                    <div className="flex overflow-x-auto custom-scrollbar pb-2 gap-2">
                      {activeConvictions.map(stock => {
                        const cleanName = cleanSymbol(stock.symbol);
                        return (
                          <button
                            key={stock.symbol}
                            onClick={() => handleSendToForge(stock)}
                            className="shrink-0 flex items-center gap-2 px-3 py-1.5 bg-accent-green/10 border border-accent-green/30 rounded-md hover:bg-accent-green hover:text-black transition-colors group"
                          >
                            <span className="font-black text-xs group-hover:text-black text-white">{cleanName}</span>
                            <span className="text-[10px] font-mono font-bold group-hover:text-black text-accent-green">{stock.probability || stock.confidence || 0}%</span>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}
              {/* TOP ROW: Title + Clear */}
              <div className="flex justify-between items-center">
                <h2 className="text-white font-black tracking-widest text-base md:text-lg flex items-center gap-2">
                  <svg className="w-4 h-4 md:w-5 md:h-5 text-accent-green shrink-0" fill="currentColor" viewBox="0 0 20 20">
                    <path d="M13 7H7v6h6V7z" />
                    <path fillRule="evenodd" d="M7 2a1 1 0 012 0v1h2V2a1 1 0 112 0v1h2a2 2 0 012 2v2h1a1 1 0 110 2h-1v2h1a1 1 0 110 2h-1v2a2 2 0 01-2 2h-2v1a1 1 0 11-2 0v-1H9v1a1 1 0 11-2 0v-1H5a2 2 0 01-2-2v-2H2a1 1 0 110-2h1V9H2a1 1 0 010-2h1V5a2 2 0 012-2h2V2zM5 5h10v10H5V5z" clipRule="evenodd" />
                  </svg>
                  PORTFOLIO
                </h2>
                <button
                  onClick={handleClearAllTrades}
                  className="px-3 py-1 border border-accent-red/30 text-accent-red text-[9px] md:text-[10px] font-black tracking-widest uppercase rounded hover:bg-accent-red/10 transition-all"
                >
                  CLEAR ALL
                </button>
              </div>

              {/* BOTTOM ROW: P&L Summary */}
              {(() => {
                let totalInvested = 0;
                let currentLiveValue = 0;

                activeTrades.forEach(trade => {
                  const entry = Number(trade.entry_price) || 0;
                  const qty = Number(trade.quantity) || 100;
                  const liveStock = stocks.find(s => cleanSymbol(s.symbol) === cleanSymbol(trade.symbol));
                  const spot = liveStock?.price || entry;
                  const isLong = Number(trade.target) > entry;

                  totalInvested += (entry * qty);

                  if (isLong) {
                    currentLiveValue += (spot * qty);
                  } else {
                    const diff = (entry - spot) * qty;
                    currentLiveValue += (entry * qty) + diff;
                  }
                });

                const totalPnL = currentLiveValue - totalInvested;
                const totalPct = totalInvested > 0 ? (totalPnL / totalInvested) * 100 : 0;
                const isPositive = totalPnL >= 0;

                return (
                  <div className={`flex items-center gap-4 md:gap-8 px-4 md:px-6 py-2 border rounded-xl backdrop-blur-md w-full ${isPositive ? 'border-accent-green/30 bg-accent-green/5' : 'border-accent-red/30 bg-accent-red/5'}`}>
                    <div className="flex flex-col">
                      <span className="text-[9px] text-zinc-500 font-black tracking-widest uppercase">Total P&L</span>
                      <span className={`text-lg md:text-xl font-mono font-black ${isPositive ? 'text-accent-green' : 'text-accent-red'}`}>
                        {isPositive ? '+' : ''}₹{totalPnL.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </span>
                    </div>
                    <div className="flex flex-col border-l border-white/10 pl-4 md:pl-6">
                      <span className="text-[9px] text-zinc-500 font-black tracking-widest uppercase">Net Return</span>
                      <span className={`text-base md:text-lg font-mono font-black ${isPositive ? 'text-accent-green' : 'text-accent-red'}`}>
                        {isPositive ? '+' : ''}{totalPct.toFixed(2)}%
                      </span>
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* Desktop column headers — hidden on mobile */}
            <div className="hidden md:flex items-center px-10 py-3 text-[10px] text-zinc-500 uppercase font-black tracking-[0.2em] bg-gradient-to-r from-white/[0.02] to-transparent border-b border-accent-green/5">
              <span className="w-[15%]">Position</span>
              <span className="w-[10%] text-right text-zinc-400">Qty</span>
              <span className="w-[20%] text-right text-zinc-300">Avg Entry</span>
              <span className="w-[15%] text-right text-white">Spot</span>
              <span className="w-[15%] text-right">Net %</span>
              <span className="w-[15%] text-right text-white">P&L</span>
              <span className="w-[10%] text-right pr-2">Exit</span>
            </div>

            <div className="flex-1 overflow-auto custom-scrollbar pb-10">
              {(() => {
                // 1. Group activeTrades by symbol
                const grouped: Record<string, {
                  symbol: string,
                  totalQty: number,
                  totalInvested: number,
                  isLong: boolean,
                  target: number, // Using the first lot's target as a loose display proxy
                  stop_loss: number, // Using first lot's SL as proxy
                  trade_type: string,
                  lots: Trade[]
                }> = {};

                activeTrades.forEach(t => {
                  const sym = cleanSymbol(t.symbol);
                  const entry = Number(t.entry_price) || 0;
                  const qty = Number(t.quantity) || 100;
                  const target = Number(t.target) || 0;
                  const sl = Number(t.stop_loss) || 0;

                  if (!grouped[sym]) {
                    grouped[sym] = {
                      symbol: sym,
                      totalQty: 0,
                      totalInvested: 0,
                      isLong: target > entry, // Determine direction based on first lot
                      target: target,
                      stop_loss: sl,
                      trade_type: t.trade_type,
                      lots: []
                    };
                  }

                  grouped[sym].totalQty += qty;
                  grouped[sym].totalInvested += (entry * qty);
                  grouped[sym].lots.push(t);
                });

                const positions = Object.values(grouped);

                if (positions.length === 0) return null;

                // 2. Map over grouped positions
                return positions.map((pos) => {
                  const liveStock = stocks.find(s => cleanSymbol(s.symbol) === pos.symbol);
                  const avgEntry = pos.totalQty > 0 ? pos.totalInvested / pos.totalQty : 0;
                  const spot = liveStock?.price || avgEntry;

                  // Position-level Math
                  const livePnL = pos.isLong
                    ? (spot - avgEntry) * pos.totalQty
                    : (avgEntry - spot) * pos.totalQty;

                  const pctChange = avgEntry > 0
                    ? (pos.isLong ? ((spot - avgEntry) / avgEntry) * 100 : ((avgEntry - spot) / avgEntry) * 100)
                    : 0;

                  const isNearTarget = pos.isLong ? spot >= pos.target * 0.995 : spot <= pos.target * 1.005;
                  const isNearSL = pos.isLong ? spot <= pos.stop_loss * 1.005 : spot >= pos.stop_loss * 0.995;

                  return (
                    <div key={pos.symbol} className="group relative border-b border-white/[0.05] transition-colors duration-200">

                      {/* MOBILE CARD VIEW */}
                      <div className="md:hidden p-4 flex flex-col gap-2">
                        <div className="flex justify-between items-start">
                          <div className="flex flex-col gap-1">
                            <span className="font-black text-white text-lg tracking-wide">{pos.symbol}</span>
                            <div className="flex gap-1.5">
                              <span className={`px-1.5 py-0.5 rounded text-[8px] font-black border uppercase ${pos.trade_type === 'LIVE' ? 'border-accent-green text-accent-green bg-accent-green/5' : 'border-blue-500 text-blue-500 bg-blue-500/5'}`}>
                                {pos.trade_type}
                              </span>
                              <span className="px-1.5 py-0.5 rounded text-[8px] text-zinc-500 font-black border border-zinc-700 bg-zinc-800/50 uppercase">
                                {pos.lots.length} LOT{pos.lots.length > 1 ? 'S' : ''}
                              </span>
                            </div>
                          </div>
                          <div className="flex flex-col items-end">
                            <span className={`text-xl font-mono font-black ${livePnL >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                              {livePnL >= 0 ? '+' : '-'}₹{Math.abs(livePnL).toLocaleString('en-IN', { minimumFractionDigits: 0 })}
                            </span>
                            <span className={`text-sm font-bold font-mono ${pctChange >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                              {pctChange > 0 ? '+' : ''}{pctChange.toFixed(2)}%
                            </span>
                          </div>
                        </div>
                        <div className="flex justify-between items-center mt-1">
                          <span className="text-[10px] text-zinc-500 font-mono">Spot: <span className="text-zinc-300">₹{spot.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span></span>
                          <button
                            onClick={(e) => { e.stopPropagation(); pos.lots.forEach(lot => handleCloseTrade(lot.symbol, lot.id)); }}
                            className="px-3 py-1 bg-zinc-900 border border-zinc-700 text-zinc-300 text-[9px] font-black uppercase rounded hover:bg-accent-red/20 hover:text-accent-red hover:border-accent-red/50 transition-all"
                          >
                            EXIT
                          </button>
                        </div>
                        {(isNearTarget || isNearSL) && (
                          <div className={`text-[9px] font-black tracking-widest px-2 py-1 rounded border text-center ${isNearTarget ? 'text-accent-green border-accent-green/30 bg-accent-green/10 animate-pulse' : 'text-accent-red border-accent-red/30 bg-accent-red/10 animate-pulse'}`}>
                            {isNearTarget ? '🎯 TARGET PROXIMITY' : '⚠️ STOP OVERRIDE RISK'}
                          </div>
                        )}
                      </div>

                      {/* DESKTOP ROW VIEW */}
                      <div className="hidden md:flex items-center px-10 py-4 cursor-default z-10 relative hover:bg-white/[0.02]">
                        <div className="w-[15%] flex flex-col gap-1">
                          <span className="font-black text-white text-[15px] tracking-wide group-hover:text-accent-green transition-colors">
                            {pos.symbol}
                          </span>
                          <div className="flex gap-2">
                            <span className={`w-max px-1.5 py-0.5 rounded text-[8px] font-black border uppercase tracking-wider ${pos.trade_type === 'LIVE' ? 'border-accent-green text-accent-green bg-accent-green/5' : 'border-blue-500 text-blue-500 bg-blue-500/5'}`}>
                              {pos.trade_type}
                            </span>
                            <span className="w-max px-1.5 py-0.5 rounded text-[8px] text-zinc-500 font-black border border-zinc-700 bg-zinc-800/50 uppercase tracking-wider">
                              {pos.lots.length} LOT{pos.lots.length > 1 ? 'S' : ''}
                            </span>
                          </div>
                        </div>
                        <div className="w-[10%] text-right text-zinc-400 font-mono text-[13px]">{pos.totalQty}</div>
                        <div className="w-[20%] text-right text-zinc-400 font-mono text-[13px]">₹{avgEntry.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                        <div className="w-[15%] text-right font-mono text-[14px]">
                          <PriceDisplay price={spot} className="text-zinc-100" prefix="₹" />
                        </div>
                        <div className={`w-[15%] text-right font-mono font-bold text-[14px] ${pctChange >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                          {pctChange > 0 ? '+' : ''}{pctChange.toFixed(2)}%
                        </div>
                        <div className={`w-[15%] text-right font-mono font-black text-[15px] ${livePnL >= 0 ? 'text-accent-green' : 'text-accent-red'}`}>
                          {livePnL >= 0 ? '+' : '-'}₹{Math.abs(livePnL).toLocaleString('en-IN', { minimumFractionDigits: 2 })}
                        </div>
                        <div className="w-[10%] flex justify-end pr-2">
                          <button
                            onClick={(e) => { e.stopPropagation(); pos.lots.forEach(lot => handleCloseTrade(lot.symbol, lot.id)); }}
                            className="px-4 py-1.5 bg-zinc-900 border border-zinc-700 text-zinc-300 text-[10px] font-black tracking-widest uppercase rounded cursor-pointer hover:bg-accent-red/20 hover:text-accent-red hover:border-accent-red/50 transition-all"
                          >
                            EXIT ALL
                          </button>
                        </div>
                      </div>

                      {/* EXPANDING DETAILS ACCORDION (Individual Lots) */}
                      <div className="max-h-0 overflow-hidden group-hover:max-h-[500px] transition-all duration-300 ease-in-out bg-black/40 border-t border-white/0 group-hover:border-white/[0.02]">
                        <div className="px-10 py-5 flex flex-col gap-4">

                          {/* Alert Header inside accordion */}
                          <div className="flex items-center justify-between border-b border-white/[0.05] pb-3 mb-1">
                            <span className="text-[10px] font-black tracking-widest text-zinc-500 uppercase">Execution History ({pos.lots.length} Lots)</span>
                            <div className="flex text-right">
                              {isNearTarget ? (
                                <span className="text-[10px] font-black tracking-widest text-accent-green bg-accent-green/10 px-3 py-1 rounded border border-accent-green/30 animate-pulse">🎯 TARGET PROXIMITY</span>
                              ) : isNearSL ? (
                                <span className="text-[10px] font-black tracking-widest text-accent-red bg-accent-red/10 px-3 py-1 rounded border border-accent-red/30 animate-pulse">⚠️ STOP OVERRIDE RISK</span>
                              ) : null}
                            </div>
                          </div>

                          {/* Map over individual lots */}
                          {pos.lots.map((lot, idx) => {
                            const lotEntry = Number(lot.entry_price) || 0;
                            const lotQty = Number(lot.quantity) || 100;
                            const lotDate = new Date(lot.timestamp);
                            const timeString = lotDate.toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit', hour12: true });

                            return (
                              <div key={lot.id} className="flex gap-10 items-center text-xs font-mono text-zinc-500 hover:bg-white/[0.02] p-2 rounded transition-colors -mx-2">
                                <div className="w-12 text-[10px] font-black text-zinc-600">#{idx + 1}</div>
                                <div className="flex-1">
                                  <span className="block text-[9px] uppercase tracking-widest text-zinc-600 font-sans font-black">Entry Price</span>
                                  <span className="text-zinc-300">₹{lotEntry.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                                </div>
                                <div className="flex-1">
                                  <span className="block text-[9px] uppercase tracking-widest text-zinc-600 font-sans font-black">Quantity</span>
                                  <span className="text-zinc-300">{lotQty}</span>
                                </div>
                                <div className="flex-1">
                                  <span className="block text-[9px] uppercase tracking-widest text-zinc-600 font-sans font-black">Stop Loss</span>
                                  <span className="text-accent-red/70">₹{Number(lot.stop_loss).toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                                </div>
                                <div className="flex-1">
                                  <span className="block text-[9px] uppercase tracking-widest text-zinc-600 font-sans font-black">Target</span>
                                  <span className="text-accent-green/70">₹{Number(lot.target).toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                                </div>
                                <div className="flex-1">
                                  <span className="block text-[9px] uppercase tracking-widest text-zinc-600 font-sans font-black">Execution Time</span>
                                  <span className="text-zinc-400 tabular-nums">{timeString}</span>
                                </div>
                                <div>
                                  <button
                                    onClick={(e) => { e.stopPropagation(); handleCloseTrade(lot.symbol, lot.id); }}
                                    className="text-[9px] font-black text-zinc-600 hover:text-accent-red border border-zinc-800 hover:border-accent-red/50 px-2 py-1 rounded transition-colors"
                                  >
                                    CLOSE LOT
                                  </button>
                                </div>
                              </div>
                            );
                          })}

                        </div>
                      </div>
                    </div>
                  );
                });
              })()}

              {activeTrades.length === 0 && (
                <div className="flex flex-col items-center justify-center flex-1 py-24">
                  <svg className="w-12 h-12 text-zinc-800 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                  </svg>
                  <span className="text-xs font-bold tracking-[0.4em] text-zinc-700 uppercase">Waiting for signals</span>
                </div>
              )}
            </div>
          </section>
        ) : (
          /* --- FORGE VIEW (MINIMAL, MOBILE-FIRST) --- */
          <section className="flex-1 flex flex-col gap-4 animate-slide-in">

            {/* SEARCH & AUDIT HEADER */}
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-end bg-zinc-950 border border-zinc-800 p-4 rounded-xl shadow-xl gap-3">
              <div className="w-full sm:w-auto">
                <label className="text-[10px] text-zinc-500 font-bold tracking-[0.2em] mb-1 block">SECURITY IDENTIFIER</label>
                <div className="flex items-center gap-2">
                  <div className="flex items-center bg-black border border-zinc-700 px-3 py-2 focus-within:border-accent-green transition-colors flex-1 sm:flex-initial">
                    <span className="text-zinc-600 mr-2 text-sm">►</span>
                    <input
                      className="bg-transparent border-none outline-none text-xl text-white font-mono tracking-widest uppercase w-full sm:w-36"
                      placeholder="TICKER"
                      value={forgeSearch}
                      onChange={(e) => setForgeSearch(e.target.value.toUpperCase())}
                      onKeyDown={(e) => e.key === 'Enter' && runAudit(forgeSearch)}
                    />
                  </div>
                  <button
                    onClick={() => runAudit(forgeSearch)}
                    className="bg-zinc-800 hover:bg-accent-green text-zinc-300 hover:text-black px-5 py-2.5 font-bold text-xs tracking-widest transition-colors uppercase border border-zinc-700 hover:border-accent-green shrink-0"
                  >
                    {isAuditing ? 'SCANNING...' : 'LOAD'}
                  </button>
                </div>
              </div>
              <span className="text-xs text-accent-green animate-pulse flex items-center gap-2">
                <span className="w-1.5 h-1.5 bg-accent-green rounded-full"></span>
                LIVE
              </span>
            </div>

            {/* MAIN BODY: Gauge + Order Ticket — stacked mobile, 2-column desktop */}
            <div className="flex flex-col md:grid md:grid-cols-2 md:gap-8 gap-4 flex-1 min-h-0 w-full max-w-6xl md:mx-auto md:items-start">

              {/* AI PROBABILITY GAUGE */}
              {(() => {
                const confValue = forgeMetrics?.probability || 0;
                let confColor = 'text-red-500';
                let boxClass = 'border-red-500/20 bg-red-500/5';
                let statusText = 'LOW PROBABILITY';

                if (confValue > 80) {
                  confColor = 'text-green-500';
                  boxClass = 'border-green-500/20 bg-green-500/5';
                  statusText = 'HIGH CONVICTION';
                } else if (confValue > 50) {
                  confColor = 'text-yellow-500';
                  boxClass = 'border-yellow-500/20 bg-yellow-500/5';
                  statusText = 'NEUTRAL / WATCH';
                }

                if (!forgeMetrics && !isAuditing) {
                  confColor = 'text-gray-500';
                  boxClass = 'border-gray-800 bg-black';
                  statusText = 'AWAITING INPUT';
                }

                return (
                  <div className={`p-8 rounded-xl border text-center flex flex-col justify-center items-center transition-all md:flex-1 ${boxClass}`}>
                    <span className="text-[10px] text-gray-500 font-bold tracking-[0.2em] uppercase mb-3">QUANT MODEL ODDS</span>
                    <div className="flex items-baseline gap-1 my-3">
                      <span className={`text-7xl md:text-6xl font-extrabold tabular-nums tracking-tight ${isAuditing ? 'text-gray-600 animate-pulse' : confColor}`}>
                        {isAuditing ? '--' : confValue}
                      </span>
                      <span className={`text-3xl font-bold ${isAuditing ? 'text-gray-700' : 'text-gray-500'}`}>%</span>
                    </div>
                    <span className={`text-[11px] font-black tracking-widest uppercase ${confColor}`}>
                      {isAuditing ? 'CALCULATING...' : statusText}
                    </span>
                    {forgeSearch && (
                      <span className="text-[10px] text-zinc-600 font-mono mt-3 tracking-widest">{forgeSearch}</span>
                    )}
                  </div>
                );
              })()}

              {/* ORDER TICKET */}
              <div className="w-full flex flex-col bg-black border border-zinc-800 rounded-xl shrink-0 overflow-hidden">
                {/* Ticket Header */}
                <div className="px-4 py-2.5 border-b border-zinc-800 bg-zinc-950 flex justify-between items-center">
                  <span className="text-[10px] text-zinc-400 font-black tracking-[0.2em] uppercase">Execution Ticket</span>
                  <span className="text-[9px] text-accent-green font-mono border border-accent-green/30 px-1.5 py-0.5">LIVE</span>
                </div>

                <div className="p-5 flex-1 flex flex-col gap-6 overflow-y-auto custom-scrollbar">
                  {/* ASSET SUMMARY */}
                  <div className="flex justify-between items-end pb-3 border-b border-zinc-800">
                    <div>
                      <span className="text-3xl font-black text-white tracking-tight">{forgeSearch || '---'}</span>
                      <span className="text-[10px] text-zinc-500 block uppercase tracking-widest mt-1">Spot Equities</span>
                    </div>
                    <div className="text-right flex flex-col items-end gap-1.5">
                      {/* Always use forgeEntry (set from live tick) — never forgeMetrics.price which can be 0 */}
                      <span className="text-lg font-mono font-bold text-zinc-200">₹{Number(forgeEntry) > 0 ? Number(forgeEntry).toLocaleString('en-IN', { minimumFractionDigits: 2 }) : ((forgeMetrics as any)?.price?.toLocaleString('en-IN', { minimumFractionDigits: 2 }) || '---')}</span>
                      <div className="flex bg-black border border-zinc-700 p-0.5">
                        <button
                          onClick={() => setForgeSignal('BUY')}
                          className={`px-4 py-1 text-[9px] font-black tracking-widest uppercase transition-all ${forgeSignal === 'BUY' ? 'bg-accent-green text-black' : 'text-zinc-600 hover:text-zinc-300'}`}
                        >
                          BUY
                        </button>
                        <button
                          onClick={() => setForgeSignal('SELL')}
                          className={`px-4 py-1 text-[9px] font-black tracking-widest uppercase transition-all ${forgeSignal === 'SELL' ? 'bg-accent-red text-black' : 'text-zinc-600 hover:text-zinc-300'}`}
                        >
                          SELL
                        </button>
                      </div>
                    </div>
                  </div>

                  {/* INPUTS GRID */}
                  <div className="space-y-4">
                    <div>
                      <div className="flex justify-between text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">
                        <span>Limit Entry Price</span>
                        <span>(INR)</span>
                      </div>
                      <input
                        type="number" step="0.05"
                        className="w-full bg-gray-900 border border-gray-700 rounded-md p-2 text-sm text-white font-mono outline-none focus:border-blue-500 transition-colors"
                        value={forgeEntry}
                        onChange={(e) => setForgeEntry(e.target.value === '' ? '' : parseFloat(e.target.value))}
                        onFocus={(e) => e.target.select()}
                      />
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="flex justify-between text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">
                          <span>Stop Loss</span>
                          <span className="text-red-500">{(aiSlPercent).toFixed(1)}%</span>
                        </div>
                        <div className="w-full bg-gray-900 border border-gray-700 rounded-md p-2 text-right text-sm text-red-500 font-mono">
                          {calculatedSL.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                      </div>
                      <div>
                        <div className="flex justify-between text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">
                          <span>Target</span>
                          <span className="text-green-500">{(aiTargetPercent).toFixed(1)}%</span>
                        </div>
                        <div className="w-full bg-gray-900 border border-gray-700 rounded-md p-2 text-right text-sm text-green-500 font-mono">
                          {calculatedTarget.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                      </div>
                    </div>

                    <div>
                      <div className="flex justify-between text-[10px] text-gray-500 font-bold uppercase tracking-widest mb-1">
                        <span>Order Quantity</span>
                        <span>(Units)</span>
                      </div>
                      <input
                        type="number"
                        className="w-full bg-gray-900 border border-gray-700 rounded-md p-2 text-sm text-white font-mono outline-none focus:border-blue-500 transition-colors"
                        value={forgeQty}
                        onChange={(e) => setForgeQty(e.target.value === '' ? '' : parseInt(e.target.value))}
                        onFocus={(e) => e.target.select()}
                        placeholder="Qty"
                      />
                    </div>
                  </div>

                  {/* MARGIN / RISK SUMMARY */}
                  <div className="bg-black border border-zinc-800 p-4 mt-auto">
                    <div className="flex justify-between text-[10px] font-mono mb-2">
                      <span className="text-zinc-500">Capital Required:</span>
                      <span className="text-white text-sm">₹{totalUsed.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                    </div>
                    <div className="flex justify-between text-[10px] font-mono">
                      <span className="text-zinc-500">Calculated Risk:</span>
                      <span className="text-accent-red text-sm">₹{totalRisk.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                    </div>

                    <div className="flex justify-between text-[10px] font-mono mt-3 pt-3 border-t border-zinc-800/50 items-center">
                      <span className="text-zinc-500">MC Sim ({isAuditing ? '...' : '1M'} Paths):</span>
                      <span className={`text-sm font-bold ${isAuditing ? 'text-zinc-600 animate-pulse' :
                        (forgeMetrics?.mc_win_rate >= 60 ? 'text-accent-green drop-shadow-[0_0_8px_rgba(57,255,20,0.5)]' :
                          forgeMetrics?.mc_win_rate < 40 ? 'text-accent-red' : 'text-yellow-400')
                        }`}>
                        {isAuditing ? 'SIMULATING' : (forgeMetrics?.mc_win_rate !== undefined ? `${Number(forgeMetrics.mc_win_rate).toFixed(1)}% WIN` : '---')}
                      </span>
                    </div>
                  </div>
                </div>

                {/* ACTION BUTTONS */}
                <div className="p-3 bg-black border-t border-zinc-800 flex flex-col gap-2">
                  <button
                    onClick={executeLiveTrade}
                    disabled={!forgeMetrics || isAuditing}
                    className={`w-full py-2.5 text-[11px] font-black uppercase tracking-widest transition-all border ${
                      !forgeMetrics || isAuditing
                        ? 'bg-zinc-900 border-zinc-800 text-zinc-600 cursor-not-allowed'
                        : forgeSignal === 'BUY'
                          ? 'bg-accent-green border-accent-green text-black hover:bg-accent-green/90'
                          : 'bg-accent-red border-accent-red text-black hover:bg-accent-red/90'
                    }`}
                  >
                    {isAuditing ? 'ANALYZING...' : `${forgeSignal}  ${forgeMetrics?.probability || 0}%`}
                  </button>
                  <button
                    onClick={handleVirtualForge}
                    className="w-full bg-transparent border border-zinc-700 text-zinc-400 py-2.5 text-[11px] font-black uppercase tracking-widest hover:border-zinc-500 hover:text-zinc-200 transition-colors"
                  >
                    VIRTUAL PAPER TRADE
                  </button>
                </div>
              </div>
            </div>
          </section>
        )}

        </main>

        {/* --- MOBILE BOTTOM NAVIGATION --- */}
        <div className="flex md:hidden fixed bottom-4 left-1/2 transform -translate-x-1/2 w-[92%] max-w-[420px] h-16 rounded-full bg-zinc-950/95 backdrop-blur-xl border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.6)] z-50 items-center justify-evenly px-2">
          {['dashboard', 'convictions', 'forge'].map((v) => (
            <button
              key={v}
              onClick={() => setView(v as any)}
              className={`flex flex-col items-center justify-center flex-1 h-[75%] mx-1 rounded-full transition-all duration-300 ${
                view === v
                ? 'bg-white text-black font-black shadow-lg py-3 px-4'
                : 'text-zinc-500 font-semibold hover:text-zinc-200'
              }`}
            >
              <span className={`tracking-widest uppercase ${view === v ? 'text-xs font-black' : 'text-[10px]'}`}>
                {v === 'convictions' ? 'CONVICT' : v.toUpperCase()}
              </span>
            </button>
          ))}
        </div>
      </SignedIn>

      <style>{`
        @keyframes slideIn {
          from { transform: translateX(100%); opacity: 0; }
          to { transform: translateX(0); opacity: 1; }
        }
        .animate-slide-in {
          animation: slideIn 0.3s ease-out forwards;
        }
        .custom-scrollbar::-webkit-scrollbar { width: 8px; height: 8px; }
        .custom-scrollbar::-webkit-scrollbar-track { background: #0d1117; border-left: 1px solid rgba(57, 255, 20, 0.1); }
        .custom-scrollbar::-webkit-scrollbar-thumb { background: rgba(57, 255, 20, 0.2); border-radius: 4px; }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover { background: rgba(57, 255, 20, 0.4); }
        .accent-green { color: #39ff14; }
        .bg-accent-green { background-color: #39ff14; }
        .border-accent-green { border-color: #39ff14; }
        .accent-red { color: #ff3131; }
        .bg-accent-red { background-color: #ff3131; }
        .border-accent-red { border-color: #ff3131; }
      `}</style>
    </div>
  );
}

export default App;