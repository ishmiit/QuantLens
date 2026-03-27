import pandas as pd
import numpy as np
import plotly.express as px

# 1. Backtest Settings
FILE_NAME = "backtest_apex_v6.csv"
INITIAL_CAPITAL = 100000.0   
FIXED_TRADE_ALLOCATION = 5000.0  # Eternally locked at ₹5,000 per trade

print(f"📊 Loading trades from {FILE_NAME}...")
df = pd.read_csv(FILE_NAME)
df['Entry Time'] = pd.to_datetime(df['Entry Time'])
df['Exit Time'] = pd.to_datetime(df['Exit Time'])

# 2. Build Chronological Event Queue
events = []
for idx, row in df.iterrows():
    events.append({
        'time': row['Entry Time'],
        'type': 'ENTRY',
        'pnl': row['PnL %'],
        'id': idx
    })

# Sort entry events strictly by time to prevent looking ahead
events.sort(key=lambda x: x['time'])

# 3. Setup State Variables
capital = INITIAL_CAPITAL
open_trades = []
history = [] 

accepted_trades = 0
skipped_trades = 0

all_times = sorted(list(set(df['Entry Time'].tolist() + df['Exit Time'].tolist())))
current_entry_idx = 0
total_entries = len(events)

def get_equity():
    return capital + sum(t['invested'] for t in open_trades)

print("🚀 Running Fixed Lot Portfolio Simulation...")

# 4. The Event Loop
for current_time in all_times:
    
    # -- STEP A: PROCESS EXITS FIRST --
    exits_to_remove = []
    for t in open_trades:
        if t['exit_time'] <= current_time:
            capital += t['return_amount']
            exits_to_remove.append(t)
            
    for t in exits_to_remove:
        open_trades.remove(t)
        
    # -- STEP B: PROCESS ENTRIES --
    while current_entry_idx < total_entries and events[current_entry_idx]['time'] == current_time:
        ev = events[current_entry_idx]
        
        # DYNAMIC SLOT CALCULATION
        current_equity = get_equity()
        # Number of slots = Total Equity / 5000 (Drops the decimal)
        max_positions = int(current_equity / FIXED_TRADE_ALLOCATION)
        
        # Check if we have an open slot
        if len(open_trades) < max_positions:
            
            # Ensure we actually have the free cash available
            if capital >= FIXED_TRADE_ALLOCATION * 0.95: 
                actual_alloc = min(capital, FIXED_TRADE_ALLOCATION)
                capital -= actual_alloc
                
                return_amt = actual_alloc * (1 + (ev['pnl'] / 100.0))
                exit_time = df.loc[ev['id'], 'Exit Time']
                
                open_trades.append({
                    'exit_time': exit_time,
                    'return_amount': return_amt,
                    'invested': actual_alloc
                })
                accepted_trades += 1
            else:
                skipped_trades += 1
        else:
            skipped_trades += 1
            
        current_entry_idx += 1
        
    history.append({'Date': current_time, 'Equity': get_equity()})

# 5. Final Calculations
final_equity = get_equity()
years = (all_times[-1] - all_times[0]).days / 365.25

absolute_return = ((final_equity - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100
cagr = ((final_equity / INITIAL_CAPITAL) ** (1 / years) - 1) * 100

history_df = pd.DataFrame(history)
history_df['Peak'] = history_df['Equity'].cummax()
history_df['Drawdown'] = ((history_df['Equity'] - history_df['Peak']) / history_df['Peak']) * 100
max_drawdown = history_df['Drawdown'].min()

# 6. Terminal Output
print("\n" + "="*45)
print("💰 FIXED LOT PORTFOLIO (DYNAMIC SLOTS) 💰")
print("="*45)
print(f"Starting Capital : ₹{INITIAL_CAPITAL:,.2f}")
print(f"Ending Capital   : ₹{final_equity:,.2f}")
print(f"Time in Market   : {years:.2f} Years")
print(f"Risk Per Trade   : ₹{FIXED_TRADE_ALLOCATION:,.2f} (Locked)")
print(f"Absolute Return  : +{absolute_return:,.2f}%")
print(f"Yearly Return    : {cagr:.2f}% (CAGR)")
print(f"Max Drawdown     : {max_drawdown:.2f}%")
print(f"Trades Caught    : {accepted_trades}")
print(f"Trades Skipped   : {skipped_trades} (Capacity full)")
print("="*45)

# 7. Generate Chart
print("📈 Generating Interactive Equity Curve...")
fig = px.line(history_df, x='Date', y='Equity', title='Fixed Lot Equity Curve (Locked at ₹5,000 per trade)')
fig.write_html("equity_curve_fixed_lot.html")
print("✅ Saved as 'equity_curve_fixed_lot.html'.")