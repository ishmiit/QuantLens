import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import classification_report, accuracy_score, make_scorer, fbeta_score, precision_recall_curve
# Removed CalibratedClassifierCV to prevent over-smoothing base rate collapse
import joblib

def train_dual_strategy():
    # Feature columns matching your CSV export
    feature_cols = [
        'rvol', 'change_percent', 'cluster_id', 'rsi', 'dist_sma_20', 
        'volatility', 'adx', 'obv', 'bb_pb', 'vwap_dist', 'day_of_week', 'time_float'
    ]
    
    strategies = [
        {
            "name": "INTRADAY (Sniper)",
            "target": "target_intraday",
            "file": "intraday_model.joblib",
            "csv_path": "train_intraday_30m_v2.csv",
            "search_space": {
                "max_depth": [3, 4, 5],
                "learning_rate": [0.05, 0.1, 0.2],
                "n_estimators": [100, 150, 200],
                "reg_alpha": [0, 0.1, 1],
                "reg_lambda": [1, 1.5, 2]
            }
        },
        {
            "name": "SWING (Voyager)",
            "target": "target_swing",
            "file": "swing_model.joblib",
            "csv_path": "train_swing_trend_v2.csv",
            "search_space": {
                "max_depth": [5, 6, 7, 8],
                "learning_rate": [0.01, 0.03, 0.05],
                "n_estimators": [200, 250, 300],
                "reg_alpha": [0.1, 1, 5],
                "reg_lambda": [1, 2, 5]
            }
        }
    ]

    for strat in strategies:
        print(f"\n{'='*10} Training {strat['name']} {'='*10}")
        
        print(f"📂 Loading data from {strat['csv_path']}...")
        df = pd.read_csv(strat['csv_path'])
        df = df.dropna()
        
        X = df[feature_cols]
        y = df[strat['target']]

        print(f"📊 Training on {len(df)} rows...")

        # Reset to natural dataset imbalance for true probability curve
        pos_weight = (len(y) - sum(y)) / sum(y) if sum(y) > 0 else 1

        # Split into Train, Validation, and Test
        X_train_full, X_test, y_train_full, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        X_train, X_val, y_train, y_val = train_test_split(X_train_full, y_train_full, test_size=0.1, random_state=42)

        # 1. Randomized Search - MAX CPU UTILIZATION
        print(f"🔎 Tuning Hyperparameters (Using all CPU cores)...")
        base_xgb = XGBClassifier(
            objective='binary:logistic',
            scale_pos_weight=pos_weight,
            eval_metric='logloss',
            n_jobs=-1  # Use all cores for individual model training
        )

        # F2 scoring values recall higher than precision, forcing aggressive tree structures
        ftwo_scorer = make_scorer(fbeta_score, beta=2)
        
        search = RandomizedSearchCV(
            base_xgb, 
            param_distributions=strat['search_space'], 
            n_iter=10, 
            cv=3, 
            scoring=ftwo_scorer, 
            random_state=42,
            n_jobs=-1  # Use all cores for parallelizing the search iterations
        )
        search.fit(X_train, y_train)
        best_model = search.best_estimator_
        print(f"✨ Best Params: {search.best_params_}")

        # 2. Re-fit with Early Stopping - API FIX
        print(f"🚀 Fitting with Early Stopping...")
        
        # We must set early_stopping_rounds here for modern XGBoost versions
        best_model.set_params(early_stopping_rounds=10, eval_metric='logloss')
        
        best_model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )

        # 3. Use Raw Model (Calibration Nuked to prevent over-smoothing)
        # We skip CalibratedClassifierCV and use raw tree probabilities instead.

        # 4. Manual Threshold Sweep
        print(f"\n🎯 Manual Threshold Sweep ({strat['name']}):")
        print(f"{'Threshold':<10} | {'Win Rate (Prec.)':<16} | {'Trades Caught (Rec.)':<18}")
        print("-" * 52)
        
        y_probs = best_model.predict_proba(X_test)[:, 1]
        from sklearn.metrics import precision_score, recall_score
        
        for thresh in np.arange(0.50, 0.96, 0.02):
            y_pred_sweep = (y_probs >= thresh).astype(int)
            prec = precision_score(y_test, y_pred_sweep, pos_label=1, zero_division=0)
            rec = recall_score(y_test, y_pred_sweep, pos_label=1, zero_division=0)
            
            print(f"{thresh:<10.2f} | {prec*100:<15.1f}% | {rec*100:<17.1f}%")
        
        # Default to 0.5 for the saved model behavior or use a placeholder
        y_pred_final = (y_probs >= 0.5).astype(int)
        print(f"\n✅ {strat['name']} (at 0.50) Accuracy: {accuracy_score(y_test, y_pred_final):.2%}")

        # Save the Raw XGBoost Model
        joblib.dump(best_model, strat['file'])
        print(f"💾 Saved as {strat['file']}")
        
if __name__ == "__main__":
    train_dual_strategy()