import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split

def train_specialist(csv_file, target_column, drop_columns, output_json):
    print(f"\n📊 Loading {csv_file}...")
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"❌ Could not find {csv_file}. Skipping.")
        return

    print(f"🧹 Cleaning data for {target_column}...")
    # Drop timestamps, symbols, and the OTHER target column so it doesn't cheat
    X = df.drop(columns=drop_columns)
    y = df[target_column]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print(f"🧠 Training the {output_json} model...")
    model = xgb.XGBClassifier(n_estimators=100, learning_rate=0.1, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    accuracy = model.score(X_test, y_test)
    print(f"🎯 {output_json} Accuracy: {accuracy * 100:.2f}%")

    model.save_model(output_json)
    print(f"💾 Saved {output_json} successfully!")

# --- EXECUTE BOTH TRAININGS ---

# 1. Train the Intraday Specialist
train_specialist(
    csv_file='train_intraday_30m_v2.csv', # CHANGE THIS if your file is named differently
    target_column='target_intraday',
    drop_columns=['symbol', 'timestamp', 'target_intraday', 'target_swing'], 
    output_json='model_intraday.json'
)

# 2. Train the Swing Specialist
train_specialist(
    csv_file='train_swing_trend_v2.csv',    # CHANGE THIS if your file is named differently
    target_column='target_swing',
    drop_columns=['symbol', 'timestamp', 'target_intraday', 'target_swing'],
    output_json='model_swing.json'
)

print("\n✅ DONE! Upload BOTH model_intraday.json and model_swing.json to GitHub.")