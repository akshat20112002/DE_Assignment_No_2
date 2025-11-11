from pathlib import Path

def save_step(df, step_name):
    Path("reports").mkdir(exist_ok=True)
    path = f"reports/{step_name}"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    print(f"Saved: {path}")