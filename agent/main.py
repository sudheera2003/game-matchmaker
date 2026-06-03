import math
import os
import subprocess
import time
import warnings

import pandas as pd
from dotenv import load_dotenv
from prophet import Prophet
from pymongo import MongoClient

# suppress minor Prophet warnings
warnings.filterwarnings("ignore")


def generate_traffic_forecast():
    load_dotenv()
    uri = os.getenv("ATLAS_URI")
    client = MongoClient(uri)

    # access the active sessions collection
    collection = client.matchmaker.active_sessions

    print("Fetching player matchmaking data from Atlas...")

    # extract only the created_at timestamps
    cursor = collection.find({}, {"_id": 0, "created_at": 1})
    timestamps = [doc["created_at"] for doc in cursor if "created_at" in doc]

    if len(timestamps) < 5:
        print(
            "Not enough data to run an accurate AI forecast. Waiting for more players..."
        )
        return

    # format the data for Prophet
    df = pd.DataFrame(timestamps, columns=["ds"])

    # Strip timezone info
    df["ds"] = df["ds"].dt.tz_localize(None)

    # Group by minute to calculate traffic volume per minute
    df = df.groupby(df["ds"].dt.floor("min")).size().reset_index(name="y")

    print(f"Loaded {len(df)} time segments. Training Prophet AI Model...")

    # Initialize and Train the Model
    m = Prophet(changepoint_prior_scale=0.1)
    m.fit(df)

    # Predict the next 10 minutes
    future = m.make_future_dataframe(periods=10, freq="min")
    forecast = m.predict(future)

    print("\n=== AI SERVER TRAFFIC FORECAST ===")

    # Grab the last 10 rows
    predictions = forecast[["ds", "yhat"]].tail(10)

    for index, row in predictions.iterrows():
        time_str = row["ds"].strftime("%H:%M")
        predicted_players = max(0, int(row["yhat"]))
        print(f"Time: {time_str} | Expected Traffic: {predicted_players} players/min")

    # agent engine
    max_spike = int(predictions["yhat"].max())
    print(f"\n[AI ANALYSIS] Peak traffic of {max_spike} players/min predicted.")

    # calculate exact infrastructure needs based on AI prediction
    target_gateways = max(1, math.ceil(max_spike / 5))
    target_workers = max(1, math.ceil(max_spike / 10))

    print(
        f"Agent Action: Synchronizing infrastructure to {target_gateways} Gateways and {target_workers} Workers..."
    )

    try:
        # inject the calculated variables directly into the Docker command.
        result = subprocess.run(
            [
                "docker-compose",
                "up",
                "-d",
                "--scale",
                f"gateway={target_gateways}",
                "--scale",
                f"matchmaker={target_workers}",
            ],
            cwd="..",
            capture_output=True,
            text=True,
            check=True,
        )
        print("Infrastructure is perfectly balanced.")

    except subprocess.CalledProcessError as e:
        print("\nCould not execute the Docker command.")
        print(e.stderr)


if __name__ == "__main__":
    print("Starting in background mode...")

    # infinite event loop
    while True:
        print("\n" + "=" * 40)
        print("--- RUNNING AI FORECAST CYCLE ---")
        print("=" * 40)

        try:
            # Run the prediction and scaling logic
            generate_traffic_forecast()
        except Exception as e:
            # If the database drops connection, catch the error
            print(f"\nAgent encountered an unexpected error: {e}")

        # Go to sleep for exactly 60 seconds
        print("\nCycle complete. Sleeping for 60 seconds...")
        time.sleep(60)
