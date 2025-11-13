import polars as pl
from datetime import datetime
import os


def main():
    # === Load dataset ===
    csv_path = "data/202306-divvy-tripdata.csv"

    # Read CSV lazily (efficient for large files)
    pl_df = pl.scan_csv(csv_path, schema_overrides={"end_station_id": pl.Utf8})

    # === Parse datetime columns ===
    pl_df = pl_df.with_columns([
        pl.col("started_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"),
        pl.col("ended_at").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S"),
    ])

    # === Compute daily ride counts ===
    daily_counts = (
        pl_df
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.len().alias("ride_count"))
        .sort("date")
    )

    print("\n=== Daily Ride Counts ===")
    print(daily_counts.collect().head())

    # === Compute weekly ride stats ===
    weekly_stats = (
        pl_df.sort("started_at")
        .group_by_dynamic("started_at", every="1w", closed="left")
        .agg(pl.len().alias("ride_count"))
        .select([
            pl.col("ride_count").mean().alias("avg_rides_per_week"),
            pl.col("ride_count").max().alias("max_rides_per_week"),
            pl.col("ride_count").min().alias("min_rides_per_week"),
        ])
    )

    print("\n=== Weekly Ride Statistics ===")
    print(weekly_stats.collect())

    # === Compare rides with last week ===
    first_last_ride = daily_counts.with_columns([
        pl.col("ride_count").cast(pl.Int32).alias("ride_count"),
        pl.col("ride_count").cast(pl.Int32).shift(7).alias("last_week_ride_count"),
        (
            (pl.col("ride_count").cast(pl.Int32) - pl.col("ride_count").cast(pl.Int32).shift(7))
            .fill_null(0)
        ).alias("diff_from_last_week")
    ])

    print("\n=== Daily Ride Comparison with Last Week ===")
    print(first_last_ride.collect())

    # === Save reports with timestamp ===
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = "reports"
    os.makedirs(reports_dir, exist_ok=True)

    # Save all outputs separately (must collect first)
    daily_path = f"{reports_dir}/daily_counts_{timestamp}.csv"
    weekly_path = f"{reports_dir}/weekly_stats_{timestamp}.csv"
    compare_path = f"{reports_dir}/ride_diff_{timestamp}.csv"

    daily_counts.collect().write_csv(daily_path)
    weekly_stats.collect().write_csv(weekly_path)
    first_last_ride.collect().write_csv(compare_path)

    print("\nReports generated:")
    print(f" - Daily counts:   {daily_path}")
    print(f" - Weekly stats:   {weekly_path}")
    print(f" - Ride comparison:{compare_path}")


if __name__ == "__main__":
    main()
