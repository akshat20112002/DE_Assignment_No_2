import duckdb
db_path = "cars.duckdb"
csv_file = "./data/*.csv"

def get_connection():
    return duckdb.connect(db_path)

def create_table():
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS DuckDB (
        vin VARCHAR,
        county VARCHAR,
        city VARCHAR,
        state VARCHAR(2),
        postal_code VARCHAR,
        model_year INTEGER,
        make VARCHAR,
        model VARCHAR,
        electric_vehicle_type VARCHAR,
        cafv_eligibility VARCHAR,
        electric_range INTEGER,
        base_msrp INTEGER,
        legislative_district VARCHAR,
        dol_vehicle_id BIGINT,
        vehicle_location VARCHAR,
        electric_utility VARCHAR,
        census_tract_2020 BIGINT
        );
        """
    )
    conn.close()

def load_csv():
    conn = get_connection()
    conn.execute(f"""
    COPY DuckDB FROM '{csv_file}'
    (HEADER TRUE);
    """)

    conn.close()

