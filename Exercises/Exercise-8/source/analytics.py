from source.database import get_connection
import os
def queries():
    os.makedirs("Output", exist_ok=True)
    conn = get_connection()
    
    # Count the number of electric cars per city.
    df1 = conn.execute("select city, count(*) as counts_cars from DuckDB group by city order by counts_cars desc;").df()
    df1.to_csv("Output/cars_per_city.csv", index=True)

    # Find the top 3 most popular electric vehicles.
    df2 = conn.execute("select make, model, count(*) as counts from DuckDB group by make, model order by counts desc limit 3;").df()
    df2.to_csv("Output/most_popular_cars.csv", index = True)

    # Find the most popular electric vehicle in each postal code.
    df3 = conn.execute("with pop_ev as (select postal_code, make, model, row_number() over (partition by postal_code order by count(*) desc) as ranking from DuckDB group by postal_code, make, model) select postal_code, make, model from pop_ev where ranking = 1").df()
    df3.to_csv("Output/popular_ev_per_code.csv", index= True)

    #model year parquet stays same
    conn.execute(
        """
        COPY (
            SELECT model_year, count(*) AS counting
            FROM DuckDB GROUP BY model_year
        )
        TO 'Output/model_year.parquet'
        (FORMAT PARQUET, PARTITION_BY (model_year), OVERWRITE_OR_IGNORE TRUE);
        """
    )


    conn.close()