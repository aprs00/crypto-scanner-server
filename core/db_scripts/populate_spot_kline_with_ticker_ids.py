import os
import psycopg2
import dotenv

dotenv.load_dotenv()

from exchange_connections.constants import test_socket_symbols, ticker_colors

db_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_NAME"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}


def insert_data(tickers, colors):
    global conn, cursor

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Loop through tickers and colors to insert into the table
        for ticker, color in zip(tickers, colors):
            cursor.execute(
                "INSERT INTO crypto_scanner_binance_spot_tickers (name, color) VALUES (%s, %s);",
                (ticker[:-4], color),
            )

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data to PostgreSQL:", error)

    finally:
        # Close database connection
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")


# Call the function to insert data
if __name__ == "__main__":
    insert_data(test_socket_symbols, ticker_colors)
