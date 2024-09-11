import psycopg2
import os
import dotenv

dotenv.load_dotenv()

# Database connection details
db_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_NAME"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

test_socket_symbols = [
    "SOLUSDT",
    "DOGEUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "LINKUSDT",
    "FILUSDT",
    "NEARUSDT",
    "ADAUSDT",
    "WLDUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "WIFUSDT",
    "SUIUSDT",
    "BCHUSDT",
    "ETCUSDT",
    "GALAUSDT",
    "DOTUSDT",
    "APTUSDT",
    "FETUSDT",
    "UNIUSDT",
    "FTMUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "STXUSDT",
    "RUNEUSDT",
    "BLURUSDT",
    "MKRUSDT",
    "SANDUSDT",
    "ARKMUSDT",
    "ATOMUSDT",
    "GMTUSDT",
    "THETAUSDT",
    "AXSUSDT",
    "EOSUSDT",
    "MINAUSDT",
    "LDOUSDT",
    "MASKUSDT",
    "MEMEUSDT",
    "CFXUSDT",
    "CRVUSDT",
    "APEUSDT",
    "TRBUSDT",
    "XLMUSDT",
    "AAVEUSDT",
    "GRTUSDT",
    "ENAUSDT",
    "WIFUSDT",
    "PEPEUSDT",
    "ICPUSDT",
    "PENDLEUSDT",
    "COSUSDT",
    "YGGUSDT",
    "SUIUSDT",
    "OPUSDT",
    "RNDRUSDT",
    "INJUSDT",
]
ticker_colors = [
    "#4DFF4D",
    "#B9B4C7",
    "#6243B6",
    "#00B3B3",
    "#9A3B3B",
    "#ABC4AA",
    "#B30000",
    "#6B7F4F",
    "#C08261",
    "#A78295",
    "#FFB3B3",
    "#84A7A1",
    "#03C988",
    "#892CDC",
    "#63b598",
    "#ce7d78",
    "#ea9e70",
    "#a48a9e",
    "#c6e1e8",
    "#648177",
    "#0d5ac1",
    "#f205e6",
    "#1c0365",
    "#14a9ad",
    "#4ca2f9",
    "#a4e43f",
    "#d298e2",
    "#6119d0",
    "#d2737d",
    "#c0a43c",
    "#f2510e",
    "#651be6",
    "#79806e",
    "#61da5e",
    "#cd2f00",
    "#9348af",
    "#01ac53",
    "#c5a4fb",
    "#996635",
    "#b11573",
    "#4bb473",
    "#75d89e",
    "#2f3f94",
    "#2f7b99",
    "#da967d",
    "#34891f",
    "#b0d87b",
    "#ca4751",
    "#7e50a8",
    "#c4d647",
    "#e0eeb8",
    "#11dec1",
    "#289812",
    "#566ca0",
    "#ffdbe1",
]


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
