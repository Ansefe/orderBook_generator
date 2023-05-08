from flask import Flask
import json
import requests
import pandas as pd
import pickle
from multiprocessing import Process
import psycopg2

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname="railway",
    user="postgres",
    password="U8ZYQKMJHgNfzR5ITo9u",
    host="containers-us-west-129.railway.app",
    port="7304",
)

# Cursor para ejecutar comandos SQL
cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, book bytea)")
conn.commit()
cursor.close()

# app = Flask(__name__)

# define las variables necesarias
symbol = "BTCUSDT"
depth_url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5000"
ws_url = f"wss://stream.binance.com:443/ws/{symbol}@depth"
last_update_id = None
orderBook = []
snapshot = {}
buffer = []
isFirstEvent = True
order_book_json = {
    'orderBook': []
}

# función para procesar los datos


def replaceOrderBook(message, orderBook):
    bids = message['b']
    asks = message['a']

    # print('orderBook \n', orderBook)
    for bid in bids[:5]:
        for buff in orderBook:
            if buff[0] == bid[0]:
                buff[1] = bid[1]
                break
        else:
            orderBook.append(bid)
            
    for ask in asks[:5]:
        for buff in orderBook:
            if buff[0] == ask[0]:
                buff[1] = ask[1]
                break
        else:
            orderBook.append(ask)


def process_message(ws, message):
    global snapshot, orderBook, isFirstEvent, buffer, order_book_json
    message = json.loads(message)
    cursor = conn.cursor()
    
    if message['u'] > snapshot['lastUpdateId']:
        
        if (message['U'] <= (snapshot['lastUpdateId'] + 1)) and (message['u'] >= (snapshot['lastUpdateId'] + 1)) and isFirstEvent:
            isFirstEvent = False
            replaceOrderBook(message, orderBook)
        elif not isFirstEvent and (message['U'] == (buffer['u'] + 1)):
            replaceOrderBook(message, orderBook)
    buffer = message
    # Crear un DataFrame de Pandas con los datos del libro de órdenes
    df = pd.DataFrame(orderBook, columns=['Price', 'Quantity'])
    df['Price'] = pd.to_numeric(df['Price'])
    df['Quantity'] = pd.to_numeric(df['Quantity'])
    # Ordenar los datos por el precio
    df = df.sort_values('Price')
    # eliminar filas donde Quantity es 0
    df = df[df['Quantity'] != 0]
    serialized_book = pickle.dumps(df.values.tolist())
    # Inserción en la tabla
    cursor.execute("INSERT INTO orders (id, book) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET book = EXCLUDED.book", ('1', serialized_book,))
    # Confirmar cambios
    conn.commit()
    cursor.close()
    print('Obtuve')


def on_error(ws, error):
    print(error)
def on_close(ws):
    print("Connection closed")
def on_open(ws):
    global snapshot, orderBook
    print("Connection opened")
    snapshot = requests.get(depth_url).json()
    orderBook = snapshot['bids'] + snapshot['asks']
    payload = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@depth"
        ],
        "id": 1
    }
    ws.send(json.dumps(payload))

def create_app():
    app = Flask(__name__)

    @app.route("/")
    def index():
        return "Websocket App!"

    return app
