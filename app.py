from flask import Flask, jsonify
import json
import websocket
import requests
import pandas as pd
import sqlite3
import pickle
from multiprocessing import Process
import psycopg2

# Conexión a la base de datos
conn = psycopg2.connect(
    dbname="myorderbook",
    user="ansefe",
    password="jKDBjuLmu3GnzICLX1FyGPJhbsBmfG4J",
    host="dpg-chatdlu7avjcvo2h9vvg-a",
    port="5432",
)

# Cursor para ejecutar comandos SQL
cursor = conn.cursor()
cursor.execute("DROP TABLE orders")
cursor.execute('''CREATE TABLE orders
                  (id INTEGER PRIMARY KEY, 
                   book BLOB)''')
print('general',cursor)


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
    cursor.execute("INSERT OR REPLACE INTO orders (id, book) VALUES (?, ?) ", ('1', serialized_book,))
    # Confirmar cambios
    conn.commit()


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

################################################################################################################
def create_app():
    conn = sqlite3.connect('myOrderBook.db', check_same_thread=False)
    cursor = conn.cursor()
    app = Flask(__name__)
    @app.route("/")
    def index():
        return "Hello, world!"

    @app.route('/order-book')
    def order_book():
        # Consulta a la tabla
        cursor.execute("SELECT book FROM orders WHERE id=1")
        print('appCursor',cursor)

        # Recuperar el objeto serializado
        print(cursor.fetchone())
        serialized_book = cursor.fetchone()[0]

        # Deserialización del objeto
        order_book = pickle.loads(serialized_book)
        return jsonify({'orderBook': order_book})
    
    return app
