from flask import Flask, jsonify
import json
import websocket
import requests
import pandas as pd
import sqlite3
import pickle
from multiprocessing import Process

# Conexión a la base de datos
conn = sqlite3.connect('myOrderBook.db', check_same_thread=False)

# Cursor para ejecutar comandos SQL
cursor = conn.cursor()
cursor.execute("DROP TABLE orders")
cursor.execute('''CREATE TABLE orders
                  (id INTEGER PRIMARY KEY, 
                   book BLOB)''')

app = Flask(__name__)

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
@app.route('/order-book')
def order_book():
    # Consulta a la tabla
    cursor.execute("SELECT book FROM orders WHERE id=1")

    # Recuperar el objeto serializado
    serialized_book = cursor.fetchone()[0]

    # Deserialización del objeto
    order_book = pickle.loads(serialized_book)
    return jsonify({'orderBook': order_book})

# Define la función para ejecutar la conexión al websocket
def run_websocket():
    ws = websocket.WebSocketApp(ws_url,
                                on_message=process_message,
                                on_open=on_open,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()

# Define la función para ejecutar la aplicación Flask
def run_flask():
    app.run()

if __name__ == "__main__":
    # Inicia un proceso en segundo plano para la conexión al websocket
    websocket_process = Process(target=run_websocket)
    # Inicia un proceso en segundo plano para la aplicación Flask
    flask_process = Process(target=run_flask)
    
    websocket_process.start()
    flask_process.start()
    
    # Espera a que los procesos terminen
    websocket_process.join()
    flask_process.join()
