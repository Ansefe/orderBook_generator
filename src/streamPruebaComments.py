import json
import websocket
import requests
import pandas as pd
import matplotlib.pyplot as plt

# define las variables necesarias
symbol = "BTCUSDT"
depth_url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=1"
ws_url = f"wss://stream.binance.com:443/ws/{symbol}@depth"
last_update_id = None
orderBook = []
snapshot = {}
buffer = []
isFirstEvent = True

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
    global snapshot, orderBook, isFirstEvent, buffer
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
    print(df)
    # Graficar los datos
    # plt.bar(df['Price'], df['Quantity'])
    # plt.xlabel('Price')
    # plt.ylabel('Quantity')
    # plt.show()
    # print(json.dumps(message['u'], indent=4))
    order_book_json = {
    'orderBook': df.values.tolist()
    }
    print(order_book_json)


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


# establece la conexión al websocket
if __name__ == "__main__":
    ws = websocket.WebSocketApp(ws_url,
                                on_message=process_message,
                                on_open=on_open,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
