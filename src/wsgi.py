from app import create_app, process_message, on_open, on_close, on_error
from multiprocessing import Process
import websocket
import gevent.monkey
gevent.monkey.patch_all()

symbol = "BTCUSDT"
ws_url = f"wss://stream.binance.com:443/ws/{symbol}@depth"

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
    app = create_app()
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