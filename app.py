from flask import Flask, jsonify
from flask_cors import CORS
import pickle
import psycopg2

# Conexión a la base de datos
# conn = psycopg2.connect(
#     dbname="railway",
#     user="postgres",
#     password="U8ZYQKMJHgNfzR5ITo9u",
#     host="containers-us-west-129.railway.app",
#     port="7304",
# )

DATABASE_URL = "postgres://bbjjpnee:3PpJnkfCnQ6jexFZe9gW-DZ0_r83WtTu@mahmud.db.elephantsql.com/bbjjpnee"
conn = psycopg2.connect(DATABASE_URL, sslmode='require')

# Cursor para ejecutar comandos SQL
cursor = conn.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, book bytea)")
conn.commit()
cursor.close()

################################################################################################################


def create_app():
    app = Flask(__name__)
    CORS(app)

    @app.route("/")
    def index():
        return "Welcome to the orderBook API!"

    @app.route('/order-book')
    def order_book():
        # Consulta a la tabla
        cursor = conn.cursor()
        cursor.execute("SELECT book FROM orders WHERE id=1")
        result = cursor.fetchone()
        if (result is not None):
            serialized_book = result[0]
            # Deserialización del objeto
            order_book = pickle.loads(serialized_book)
            cursor.close()
            return jsonify({'orderBook': order_book})
        else:
            cursor.close()
            return jsonify({'orderBook': []})

    return app
