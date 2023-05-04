from flask import Blueprint, jsonify
import requests

main = Blueprint('orderBook', __name__)

symbol = '1000SHIBUSDT'
url = 'https://fapi.binance.com/fapi/v1/depth?symbol=' + symbol + '&limit=' + '5000'
data = requests.get(url).json()
print(data['bids'])

@main.route('/')
def get_orderBook():
    return jsonify({'message': "Cualquiercosa"})
