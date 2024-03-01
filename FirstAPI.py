import requests
import json


cotacoe = requests.get(
    ' https://economia.awesomeapi.com.br/last/USD-BRL,EUR-BRL,BTC-BRL')
cotacoe = cotacoe.json()
cotacao_dolar = cotacoe['USDBRL']['bid']

print(cotacao_dolar)
