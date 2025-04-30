import requests

weather_url = "https://wttr.in/Belo+Horizonte"

try:
    response = requests.get(weather_url)
    data = response.text
    if response.status_code == 200:
        print(data)
    else:
        print("Não foi possível obter os dados de previsão do tempo.")
except Exception as e:
    print(f"Ocorreu um erro ao acessar a API: {e}")
