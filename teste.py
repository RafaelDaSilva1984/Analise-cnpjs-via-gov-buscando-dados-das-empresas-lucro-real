import requests

cnpj = '05940084000167'
url = f'https://publica.cnpj.ws/cnpj/{cnpj}'

try:
    response = requests.get(url, timeout=15)
    print(f"Status: {response.status_code}")
    print(response.json())
except Exception as e:
    print(f"[ERRO] {e}")

