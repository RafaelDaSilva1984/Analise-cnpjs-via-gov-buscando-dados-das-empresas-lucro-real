=# Análise de CNPJs - Busca via API Pública

Projeto em Python para consultar dados de CNPJs usando uma API pública do governo.  
Salva resultados em Excel, com tratamento de erros e checkpoint para continuidade.

## Como usar

1. Coloque seu arquivo CSV com CNPJs na pasta `input_data` (ou ajuste o caminho no script).  
2. Execute o script Python principal.  
3. Os resultados serão salvos na pasta `output_data_cnae`.  
4. Veja os erros no arquivo `erros.xlsx`.

## Instalar dependências

Use o `requirements.txt` para instalar todas as bibliotecas necessárias:

```bash
pip install -r requirements.txt

Tecnologias usadas
Python 3

Requests

Pandas

Openpyxl

tqdm

API pública de CNPJ

Como contribuir
Abra issues ou envie pull requests.





