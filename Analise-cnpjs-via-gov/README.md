# Análise de CNPJs via API Pública

Este projeto automatiza a consulta de CNPJs utilizando a API `publica.cnpj.ws`, salvando os dados em planilhas Excel com verificação de duplicatas.

## Requisitos

- Python 3.9+
- `pip install -r requirements.txt`

## Como usar

1. Coloque seus CNPJs em `input_data/cnpj_limpos.csv`
2. Execute:
   ```bash
   python main.py
   ```

## Funcionalidades

- Consulta dados públicos por CNPJ
- Registro de erros
- Checkpoint automático
- Remove duplicatas a cada 150 registros

## Licença

MIT - Livre para uso e modificação.
