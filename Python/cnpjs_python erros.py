import pandas as pd
import requests
import time
import re
from datetime import datetime
from pathlib import Path

# --- Configurações ---
ARQUIVO_CNPJS = "Erros/erros.xlsx"
PASTA_SAIDA = "output_data_cnae"
ARQUIVO_CHECKPOINT = "checkpoint_erros.txt"
ARQUIVO_SAIDA = f"{PASTA_SAIDA}/resultado_erros_atualizado.xlsx"
URL_API = "https://publica.cnpj.ws/cnpj/{}"
TEMPO_ENTRE_REQUISICOES = 15  # segundos
MAX_TENTATIVAS = 3

# --- Funções auxiliares ---
def limpar_cnpj(cnpj):
    return re.sub(r'\D', '', str(cnpj))

def carregar_checkpoint(caminho):
    try:
        cnpj = Path(caminho).read_text().strip()
        return limpar_cnpj(cnpj)
    except FileNotFoundError:
        print("[AVISO] Checkpoint não encontrado. Começando do início.")
        return None

def salvar_checkpoint(cnpj, caminho):
    Path(caminho).write_text(cnpj)

def consultar_api(cnpj):
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            resposta = requests.get(URL_API.format(cnpj), timeout=10)
            resposta.raise_for_status()
            return resposta.json(), None
        except Exception as e:
            erro = str(e)
            print(f"[ERRO] Tentativa {tentativa} falhou para {cnpj}: {erro}")
            time.sleep(12)
    return None, erro

# --- Script principal ---
def main():
    print("[INFO] Lendo CNPJs do arquivo:", ARQUIVO_CNPJS)
    df = pd.read_excel(ARQUIVO_CNPJS)
    df['cnpj'] = df['cnpj'].apply(limpar_cnpj)
    df = df.dropna(subset=['cnpj']).drop_duplicates(subset=['cnpj'])

    lista_cnpjs = df['cnpj'].tolist()
    checkpoint = carregar_checkpoint(ARQUIVO_CHECKPOINT)

    if checkpoint and checkpoint in lista_cnpjs:
        index_inicio = lista_cnpjs.index(checkpoint) + 1
        print(f"[INFO] Retomando a partir do CNPJ {checkpoint} (posição {index_inicio})")
    else:
        index_inicio = 0
        if checkpoint:
            print(f"[AVISO] Checkpoint '{checkpoint}' não encontrado na lista de CNPJs.")

    resultados = []

    # Cria pasta de saída, se não existir
    Path(PASTA_SAIDA).mkdir(parents=True, exist_ok=True)

    for i, cnpj in enumerate(lista_cnpjs[index_inicio:], start=index_inicio):
        print(f"[{i+1}/{len(lista_cnpjs)}] Consultando CNPJ: {cnpj}")
        dados, erro = consultar_api(cnpj)

        resultado = {
            "cnpj": cnpj,
            "nome": dados.get("razao_social") if dados else None,
            "uf": dados.get("estabelecimento", {}).get("estado", {}).get("sigla") if dados else None,
            "cidade": dados.get("estabelecimento", {}).get("cidade", {}).get("nome") if dados else None,
            "email": dados.get("estabelecimento", {}).get("email") if dados else None,
            "link": dados.get("estabelecimento", {}).get("site") if dados else None,
            "cnae_codigo": dados.get("estabelecimento", {}).get("atividade_principal", {}).get("subclasse") if dados else None,
            "cnae_desc": dados.get("estabelecimento", {}).get("atividade_principal", {}).get("descricao") if dados else None,
            "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "erro": erro,
        }

        resultados.append(resultado)
        salvar_checkpoint(cnpj, ARQUIVO_CHECKPOINT)

        # Salva a cada 10 registros para não perder dados
        if (i+1) % 10 == 0 or (i+1) == len(lista_cnpjs):
            df_resultado = pd.DataFrame(resultados)
            df_resultado.to_excel(ARQUIVO_SAIDA, index=False)
            print(f"[INFO] Salvo progresso até o registro {i+1}")

        time.sleep(TEMPO_ENTRE_REQUISICOES)

    print("[OK] Processo finalizado. Resultado salvo em:", ARQUIVO_SAIDA)

if __name__ == "__main__":
    main()
