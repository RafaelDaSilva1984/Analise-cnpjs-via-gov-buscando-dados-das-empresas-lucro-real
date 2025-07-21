from utils import (
    ler_cnpjs, ler_cnpjs_processados, salvar_checkpoint, carregar_checkpoint,
    salvar_excel, consultar_cnpj, remover_duplicatas_final
)
import os
import time
from tqdm import tqdm

ARQUIVO_ENTRADA = 'input_data/cnpj_limpos.csv'
ARQUIVO_SAIDA = 'output_data/resultado_consulta.xlsx'
CHECKPOINT_FILE = 'checkpoint.txt'

def localizar_dados_lucro_real():
    cnpjs = ler_cnpjs(ARQUIVO_ENTRADA)
    cnpjs_processados = ler_cnpjs_processados(ARQUIVO_SAIDA)
    ultimo_checkpoint = carregar_checkpoint()
    if ultimo_checkpoint:
        try:
            index_ultimo = cnpjs.index(ultimo_checkpoint)
            cnpjs = cnpjs[index_ultimo + 1:]
        except ValueError:
            pass

    novos_cnpjs = [c for c in cnpjs if c not in cnpjs_processados]
    resultados = []
    contador_para_remover = 0

    for i, cnpj in enumerate(tqdm(novos_cnpjs, desc="Consultando CNPJs", unit="cnpj"), 1):
        dados = consultar_cnpj(cnpj)
        resultados.append(dados)
        salvar_checkpoint(cnpj)
        contador_para_remover += 1

        if i % 5 == 0 or i == len(novos_cnpjs):
            salvar_excel(resultados, ARQUIVO_SAIDA)
            resultados = []

        if contador_para_remover >= 150:
            remover_duplicatas_final(ARQUIVO_SAIDA)
            contador_para_remover = 0

        time.sleep(21)

if __name__ == '__main__':
    localizar_dados_lucro_real()
