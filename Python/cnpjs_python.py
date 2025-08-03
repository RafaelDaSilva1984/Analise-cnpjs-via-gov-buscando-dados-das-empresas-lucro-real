# consulta_cnpjs.py
# Script Principal para consultar dados de CNPJs via API e salvar resultados em Excel
# Autor: [21rfSoftwares]

import requests
import time
import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# === Caminhos dos arquivos ===
ARQUIVO_ENTRADA = r'input_data/30-07-2025 initial  05407709000120.csv'
ARQUIVO_SAIDA = r'output_data_cnae/cnpj_limpos_.xlsx'
CAMINHO_ERROS = r'Erros/erros.xlsx'
CHECKPOINT_FILE = 'Checkpoint/checkpoint.txt'

def limpar_cnpj(cnpj):
    ''
    cnpj_str = str(cnpj).zfill(14)
    return ''.join(filter(str.isdigit, cnpj_str))

def formatar_cnpj(cnpj):
    cnpj = ''.join(filter(str.isdigit, str(cnpj)))
    cnpj = cnpj.zfill(14)
    return f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}'

def ler_cnpjs(caminho):
    df = pd.read_csv(caminho)
    cnpjs = df.iloc[:, 1].tolist()
    return [limpar_cnpj(c) for c in cnpjs]

def ler_cnpjs_processados(caminho_saida):
    if not os.path.exists(caminho_saida):
        return set()
    df = pd.read_excel(caminho_saida)
    return set(df['cnpj'].astype(str).str.zfill(14))

def registrar_erro_em_excel(cnpj, erro_msg):
    df_erro = pd.DataFrame([{
        'cnpj': formatar_cnpj(cnpj),
        'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'erro': erro_msg
    }])
    if os.path.exists(CAMINHO_ERROS):
        df_existente = pd.read_excel(CAMINHO_ERROS)
        df_completo = pd.concat([df_existente, df_erro], ignore_index=True)
    else:
        df_completo = df_erro
    df_completo.to_excel(CAMINHO_ERROS, index=False)

def consultar_cnpj(cnpj):
    url = f'https://publica.cnpj.ws/cnpj/{cnpj}'
    tentativas = 0
    link_cnpj = f'https://cnpj.biz/{str(cnpj).zfill(14)}'
    while tentativas < 3:
        try:
            response = requests.get(url, timeout=35)
            if response.status_code == 404:
                print(f'[404] CNPJ não encontrado: {formatar_cnpj(cnpj)}')
                registrar_erro_em_excel(cnpj, "CNPJ não encontrado (404)")
                break
            if response.status_code == 429:
                time.sleep(15) #anterior 25
                tentativas += 1
                continue
            response.raise_for_status()
            data = response.json()
            email_extraido = data.get('estabelecimento', {}).get('email', 'N/D')
            email_formatado = 'e-mail não cadastrado' if email_extraido in ['N/D', 'Erro', 'Timeout', '', None] else email_extraido
            return {
                'cnpj': formatar_cnpj(cnpj),
                'nome': data.get('razao_social', 'N/D'),
                'uf': data.get('estabelecimento', {}).get('estado', {}).get('sigla', 'N/D'),
                'cidade': data.get('estabelecimento', {}).get('cidade', {}).get('nome', 'N/D'),
                'email': email_formatado,
                'link': link_cnpj,
                'cnae_codigo': data.get('estabelecimento', {}).get('atividade_principal', {}).get('subclasse', 'N/D'),
                'cnae_desc': data.get('estabelecimento', {}).get('atividade_principal', {}).get('descricao', 'N/D'),
                'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except requests.exceptions.RequestException as e:
            print(f'[ERRO] {formatar_cnpj(cnpj)} - {e}')
            registrar_erro_em_excel(cnpj, str(e))
            tentativas += 1
            time.sleep(13) #anterior 15

    print(f'[ERRO] Falha após 3 tentativas: {formatar_cnpj(cnpj)}')
    registrar_erro_em_excel(cnpj, "Falha após 3 tentativas ou erro desconhecido")
    return {
        'cnpj': formatar_cnpj(cnpj),
        'nome': 'Erro',
        'uf': 'Erro',
        'cidade': 'Erro',
        'email': 'e-mail não cadastrado',
        'link': link_cnpj,
        'cnae_codigo': 'Erro',
        'cnae_desc': 'Erro',
        'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def salvar_excel(lista_dados, caminho_saida):
    df_novo = pd.DataFrame(lista_dados)
    colunas_ordenadas = [
        'cnpj', 'nome', 'uf', 'cidade', 'email',
        'link', 'cnae_codigo', 'cnae_desc', 'data_hora'
    ]
    df_novo = df_novo[colunas_ordenadas]
    if os.path.exists(caminho_saida):
        df_existente = pd.read_excel(caminho_saida)
        df_existente['cnpj'] = df_existente['cnpj'].astype(str).str.zfill(14)
        df_existente = df_existente[colunas_ordenadas]
        df_completo = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_completo = df_novo
    df_completo.drop_duplicates(subset='cnpj', inplace=True)
    df_completo.to_excel(caminho_saida, index=False)

def salvar_checkpoint(cnpj):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(cnpj)

def carregar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return f.read().strip()
    return None

def remover_duplicatas_final():
    caminho_entrada = ARQUIVO_SAIDA
    caminho_saida = caminho_entrada.replace('.xlsx', '_sem_duplicatas.xlsx')
    if not os.path.exists(caminho_entrada):
        print(f'[ERRO] Arquivo {caminho_entrada} não encontrado.')
        return
    try:
        df = pd.read_excel(caminho_entrada)
        df_sem_duplicatas = df.drop_duplicates(subset=[
            'cnpj', 'nome', 'uf', 'cidade', 'email', 'link', 'cnae_codigo', 'cnae_desc'
        ])
        df_sem_duplicatas.to_excel(caminho_saida, index=False)
        print(f'[OK] Duplicatas removidas. Resultado salvo em: {caminho_saida}')
    except Exception as e:
        print(f'[ERRO] Falha ao remover duplicatas: {e}')

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
    contador_para_remover_duplicatas = 0

    for i, cnpj in enumerate(tqdm(novos_cnpjs, desc="Consultando CNPJs", unit="cnpj"), 1):
        dados = consultar_cnpj(cnpj)
        resultados.append(dados)
        salvar_checkpoint(cnpj)
        contador_para_remover_duplicatas += 1

        if i % 5 == 0 or i == len(novos_cnpjs):
            salvar_excel(resultados, ARQUIVO_SAIDA)
            resultados = []

        if contador_para_remover_duplicatas >= 10:
            remover_duplicatas_final()
            contador_para_remover_duplicatas = 0

        time.sleep(14) #anterior 21

if __name__ == '__main__':
    localizar_dados_lucro_real()
    remover_duplicatas_final()
