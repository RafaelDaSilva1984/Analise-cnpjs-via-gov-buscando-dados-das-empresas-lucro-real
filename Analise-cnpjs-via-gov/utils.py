import os
import requests
import pandas as pd
from datetime import datetime
import time

CAMINHO_ERROS = 'erros/erros.xlsx'
CHECKPOINT_FILE = 'checkpoint.txt'

def limpar_cnpj(cnpj):
    return ''.join(filter(str.isdigit, str(cnpj).zfill(14)))

def formatar_cnpj(cnpj):
    cnpj = limpar_cnpj(cnpj)
    return f'{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}'

def ler_cnpjs(caminho):
    df = pd.read_csv(caminho)
    return [limpar_cnpj(c) for c in df.iloc[:, 0].tolist()]

def ler_cnpjs_processados(caminho_saida):
    if not os.path.exists(caminho_saida):
        return set()
    df = pd.read_excel(caminho_saida)
    return set(df['cnpj'].astype(str).str.zfill(14))

def registrar_erro_em_excel(cnpj, erro_msg):
    os.makedirs('erros', exist_ok=True)
    df_erro = pd.DataFrame([{
        'cnpj': formatar_cnpj(cnpj),
        'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'erro': erro_msg
    }])
    if os.path.exists(CAMINHO_ERROS):
        df_existente = pd.read_excel(CAMINHO_ERROS)
        df = pd.concat([df_existente, df_erro], ignore_index=True)
    else:
        df = df_erro
    df.to_excel(CAMINHO_ERROS, index=False)

def consultar_cnpj(cnpj):
    url = f'https://publica.cnpj.ws/cnpj/{cnpj}'
    tentativas = 0
    link = f'https://cnpj.biz/{str(cnpj).zfill(14)}'

    while tentativas < 3:
        try:
            r = requests.get(url, timeout=35)
            if r.status_code == 404:
                registrar_erro_em_excel(cnpj, "CNPJ não encontrado (404)")
                break
            if r.status_code == 429:
                time.sleep(25)
                tentativas += 1
                continue
            r.raise_for_status()
            data = r.json()
            email = data.get('estabelecimento', {}).get('email', 'N/D')
            email = 'e-mail não cadastrado' if email in ['N/D', '', None] else email

            return {
                'cnpj': formatar_cnpj(cnpj),
                'nome': data.get('razao_social', 'N/D'),
                'uf': data.get('estabelecimento', {}).get('estado', {}).get('sigla', 'N/D'),
                'cidade': data.get('estabelecimento', {}).get('cidade', {}).get('nome', 'N/D'),
                'email': email,
                'link': link,
                'cnae_codigo': data.get('estabelecimento', {}).get('atividade_principal', {}).get('subclasse', 'N/D'),
                'cnae_desc': data.get('estabelecimento', {}).get('atividade_principal', {}).get('descricao', 'N/D'),
                'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            registrar_erro_em_excel(cnpj, str(e))
            tentativas += 1
            time.sleep(15)

    return {
        'cnpj': formatar_cnpj(cnpj), 'nome': 'Erro', 'uf': 'Erro',
        'cidade': 'Erro', 'email': 'e-mail não cadastrado', 'link': link,
        'cnae_codigo': 'Erro', 'cnae_desc': 'Erro',
        'data_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

def salvar_excel(lista, caminho_saida):
    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
    df_novo = pd.DataFrame(lista)
    cols = ['cnpj', 'nome', 'uf', 'cidade', 'email', 'link', 'cnae_codigo', 'cnae_desc', 'data_hora']
    df_novo = df_novo[cols]
    if os.path.exists(caminho_saida):
        df_existente = pd.read_excel(caminho_saida)
        df_existente['cnpj'] = df_existente['cnpj'].astype(str).str.zfill(14)
        df = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df = df_novo
    df.drop_duplicates(subset='cnpj', inplace=True)
    df.to_excel(caminho_saida, index=False)

def salvar_checkpoint(cnpj):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(cnpj)

def carregar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return f.read().strip()
    return None

def remover_duplicatas_final(caminho_saida):
    if not os.path.exists(caminho_saida):
        print(f'[ERRO] Arquivo {caminho_saida} não encontrado.')
        return
    try:
        df = pd.read_excel(caminho_saida)
        df_sem_duplicatas = df.drop_duplicates(subset=[
            'cnpj', 'nome', 'uf', 'cidade', 'email', 'link', 'cnae_codigo', 'cnae_desc'
        ])
        df_sem_duplicatas.to_excel(caminho_saida, index=False)
        print(f'[OK] Duplicatas removidas.')
    except Exception as e:
        print(f'[ERRO] Falha ao remover duplicatas: {e}')
