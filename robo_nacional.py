import sys
import subprocess
import time
import requests
import pandas as pd
import sqlite3
import unicodedata
import re
from datetime import datetime, timedelta

try:
    import urllib3
    import streamlit as st
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "requests", "streamlit", "urllib3"])
    import urllib3
    import streamlit as st

urllib3.disable_warnings()
st.set_page_config(page_title="Robô Nacional | Varejador", page_icon="🚜", layout="wide")

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

# ==========================================
# 🗄️ NOVO COFRE: BANCO NACIONAL
# ==========================================
def conectar_banco_nacional():
    caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'banco_nacional.db')
    conn = sqlite3.connect(caminho, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS itens_nacionais (
            id_item TEXT UNIQUE,
            estado TEXT,
            orgao TEXT,
            municipio TEXT,
            data_assinatura TEXT,
            descricao_item TEXT,
            unid_medida TEXT,
            valor_unitario REAL,
            credor TEXT,
            origem TEXT,
            link_pncp TEXT
        )
    ''')
    conn.commit()
    return conn

def contar_itens_nacionais():
    conn = conectar_banco_nacional()
    try: res = conn.execute("SELECT COUNT(*) FROM itens_nacionais").fetchone()[0]
    except: res = 0
    conn.close()
    return res

# INTELIGÊNCIA DE EXTRAÇÃO DE MUNICÍPIO
def extrair_municipio_do_orgao(nome_orgao):
    if not nome_orgao: return None
    padroes = [
        r"PREFEITURA MUNICIPAL D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"MUNICIPIO D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"CAMARA MUNICIPAL D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"PREFEITURA D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"FUNDO MUNICIPAL DE SAUDE D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"FUNDO MUNICIPAL DE EDUCA[CÇ][AÃ]O D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)"
    ]
    nome_limpo = remover_acentos(nome_orgao)
    for padrao in padroes:
        match = re.search(remover_acentos(padrao), nome_limpo)
        if match:
            mun = match.group(1).strip().split('-')[0].split('/')[0].strip()
            return mun
    return None

# ==========================================
# 🚀 O TRATOR INVISÍVEL
# ==========================================
def rodar_arrastao_nacional(estado, data_inicio, data_fim, console):
    conn = conectar_banco_nacional()
    cursor = conn.cursor()
    
    total_contratos = 0
    total_salvos = 0
    
    str_ini = data_inicio.strftime('%Y%m%d')
    str_fim = data_fim.strftime('%Y%m%d')
    
    console.info(f"🚜 Trator Nacional rodando na UF: {estado}. De {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}...")
    
    for pagina in range(1, 21): # Puxa 1.000 contratos de cada vez por UF
        url_contratos = f"https://pncp.gov.br/api/consulta/v1/contratos?dataInicial={str_ini}&dataFinal={str_fim}&uf={estado}&pagina={pagina}&tamanhoPagina=50"
        try:
            res_cont = requests.get(url_contratos, headers=HEADERS, timeout=20, verify=False)
            if res_cont.status_code != 200: break
            contratos = res_cont.json().get('data', [])
            if not contratos: break 
            
            for contrato in contratos:
                total_contratos += 1
                orgao_ent = contrato.get('orgaoEntidade') or {}
                orgao = str(orgao_ent.get('razaoSocial', 'Desconhecido')).upper()
                cnpj_orgao = orgao_ent.get('cnpj')
                credor = contrato.get('nomeRazaoSocialFornecedor') or 'NÃO INFORMADO'
                ano_c = contrato.get('anoContrato')
                seq_c = contrato.get('sequencialContrato')
                data_ass = contrato.get('dataAssinatura', dt_fim.strftime('%Y-%m-%d'))
                if len(data_ass) > 10: data_ass = data_ass[:10]
                data_ass_fmt = datetime.strptime(data_ass, '%Y-%m-%d').strftime('%d/%m/%Y')
                
                # Inteligência do Município
                municipio = extrair_municipio_do_orgao(orgao)
                if not municipio: municipio = 'NÃO INFORMADO'
                
                if cnpj_orgao and ano_c and seq_c:
                    url_detalhe = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/contratos/{ano_c}/{seq_c}"
                    try:
                        res_detalhe = requests.get(url_detalhe, headers=HEADERS, timeout=10, verify=False)
                        if res_detalhe.status_code == 200:
                            matches = re.findall(r'(\d{14})-1-(\d+)/(\d{4})', res_detalhe.text)
                            if matches:
                                cnpj_compra, seq_compra_str, ano_compra = matches[0]
                                seq_compra = str(int(seq_compra_str)) 
                                link_pncp = f"https://pncp.gov.br/app/editais/{cnpj_compra}/{ano_compra}/{seq_compra}"
                                api_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_compra}/compras/{ano_compra}/{seq_compra}/itens?pagina=1&tamanhoPagina=500"
                                
                                res_itens = requests.get(api_itens, headers=HEADERS, timeout=15, verify=False)
                                if res_itens.status_code == 200:
                                    lista_itens = res_itens.json()
                                    if isinstance(lista_itens, dict): lista_itens = lista_itens.get('data', [])
                                    
                                    for i, it in enumerate(lista_itens):
                                        desc = remover_acentos(it.get('descricao', f'Item {i}')).upper()
                                        valor = float(it.get('valorUnitarioHomologado') or it.get('valorUnitarioEstimado') or 0.0)
                                        if valor > 0:
                                            unid_obj = it.get('unidadeMedida') or {}
                                            unid_medida = remover_acentos(unid_obj.get('nome', 'UN') if isinstance(unid_obj, dict) else str(unid_obj))
                                            id_unico = f"NAC-{cnpj_compra}-{ano_compra}-{seq_compra}-{it.get('numeroItem', i)}"
                                            
                                            cursor.execute('''
                                                INSERT OR IGNORE INTO itens_nacionais 
                                                (id_item, estado, orgao, municipio, data_assinatura, descricao_item, unid_medida, valor_unitario, credor, origem, link_pncp)
                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                            ''', (id_unico, estado, orgao, municipio.upper(), data_ass_fmt, desc, unid_medida, valor, credor, "TRATOR NACIONAL", link_pncp))
                                            
                                            if cursor.rowcount > 0:
                                                total_salvos += 1
                                    conn.commit()
                    except: pass
                # O Segredo Supremo anti-bloqueio
                time.sleep(0.05)
                
            if total_contratos % 10 == 0:
                console.success(f"🚜 [UF: {estado}] Contratos lidos: {total_contratos} | Novos Itens Nacionais no Cofre: +{total_salvos}")
                total_salvos = 0
                
        except Exception as e: pass
        
    conn.close()
    console.info(f"✅ Arrastão na UF {estado} concluído com sucesso!")

# ==========================================
# INTERFACE DO ROBÔ
# ==========================================
st.title("🚜 Trator Nacional Invisível (Popula Banco de Dados)")
st.markdown("Deixe este robô rodando em segundo plano. Ele varre o Brasil silenciosamente e cria a sua base de dados offline, imune a quedas do Governo.")

st.metric("Total de Itens no Banco Nacional (Offline)", contar_itens_nacionais())
st.divider()

c1, c2, c3 = st.columns(3)
estado_alvo = c1.selectbox("Estado Alvo", ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"])
data_hoje = datetime.now().date()
d_inicio = c2.date_input("Data Inicial", pd.to_datetime("2025-01-01").date())
d_fim = c3.date_input("Data Final", data_hoje)

if st.button("🚀 LIGAR TRATOR NESTE ESTADO", type="primary"):
    tela_logs = st.container()
    rodar_arrastao_nacional(estado_alvo, d_inicio, d_fim, tela_logs)
