import sys
import time
import requests
import sqlite3
import unicodedata
import re
import os
from datetime import datetime, timedelta

# ==========================================
# CONFIGURAÇÕES DO ROBÔ NACIONAL
# ==========================================
DIAS_RETROATIVOS = 1 # Pega sempre os dados de 1 dia atrás
DIAS_MANTER_NO_BANCO = 30 # Apaga dados mais velhos que 30 dias para não estourar os 100MB do GitHub
ESTADOS = ["AC", "AL", "AP", "AM", "BA", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"] # CE de fora

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

# ==========================================
# 🗄️ BANCO DE DADOS NACIONAL
# ==========================================
def conectar_banco_nacional():
    diretorio_base = os.path.dirname(os.path.abspath(__file__))
    caminho = os.path.join(diretorio_base, 'banco_nacional.db')
    conn = sqlite3.connect(caminho, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS itens_nacionais (
            id_item TEXT UNIQUE, estado TEXT, orgao TEXT, municipio TEXT,
            data_assinatura TEXT, descricao_item TEXT, unid_medida TEXT,
            valor_unitario REAL, credor TEXT, origem TEXT, link_pncp TEXT
        )
    ''')
    conn.commit()
    return conn

def limpar_banco_antigo(conn):
    data_limite = (datetime.now() - timedelta(days=DIAS_MANTER_NO_BANCO)).strftime('%Y-%m-%d')
    try:
        # Tenta deletar convertendo a data_assinatura do formato DD/MM/YYYY para YYYY-MM-DD na query
        conn.execute("DELETE FROM itens_nacionais WHERE substr(data_assinatura, 7, 4) || '-' || substr(data_assinatura, 4, 2) || '-' || substr(data_assinatura, 1, 2) < ?", (data_limite,))
        conn.commit()
    except Exception as e:
        print(f"Erro ao limpar banco: {e}")

# ==========================================
# 🧠 INTELIGÊNCIA E VARREDURA
# ==========================================
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

def rodar_arrastao_nacional():
    print("Iniciando Arrastão Nacional Diário...")
    conn = conectar_banco_nacional()
    cursor = conn.cursor()
    limpar_banco_antigo(conn)
    
    data_alvo = datetime.now() - timedelta(days=DIAS_RETROATIVOS)
    str_data = data_alvo.strftime('%Y%m%d')
    data_fmt = data_alvo.strftime('%d/%m/%Y')
    
    for estado in ESTADOS:
        print(f"Varrendo UF: {estado}")
        for pagina in range(1, 4): # Pega as 3 primeiras páginas de cada estado (150 contratos/dia por estado)
            url = f"https://pncp.gov.br/api/consulta/v1/contratos?dataInicial={str_data}&dataFinal={str_data}&uf={estado}&pagina={pagina}&tamanhoPagina=50"
            try:
                res = requests.get(url, headers=HEADERS, timeout=20, verify=False)
                if res.status_code != 200: break
                contratos = res.json().get('data', [])
                if not contratos: break 
                
                for contrato in contratos:
                    orgao_ent = contrato.get('orgaoEntidade') or {}
                    orgao = str(orgao_ent.get('razaoSocial', 'Desconhecido')).upper()
                    cnpj_orgao = orgao_ent.get('cnpj')
                    credor = contrato.get('nomeRazaoSocialFornecedor') or 'NÃO INFORMADO'
                    ano_c = contrato.get('anoContrato')
                    seq_c = contrato.get('sequencialContrato')
                    
                    municipio = extrair_municipio_do_orgao(orgao) or 'NÃO INFORMADO'
                    
                    if cnpj_orgao and ano_c and seq_c:
                        api_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/compras/{ano_c}/{seq_c}/itens?pagina=1&tamanhoPagina=500"
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
                                    id_unico = f"NAC-{cnpj_orgao}-{ano_c}-{seq_c}-{it.get('numeroItem', i)}"
                                    link_pncp = f"https://pncp.gov.br/app/editais/{cnpj_orgao}/{ano_c}/{seq_c}"
                                    
                                    cursor.execute('''
                                        INSERT OR IGNORE INTO itens_nacionais 
                                        (id_item, estado, orgao, municipio, data_assinatura, descricao_item, unid_medida, valor_unitario, credor, origem, link_pncp)
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    ''', (id_unico, estado, orgao, municipio.upper(), data_fmt, desc, unid_medida, valor, credor, "TRATOR NACIONAL", link_pncp))
                                    
                            conn.commit()
                time.sleep(0.5) # Pausa educada entre páginas
            except Exception as e:
                print(f"Erro na UF {estado}: {e}")
                
    conn.close()
    print("✅ Arrastão Nacional concluído.")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    rodar_arrastao_nacional()
