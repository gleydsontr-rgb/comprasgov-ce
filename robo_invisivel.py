import time
import requests
import sqlite3
import unicodedata
import re
from datetime import datetime, timedelta

# ==========================================
# 🛡️ CONFIGURAÇÕES DO ROBÔ INVISÍVEL
# ==========================================
ESTADO_ALVO = "CE" 
DIAS_RETROATIVOS = 3

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def imprimir_log(mensagem):
    hora_atual = datetime.now().strftime('%H:%M:%S')
    print(f"[{hora_atual}] 🐺 {mensagem}")

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

# ==========================================
# 🗄️ BASE DE DADOS
# ==========================================
def conectar_banco():
    conn = sqlite3.connect('banco_compras.db', timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS itens_compras (
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

# ==========================================
# 🧠 EXTRATORES (V76 - HIERARQUIA CORRIGIDA)
# ==========================================
def extrair_uf_recursivo(json_obj):
    if isinstance(json_obj, dict):
        if 'ufSigla' in json_obj and json_obj['ufSigla']: return str(json_obj['ufSigla']).upper()
        if 'uf' in json_obj and isinstance(json_obj['uf'], str) and len(json_obj['uf']) == 2: return json_obj['uf'].upper()
        if 'municipio' in json_obj and isinstance(json_obj['municipio'], dict):
            uf = json_obj['municipio'].get('uf')
            if isinstance(uf, dict) and 'sigla' in uf: return str(uf['sigla']).upper()
            if isinstance(uf, str) and len(uf) == 2: return uf.upper()
        for valor in json_obj.values():
            res = extrair_uf_recursivo(valor)
            if res: return res
    elif isinstance(json_obj, list):
        for item in json_obj:
            res = extrair_uf_recursivo(item)
            if res: return res
    return None

def extrair_municipio_recursivo(json_obj):
    if isinstance(json_obj, dict):
        for key in ['municipioNome', 'nomeMunicipio', 'cidade']:
            if key in json_obj and json_obj[key]:
                val = str(json_obj[key]).strip().upper()
                if val not in ['', 'NULL', 'NÃO INFORMADO', 'NAO INFORMADO']:
                    return remover_acentos(val)
        if 'municipio' in json_obj and isinstance(json_obj['municipio'], dict):
            nome = json_obj['municipio'].get('nome')
            if nome and str(nome).strip().upper() not in ['', 'NULL', 'NÃO INFORMADO', 'NAO INFORMADO']: 
                return remover_acentos(str(nome).strip())
        for valor in json_obj.values():
            res = extrair_municipio_recursivo(valor)
            if res: return res
    elif isinstance(json_obj, list):
        for item in json_obj:
            res = extrair_municipio_recursivo(item)
            if res: return res
    return None

def extrair_municipio_do_texto(texto):
    if not texto: return None
    texto_limpo = remover_acentos(texto)
    padroes = [
        r"PREFEITURA(?: MUNICIPAL)? D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"MUNICIPIO D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"CAMARA MUNICIPAL D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"FUNDO MUNICIPAL D[E|O|A|OS|AS]?\s*[A-ZÀ-Ú\s]* D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"CONSELHO ESCOLAR [A-ZÀ-Ú0-9\s]+ D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"SECRETARIA [A-ZÀ-Ú0-9\s]+ D[E|O|A|OS|AS]\s+([A-ZÀ-Ú0-9\s]+)",
        r"\bEM\s+([A-ZÀ-Ú0-9\s]+)[/-]\s*CE\b",  # Ex: "ESCOLA EM MORADA NOVA/CE"
        r"([A-ZÀ-Ú0-9\s]+)[/-]\s*CE\b" 
    ]
    for padrao in padroes:
        match = re.search(padrao, texto_limpo)
        if match: 
            mun = match.group(1).strip().split('-')[0].split('/')[0].strip()
            # Limpa palavras iniciais que atrapalham
            mun = re.sub(r"^(NO|NA|EM|PARA|A|DE)\s+", "", mun).strip()
            if len(mun) < 30 and len(mun) > 2: return mun
    return None

# ==========================================
# 🚀 MOTOR DE VARREDURA INVISÍVEL (V76)
# ==========================================
def rodar_varredura_invisivel():
    import urllib3
    urllib3.disable_warnings()
    
    data_fim = datetime.now().date()
    data_inicio = data_fim - timedelta(days=DIAS_RETROATIVOS)
    
    imprimir_log("INICIANDO ROTINA DIÁRIA AUTOMÁTICA")
    imprimir_log(f"Buscando dados de {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')} (V76 - Prioridade Corrigida)")
    
    conn = conectar_banco()
    cursor = conn.cursor()
    
    cache_orgaos = {}
    total_contratos = 0
    total_salvos = 0
    contratos_bloqueados = 0
    
    str_ini = data_inicio.strftime('%Y%m%d')
    str_fim = data_fim.strftime('%Y%m%d')
    
    for pagina in range(1, 11): 
        url_contratos = f"https://pncp.gov.br/api/consulta/v1/contratos?dataInicial={str_ini}&dataFinal={str_fim}&uf={ESTADO_ALVO}&pagina={pagina}&tamanhoPagina=50"
        try:
            res_cont = requests.get(url_contratos, headers=HEADERS, timeout=25, verify=False)
            if res_cont.status_code != 200: break
            contratos = res_cont.json().get('data', [])
            if not contratos: break 
            
            for contrato in contratos:
                total_contratos += 1
                orgao_ent = contrato.get('orgaoEntidade') or {}
                orgao = orgao_ent.get('razaoSocial', 'Desconhecido')
                cnpj_orgao = orgao_ent.get('cnpj')
                credor = contrato.get('nomeRazaoSocialFornecedor') or 'Não Informado'
                ano_c = contrato.get('anoContrato')
                seq_c = contrato.get('sequencialContrato')
                data_ass = contrato.get('dataAssinatura', str_fim)
                
                uf_real = None
                mun_receita = None
                
                # Bate na Receita Federal (Apenas para garantir a UF e ter backup de cidade)
                if cnpj_orgao:
                    if cnpj_orgao in cache_orgaos:
                        uf_real, mun_receita = cache_orgaos[cnpj_orgao]
                    else:
                        try:
                            res_cnpj = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_orgao}", timeout=10)
                            if res_cnpj.status_code == 200:
                                dados_cnpj = res_cnpj.json()
                                uf_real = dados_cnpj.get('uf')
                                mun_receita = dados_cnpj.get('municipio')
                                cache_orgaos[cnpj_orgao] = (uf_real, mun_receita)
                                time.sleep(0.3)
                        except: pass

                if not uf_real: 
                    uf_real = extrair_uf_recursivo(contrato)

                # A Barreira do Estado 
                if not uf_real or str(uf_real).upper() != ESTADO_ALVO:
                    contratos_bloqueados += 1
                    continue
                
                # ====================================================
                # 🔎 A NOVA HIERARQUIA DE CIDADES (O TEXTO MANDA)
                # ====================================================
                municipio_final = None
                
                # 1. Tenta o Objeto do Contrato (O mais local possível)
                if not municipio_final:
                    municipio_final = extrair_municipio_do_texto(contrato.get('objetoContrato', ''))
                
                # 2. Tenta o nome do Órgão
                if not municipio_final:
                    municipio_final = extrair_municipio_do_texto(orgao)
                    
                # 3. Tenta as gavetas do Governo (JSON)
                if not municipio_final:
                    municipio_final = extrair_municipio_recursivo(contrato)
                    
                # 4. Só agora, em último caso, usa a Sede Fiscal do CNPJ (Receita)
                if not municipio_final and mun_receita:
                    municipio_final = mun_receita
                
                if not municipio_final: 
                    municipio_final = 'NÃO INFORMADO'
                else:
                    municipio_final = remover_acentos(municipio_final)
                
                if cnpj_orgao and ano_c and seq_c:
                    url_detalhe = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_orgao}/contratos/{ano_c}/{seq_c}"
                    try:
                        res_detalhe = requests.get(url_detalhe, headers=HEADERS, timeout=25, verify=False)
                        if res_detalhe.status_code == 200:
                            texto_bruto = res_detalhe.text
                            matches = re.findall(r'(\d{14})-1-(\d+)/(\d{4})', texto_bruto)
                            
                            if matches:
                                cnpj_compra, seq_compra_str, ano_compra = matches[0]
                                seq_compra = str(int(seq_compra_str)) 

                                link_pncp = f"https://pncp.gov.br/app/editais/{cnpj_compra}/{ano_compra}/{seq_compra}"
                                api_itens = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_compra}/compras/{ano_compra}/{seq_compra}/itens?pagina=1&tamanhoPagina=500"
                                
                                # Escavação Profunda final no Edital
                                if municipio_final == 'NÃO INFORMADO':
                                    url_info_compra = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj_compra}/compras/{ano_compra}/{seq_compra}"
                                    try:
                                        res_info = requests.get(url_info_compra, headers=HEADERS, timeout=10, verify=False)
                                        if res_info.status_code == 200:
                                            info_json = res_info.json()
                                            
                                            mun_objeto = extrair_municipio_do_texto(info_json.get('objetoCompra', ''))
                                            if mun_objeto: 
                                                municipio_final = mun_objeto
                                            else:
                                                mun_edital = extrair_municipio_recursivo(info_json)
                                                if mun_edital: municipio_final = mun_edital
                                    except: pass

                                res_itens = requests.get(api_itens, headers=HEADERS, timeout=25, verify=False)
                                
                                if res_itens.status_code == 200:
                                    json_itens = res_itens.json()
                                    lista_itens = json_itens if isinstance(json_itens, list) else json_itens.get('data', [])
                                    
                                    if lista_itens:
                                        for i, it in enumerate(lista_itens):
                                            desc = remover_acentos(it.get('descricao', f'Item {i}'))
                                            valor = float(it.get('valorUnitarioHomologado') or it.get('valorUnitarioEstimado') or 0.0)
                                            unid_obj = it.get('unidadeMedida') or {}
                                            unid_medida = remover_acentos(unid_obj.get('nome', 'UN') if isinstance(unid_obj, dict) else str(unid_obj))
                                            id_unico = f"{cnpj_compra}-{ano_compra}-{seq_compra}-{it.get('numeroItem', i)}"
                                            
                                            cursor.execute('''
                                                INSERT OR IGNORE INTO itens_compras 
                                                (id_item, estado, orgao, municipio, data_assinatura, descricao_item, unid_medida, valor_unitario, credor, origem, link_pncp)
                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                            ''', (id_unico, ESTADO_ALVO, orgao, municipio_final, data_ass, desc, unid_medida, valor, credor, "API", link_pncp))
                                            
                                            if cursor.rowcount > 0:
                                                total_salvos += 1
                                        conn.commit()
                    except: pass
                
                if total_contratos % 10 == 0:
                    imprimir_log(f"Lidos: {total_contratos} | Barrados: {contratos_bloqueados} | Novos no CE: {total_salvos}")
                time.sleep(0.05)
        except Exception as e:
            imprimir_log(f"Erro na página {pagina}: {e}")
            
    conn.close()
    imprimir_log("✅ ROTINA DIÁRIA CONCLUÍDA.")

if __name__ == "__main__":
    rodar_varredura_invisivel()