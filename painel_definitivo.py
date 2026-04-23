import streamlit as st
import sqlite3
import pandas as pd
import unicodedata
import time
import os
import sys
import urllib.parse
import urllib3
import re
from datetime import datetime
from io import BytesIO, StringIO

# Desativa alertas chatos de segurança do Governo
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# BIBLIOTECAS EXTERNAS
# ==========================================
try:
    from fpdf import FPDF
except ImportError:
    st.error("⚠️ Atenção: A biblioteca fpdf não está instalada.")

try:
    import requests
except ImportError:
    st.error("⚠️ Atenção: A biblioteca requests não está instalada.")

# ==========================================
# CONFIGURAÇÃO DA PÁGINA E DESIGN CLÁSSICO
# ==========================================
st.set_page_config(page_title="Sistema Central | ComprasGov", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    /* Esconde elementos da web do Streamlit */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Fundo clássico de sistema Desktop */
    .stApp { background-color: #E0DFE3; font-family: 'Tahoma', 'Arial', sans-serif; }

    /* Remove espaços em branco excessivos */
    .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; max-width: 98% !important; }

    /* Barra de Título tipo Software Clássico */
    .portal-header {
        background: linear-gradient(to right, #0A246A, #A6CAF0);
        color: white;
        padding: 10px 15px;
        font-family: 'Tahoma', 'Arial', sans-serif;
        border: 2px solid #fff;
        border-bottom-color: #888;
        border-right-color: #888;
        margin-top: -20px;
        margin-bottom: 15px;
    }
    .portal-title { font-size: 18px; font-weight: bold; margin: 0; padding-bottom: 2px; text-shadow: 1px 1px #000; }
    .portal-subtitle { font-size: 12px; margin: 0; color: #FFF; }

    /* Textos e Títulos mais sóbrios e menores */
    h1, h2, h3 { color: #000 !important; font-family: 'Tahoma', 'Arial', sans-serif; font-size: 16px !important; border-bottom: 1px groove #ccc; padding-bottom: 2px; margin-bottom: 10px; margin-top: 10px;}
    p, span, label { font-family: 'Tahoma', 'Arial', sans-serif; font-size: 13px !important; color: #000 !important; }

    /* Botões com aspecto de botão de Software (Bordas 3D) */
    .stButton > button {
        background-color: #ECE9D8 !important;
        color: #000 !important;
        border: 2px solid !important;
        border-top-color: #FFF !important;
        border-left-color: #FFF !important;
        border-bottom-color: #716F64 !important;
        border-right-color: #716F64 !important;
        border-radius: 0px !important;
        font-weight: normal !important;
        padding: 2px 15px !important;
        box-shadow: none !important;
    }
    .stButton > button:active {
        border-top-color: #716F64 !important;
        border-left-color: #716F64 !important;
        border-bottom-color: #FFF !important;
        border-right-color: #FFF !important;
        background-color: #D4D0C8 !important;
    }

    /* Caixas de texto com efeito "afundado" CORRIGIDAS para não cortar */
    div[data-baseweb="input"] > div,
    div[data-baseweb="select"] > div,
    textarea {
        border: 2px inset #D4D0C8 !important;
        border-radius: 0px !important;
        background-color: #FFF !important;
        box-sizing: border-box !important;
    }
    input, textarea, div[data-baseweb="select"] {
        font-size: 13px !important;
    }

    /* Sidebar cinza clássica */
    [data-testid="stSidebar"] {
        background-color: #D4D0C8 !important;
        border-right: 2px ridge #FFF !important;
    }
    
    /* Expander tipo painel de controle */
    .streamlit-expanderHeader {
        background-color: #ECE9D8 !important;
        border: 1px solid #716F64 !important;
        color: #000 !important;
    }
</style>
<div class="portal-header">
    <p class="portal-title">SISTEMA INTEGRADO DE GESTÃO DE COMPRAS E LICITAÇÕES</p>
    <p class="portal-subtitle">Painel Administrativo | v19.5 Classic Desktop (Correção FPDF Logo)</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# INICIALIZAÇÃO DE VARIÁVEIS DE MEMÓRIA
# ==========================================
if 'carrinho' not in st.session_state: st.session_state.carrinho = pd.DataFrame()
if 'df_resultados' not in st.session_state: st.session_state.df_resultados = pd.DataFrame()

keys_to_init = {
    'p1_busca_form': "", 'p2_busca_form': "", 'p3_busca_form': "",
    'safe_nome_relatorio': "ITEM DA COTAÇÃO", 'safe_qtd_relatorio': 1.0,
    'input_qtd_internet_form': 1.0,
    'ultimo_item_selecionado': "", 'search_id': "default",
    'menu_option': "0. Configurações" 
}
for key, value in keys_to_init.items():
    if key not in st.session_state: st.session_state[key] = value

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

def tratar_texto(texto):
    if not texto: return ""
    return str(texto).encode('latin-1', 'replace').decode('latin-1')

# ==========================================
# 📡 BANCO DE DADOS (DUPLO COFRE LOCAL E NACIONAL)
# ==========================================
def obter_caminho_banco(nome="banco_compras.db"):
    if getattr(sys, 'frozen', False): diretorio_base = os.path.dirname(sys.executable)
    else: diretorio_base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(diretorio_base, nome)

def conectar_banco():
    caminho_banco = obter_caminho_banco('banco_compras.db')
    conn = sqlite3.connect(caminho_banco, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS solicitacoes (id INTEGER PRIMARY KEY AUTOINCREMENT, secretaria TEXT, data_solic TEXT, status TEXT, numero_solic TEXT, objeto TEXT, secretarias TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS lotes_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_solicitacao INTEGER, nome_lote TEXT, desc_lote TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_lote INTEGER, id_solicitacao INTEGER, descricao TEXT, unid_medida TEXT, quantidade REAL DEFAULT 1.0)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_compras (id INTEGER PRIMARY KEY AUTOINCREMENT, id_item TEXT, descricao_item TEXT, unid_medida TEXT, valor_unitario REAL, municipio TEXT, estado TEXT, credor TEXT, data_assinatura TEXT, link_pncp TEXT, origem TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS cotacoes_salvas (id_solicitacao INTEGER PRIMARY KEY, dados_json TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS configuracoes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome_orgao TEXT, cnpj TEXT, endereco TEXT, contato TEXT, logo BLOB)''')
    conn.commit()
    return conn

def conectar_banco_nacional():
    caminho = obter_caminho_banco('banco_nacional.db')
    conn = sqlite3.connect(caminho, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_nacionais (id_item TEXT UNIQUE, estado TEXT, orgao TEXT, municipio TEXT, data_assinatura TEXT, descricao_item TEXT, unid_medida TEXT, valor_unitario REAL, credor TEXT, origem TEXT, link_pncp TEXT)''')
    conn.commit()
    return conn

def get_config_entidade():
    # Usando SQLite puro para evitar corrupção de BLOBs pelo Pandas
    conn = conectar_banco()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT nome_orgao, cnpj, endereco, contato, logo FROM configuracoes ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        if row: return {'nome': row[0] if row[0] else '', 'cnpj': row[1] if row[1] else '', 'endereco': row[2] if row[2] else '', 'contato': row[3] if row[3] else '', 'logo': row[4]}
    except: conn.close()
    return {'nome': 'PREFEITURA MUNICIPAL', 'cnpj': '', 'endereco': '', 'contato': '', 'logo': None}

def salvar_carrinho_no_banco():
    if 'solic_importada' in st.session_state and st.session_state['solic_importada'] is not None:
        id_imp = st.session_state['solic_importada']
        conn = conectar_banco()
        if st.session_state.carrinho.empty: conn.execute("DELETE FROM cotacoes_salvas WHERE id_solicitacao=?", (id_imp,))
        else:
            df_valido = st.session_state.carrinho[st.session_state.carrinho['produto_mapa'].str.strip() != ""]
            json_data = df_valido.to_json(orient='records')
            conn.execute("REPLACE INTO cotacoes_salvas (id_solicitacao, dados_json) VALUES (?, ?)", (id_imp, json_data))
        conn.commit()
        conn.close()

# ==========================================
# 📄 FÁBRICA DE PDFs (CORREÇÃO DE EXTENSÃO DO LOGO)
# ==========================================
class RelatorioPDF(FPDF):
    def __init__(self, config, processo, tipo_relatorio):
        super().__init__(); self.config = config; self.processo = processo; self.tipo_relatorio = tipo_relatorio
    def header(self):
        if self.config.get('logo'):
            try:
                logo_b = self.config['logo']
                ext = ".png"
                if isinstance(logo_b, bytes) and logo_b.startswith(b'\xff\xd8'): ext = ".jpg"
                logo_path = f"logo_tmp_{int(time.time()*1000)}_{id(self)}{ext}"
                with open(logo_path, "wb") as f: f.write(logo_b)
                self.image(logo_path, 10, 8, 25)
            except: pass
        self.set_font('Arial', 'B', 14); self.cell(0, 6, tratar_texto(self.config.get('nome', 'ÓRGÃO COMPRADOR')), 0, 1, 'C')
        self.set_font('Arial', '', 8)
        linha_end = f"{self.config.get('endereco', '')} - CNPJ: {self.config.get('cnpj', '')}".strip(" -")
        if linha_end != "CNPJ:": self.cell(0, 4, tratar_texto(linha_end), 0, 1, 'C')
        if self.config.get('contato'): self.cell(0, 4, tratar_texto(self.config.get('contato', '')), 0, 1, 'C')
        self.ln(2); self.set_font('Arial', 'B', 10); self.cell(0, 5, tratar_texto(self.tipo_relatorio), 0, 1, 'C')
        self.set_font('Arial', '', 9); self.cell(0, 5, tratar_texto(f"Processo Nº: {self.processo}"), 0, 1, 'C')
        self.line(10, self.get_y() + 2, 200, self.get_y() + 2); self.ln(8)
    def footer(self):
        self.set_y(-15); self.set_font('Arial', 'I', 8); self.cell(0, 10, tratar_texto(f'Página {self.page_no()}'), 0, 0, 'C')

def gerar_pdf_capa(config, processo, objeto, secretarias_lista):
    pdf = FPDF(); pdf.add_page()
    if config.get('logo'):
        try:
            logo_b = config['logo']
            ext = ".png"
            if isinstance(logo_b, bytes) and logo_b.startswith(b'\xff\xd8'): ext = ".jpg"
            logo_path = f"logo_capa_{int(time.time()*1000)}{ext}"
            with open(logo_path, "wb") as f: f.write(logo_b)
            pdf.image(logo_path, 90, 15, 30); pdf.set_y(50)
        except: pdf.set_y(30)
    else: pdf.set_y(30)
    pdf.set_font('Arial', 'B', 16); pdf.cell(0, 8, tratar_texto(config.get('nome', 'ÓRGÃO COMPRADOR')), 0, 1, 'C')
    if config.get('cnpj'): pdf.set_font('Arial', '', 11); pdf.cell(0, 5, tratar_texto(f"CNPJ: {config.get('cnpj', '')}"), 0, 1, 'C')
    pdf.set_y(70); pdf.set_font('Arial', 'B', 14); pdf.cell(0, 10, tratar_texto("COTAÇÃO DE PREÇOS"), border=1, ln=1, align='C', fill=False); pdf.ln(5)
    pdf.set_font('Arial', 'B', 11); pdf.cell(95, 8, tratar_texto("Nº DO PROCESSO:"), 'L T R', 0, 'L'); pdf.cell(95, 8, tratar_texto("DATA DO PROCESSO:"), 'L T R', 1, 'L')
    pdf.set_font('Arial', '', 11); pdf.cell(95, 8, tratar_texto(processo), 'L B R', 0, 'L'); pdf.cell(95, 8, tratar_texto(datetime.now().strftime('%d/%m/%Y')), 'L B R', 1, 'L'); pdf.ln(5)
    pdf.set_font('Arial', 'B', 11); pdf.cell(0, 8, tratar_texto("DESCRIÇÃO DO OBJETO:"), 'L T R', 1, 'L')
    pdf.set_font('Arial', '', 11); pdf.multi_cell(0, 6, tratar_texto(objeto), border='L B R', align='L'); pdf.ln(5)
    pdf.set_font('Arial', 'B', 11); pdf.cell(0, 8, tratar_texto("ÓRGÃOS DO PROCESSO (SECRETARIAS SOLICITANTES):"), 0, 1, 'L')
    pdf.set_font('Arial', '', 10)
    for sec in secretarias_lista:
        if sec.strip(): pdf.cell(5, 6, "-", 0, 0, 'R'); pdf.cell(0, 6, tratar_texto(sec.strip()), 0, 1, 'L')
    pdf.set_y(-50); pdf.line(60, pdf.get_y(), 150, pdf.get_y()); pdf.ln(2)
    pdf.set_font('Arial', 'B', 10); pdf.cell(0, 6, tratar_texto("Responsável pelo Setor de Compras"), 0, 1, 'C')
    pdf.set_font('Arial', '', 9); pdf.cell(0, 5, tratar_texto("Assinatura e Carimbo"), 0, 1, 'C')
    return pdf.output(dest='S').encode('latin-1')

class RelatorioMapaPDF(FPDF):
    def __init__(self, config, processo, objeto):
        super().__init__(); self.config = config; self.processo = processo; self.objeto = objeto; self.is_resumo = True
    def header(self):
        if self.config.get('logo'):
            try:
                logo_b = self.config['logo']
                ext = ".png"
                if isinstance(logo_b, bytes) and logo_b.startswith(b'\xff\xd8'): ext = ".jpg"
                logo_path = f"logo_tmp_mapa_{int(time.time()*1000)}_{id(self)}{ext}"
                with open(logo_path, "wb") as f: f.write(logo_b)
                self.image(logo_path, 10, 8, 25)
            except: pass
        self.set_font('Arial', 'B', 12); self.cell(0, 5, tratar_texto(self.config.get('nome', '')), 0, 1, 'C')
        self.set_font('Arial', '', 8); linha_end = f"{self.config.get('endereco', '')} - CNPJ: {self.config.get('cnpj', '')}".strip(" -")
        if linha_end != "CNPJ:": self.cell(0, 4, tratar_texto(linha_end), 0, 1, 'C')
        if self.config.get('contato'): self.cell(0, 4, tratar_texto(self.config.get('contato', '')), 0, 1, 'C')
        self.ln(2); self.set_font('Arial', 'B', 10)
        if self.is_resumo: self.cell(0, 5, tratar_texto("RESUMO GERAL DO MAPA DE PREÇO"), 0, 1, 'C')
        else: self.cell(0, 5, tratar_texto("MAPA DE PREÇO - DETALHAMENTO POR COLETA"), 0, 1, 'C')
        self.set_font('Arial', 'B', 9); self.cell(0, 5, tratar_texto(f"N°: {self.processo} - DATA: {datetime.now().strftime('%d/%m/%Y')}"), 0, 1, 'L')
        if self.is_resumo: self.multi_cell(0, 5, tratar_texto(f"ESPECIFICAÇÃO/OBJETO: {self.objeto}"))
        self.ln(2)
    def footer(self):
        self.set_y(-15); self.set_font('Arial', 'I', 8); self.cell(0, 10, tratar_texto(f'Página(s): {self.page_no()}'), 0, 0, 'R')

def gerar_pdf_mapa(df_carrinho, config, processo, objeto):
    pdf = RelatorioMapaPDF(config, processo, objeto)
    if 'produto_mapa' not in df_carrinho.columns: df_carrinho['produto_mapa'] = df_carrinho['descricao_item']
    grupos = df_carrinho.groupby('produto_mapa', sort=False)
    pdf.is_resumo = True; pdf.add_page(); pdf.set_font('Arial', 'B', 8)
    cols = [("Item", 10), ("Descrição", 90), ("Unid.", 15), ("Qtd", 15), ("V. Médio", 30), ("V. Total", 30)]
    for txt, w in cols: pdf.cell(w, 6, txt, border=1, align='C')
    pdf.ln()
    total_geral = 0
    for i, (nome, gp) in enumerate(grupos, 1):
        unid = gp['unid_medida'].iloc[0]; qtd = gp['quantidade'].iloc[0]; media = gp['valor_unitario'].mean(); v_total = media * qtd; total_geral += v_total
        pdf.set_font('Arial', '', 7); pdf.cell(10, 6, str(i), 1, 0, 'C'); pdf.cell(90, 6, tratar_texto(str(nome)[:55]), 1, 0, 'L'); pdf.cell(15, 6, tratar_texto(unid), 1, 0, 'C'); pdf.cell(15, 6, str(int(qtd)) if float(qtd).is_integer() else f"{qtd:.2f}", 1, 0, 'C'); pdf.cell(30, 6, f"R$ {media:,.2f}", 1, 0, 'R'); pdf.cell(30, 6, f"R$ {v_total:,.2f}", 1, 1, 'R')
    pdf.set_font('Arial', 'B', 9); pdf.cell(160, 8, "TOTAL GERAL DA PAUTA:", 0, 0, 'R'); pdf.cell(30, 8, f"R$ {total_geral:,.2f}", 0, 1, 'R')
    pdf.is_resumo = False; pdf.add_page()
    for nome, gp in grupos:
        unid = gp['unid_medida'].iloc[0]; qtd = gp['quantidade'].iloc[0]; pdf.set_font('Arial', 'B', 8); qtd_str = f"{int(qtd)}" if float(qtd).is_integer() else f"{qtd:.2f}"; pdf.multi_cell(0, 5, tratar_texto(f"ITEM: {nome} - UNID: {unid} - QTD TOTAL: {qtd_str}")); pdf.ln(1); pdf.set_font('Arial', 'B', 7); pdf.cell(10, 5, "Pesq.", 1, 0, 'C'); pdf.cell(55, 5, "Coleta", 1, 0, 'C'); pdf.cell(85, 5, "Fornecedor", 1, 0, 'L'); pdf.cell(20, 5, "V. Unit.", 1, 0, 'C'); pdf.cell(20, 5, "V. Total", 1, 1, 'C')
        pdf.set_font('Arial', '', 7)
        for idx, (_, row) in enumerate(gp.iterrows(), 1):
            coleta = "LINK DA WEB" if row.get('origem') == 'INTERNET' else "CESTA PREÇOS GOVERNO"; v_unit = row['valor_unitario']; v_tot = v_unit * float(qtd); pdf.cell(10, 5, str(idx), 1, 0, 'C'); pdf.cell(55, 5, tratar_texto(coleta), 1, 0, 'C'); pdf.cell(85, 5, tratar_texto(row['credor'][:48]), 1, 0, 'L'); pdf.cell(20, 5, f"{v_unit:,.2f}".replace('.', ','), 1, 0, 'R'); pdf.cell(20, 5, f"{v_tot:,.2f}".replace('.', ','), 1, 1, 'R')
        pdf.ln(5)
    return pdf.output(dest='S').encode('latin-1')

def gerar_pdf_detalhado_pncp(df_carrinho, config, processo, objeto):
    pdf = RelatorioPDF(config, processo, "RELATÓRIO DETALHADO DE PREÇOS - GOVERNO"); pdf.add_page(); pdf.set_font('Arial', 'B', 10); pdf.multi_cell(0, 6, tratar_texto(f"OBJETO: {objeto}")); pdf.ln(5); df_pncp = df_carrinho[df_carrinho['origem'] != 'INTERNET']
    if df_pncp.empty:
        pdf.set_font('Arial', '', 10); pdf.cell(0, 10, tratar_texto("Nenhuma cotação do banco público adicionada."), 0, 1, 'C'); return pdf.output(dest='S').encode('latin-1')
    for index, row in df_pncp.iterrows():
        pdf.set_font('Arial', 'B', 9); pdf.set_fill_color(230, 230, 230); pdf.multi_cell(0, 6, tratar_texto(f"ITEM: {row['descricao_item']} (Unid: {row['unid_medida']})"), 1, 'L', fill=True); pdf.set_font('Arial', '', 8)
        info = f"Fornecedor: {row['credor']}\nLocalidade: {row['municipio']} - {row['estado']}\nData: {row['data_assinatura']}\nValor: R$ {row['valor_unitario']:.2f}\nLink PNCP: {row['link_pncp']}"
        pdf.multi_cell(0, 5, tratar_texto(info), 1, 'L'); pdf.ln(2)
    return pdf.output(dest='S').encode('latin-1')

def gerar_pdf_detalhado_links(df_carrinho, config, processo, objeto):
    pdf = RelatorioPDF(config, processo, "RELATÓRIO DETALHADO DE PREÇOS - INTERNET"); pdf.add_page(); pdf.set_font('Arial', 'B', 10); pdf.multi_cell(0, 6, tratar_texto(f"OBJETO: {objeto}")); pdf.ln(5); df_int = df_carrinho[df_carrinho['origem'] == 'INTERNET']
    if df_int.empty:
        pdf.set_font('Arial', '', 10); pdf.cell(0, 10, tratar_texto("Nenhuma cotação da internet adicionada."), 0, 1, 'C'); return pdf.output(dest='S').encode('latin-1')
    for index, row in df_int.iterrows():
        pdf.set_font('Arial', 'B', 9); pdf.set_fill_color(230, 230, 230); pdf.multi_cell(0, 6, tratar_texto(f"ITEM: {row['descricao_item']} (Unid: {row['unid_medida']})"), 1, 'L', fill=True); pdf.set_font('Arial', '', 8)
        info = f"Loja: {row['credor']}\nValor Final: R$ {row['valor_unitario']:.2f}\nLink: {row['link_pncp']}"
        pdf.multi_cell(0, 5, tratar_texto(info), 1, 'L'); pdf.ln(2)
    return pdf.output(dest='S').encode('latin-1')

# ==========================================
# 🛒 BARRA LATERAL E RADAR
# ==========================================
st.sidebar.title("🛒 Cotações")

if not st.session_state.carrinho.empty:
    resumo = st.session_state.carrinho.groupby('produto_mapa').agg({'valor_unitario': 'count', 'quantidade': 'max'})
    st.sidebar.success(f"**{len(resumo)}** grupos no carrinho.")
    st.sidebar.divider()
    st.sidebar.subheader("Gerenciar Carrinho")
    
    lista_itens_carrinho = [i for i in st.session_state.carrinho['produto_mapa'].unique() if i.strip()]
    item_remover = st.sidebar.selectbox("Excluir do carrinho:", [""] + lista_itens_carrinho)
    
    if st.sidebar.button("Remover Item"):
        if item_remover:
            st.session_state.carrinho = st.session_state.carrinho[st.session_state.carrinho['produto_mapa'] != item_remover]
            salvar_carrinho_no_banco(); st.rerun()

    if st.sidebar.button("Esvaziar Carrinho"):
        st.session_state.carrinho = pd.DataFrame()
        salvar_carrinho_no_banco(); st.rerun()
else:
    st.sidebar.info("Carrinho vazio.")

# ==========================================
# 🤖 RADAR DOS COFRES
# ==========================================
st.sidebar.divider()
st.sidebar.subheader("Radar do Sistema")
try:
    conn_radar = conectar_banco()
    df_radar = pd.read_sql_query("SELECT COUNT(*) as total, MAX(data_assinatura) as ultima_data FROM itens_compras", conn_radar)
    total_itens = df_radar['total'].iloc[0]
    ultima_data = df_radar['ultima_data'].iloc[0]
    conn_radar.close()
    
    st.sidebar.write(f"Itens Locais: **{total_itens:,}**".replace(',', '.'))
    
    try:
        conn_nac = conectar_banco_nacional()
        df_nac = pd.read_sql_query("SELECT COUNT(*) as total FROM itens_nacionais", conn_nac)
        total_nac = df_nac['total'].iloc[0]
        
        st.sidebar.write(f"Itens Nacionais: **{total_nac:,}**".replace(',', '.'))
        
        with st.sidebar.expander("Ver Log do Robô Nacional"):
            if total_nac > 0:
                df_ultimos = pd.read_sql_query("SELECT descricao_item, estado, valor_unitario FROM itens_nacionais ORDER BY ROWID DESC LIMIT 30", conn_nac)
                st.dataframe(df_ultimos, use_container_width=True, hide_index=True)
            else:
                st.info("O banco nacional ainda está vazio ou atualizando.")
        conn_nac.close()
    except Exception:
        st.sidebar.write(f"Itens Nacionais: **0**")
        
    st.sidebar.write(f"Ult. Sincronização: **{ultima_data}**")
except Exception:
    pass

# ==========================================
# 🗂️ MÓDULOS DE NAVEGAÇÃO
# ==========================================
opcoes_menu = ["0. Configurações", "1. Cadastro de Solicitação (Planejamento)", "2. Painel Central de Cotação (Pesquisa)", "3. Histórico e Relatórios"]
try: idx_aba = opcoes_menu.index(st.session_state['menu_option'])
except ValueError: idx_aba = 0

aba_selecionada = st.radio("Selecione o Módulo:", opcoes_menu, index=idx_aba, horizontal=True, label_visibility="collapsed")
if aba_selecionada != st.session_state['menu_option']:
    st.session_state['menu_option'] = aba_selecionada
    st.rerun()

# ==========================================
# TELA 0: CONFIGURAÇÕES DA ENTIDADE
# ==========================================
if aba_selecionada == "0. Configurações":
    st.subheader("Configurações da Entidade (Órgão)")
    st.markdown("Os dados preenchidos aqui serão utilizados como cabeçalho em **todos os relatórios PDF** gerados pelo sistema.")
    
    conn = conectar_banco()
    try: df_cfg = pd.read_sql_query("SELECT * FROM configuracoes ORDER BY id DESC LIMIT 1", conn)
    except: df_cfg = pd.DataFrame()
    
    cfg_nome = df_cfg['nome_orgao'].iloc[0] if not df_cfg.empty else "PREFEITURA MUNICIPAL"
    cfg_cnpj = df_cfg['cnpj'].iloc[0] if not df_cfg.empty else ""
    cfg_end = df_cfg['endereco'].iloc[0] if not df_cfg.empty else ""
    cfg_contato = df_cfg['contato'].iloc[0] if not df_cfg.empty else ""
    
    with st.form("form_config"):
        c1, c2 = st.columns(2)
        nome = c1.text_input("Nome da Entidade", value=cfg_nome)
        cnpj = c2.text_input("CNPJ", value=cfg_cnpj)
        end = st.text_input("Endereço Completo", value=cfg_end)
        cont = st.text_input("Contato (Telefone / Email / Site)", value=cfg_contato)
        st.markdown("Logomarca do Órgão")
        logo_file = st.file_uploader("Envie a imagem (Preferência para fundo transparente PNG)", type=['png', 'jpg', 'jpeg'])
        
        if st.form_submit_button("Salvar Configurações Gerais"):
            logo_blob = None
            if logo_file is not None: logo_blob = logo_file.read()
            elif not df_cfg.empty and df_cfg['logo'].iloc[0] is not None: logo_blob = df_cfg['logo'].iloc[0]
                
            conn.execute("DELETE FROM configuracoes")
            conn.execute("INSERT INTO configuracoes (nome_orgao, cnpj, endereco, contato, logo) VALUES (?, ?, ?, ?, ?)", (nome.upper(), cnpj, end.upper(), cont, logo_blob))
            conn.commit()
            st.success("Configurações salvas com sucesso!")
            time.sleep(1.5); st.rerun()
    conn.close()

# ==========================================
# TELA 1: SOLICITAÇÃO E IMPORTADOR
# ==========================================
elif aba_selecionada == "1. Cadastro de Solicitação (Planejamento)":
    
    c_z1, c_z2 = st.columns([4, 1])
    if c_z2.button("Zerar Planejamento (Atenção)"):
        conn = conectar_banco()
        for t in ['solicitacoes', 'lotes_solicitacao', 'itens_solicitacao', 'cotacoes_salvas']:
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.commit(); conn.close()
        conectar_banco()
        if 'solic_importada' in st.session_state: del st.session_state['solic_importada']
        st.session_state.carrinho = pd.DataFrame()
        st.success("Banco limpo. Pode importar a nova pauta."); st.rerun()

    st.markdown("### Importação Automática de Pautas")
    with st.expander("Importar Planilha (Excel/CSV)", expanded=False):
        arquivo_pauta = st.file_uploader("Selecione o arquivo da Pauta", type=["csv", "xlsx"])
        
        if arquivo_pauta:
            try:
                if arquivo_pauta.name.endswith('.csv'):
                    try: df_pauta = pd.read_csv(arquivo_pauta, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8')
                    except: arquivo_pauta.seek(0); df_pauta = pd.read_csv(arquivo_pauta, sep=None, engine='python', on_bad_lines='skip', encoding='latin-1')
                else:
                    try: df_pauta = pd.read_excel(arquivo_pauta)
                    except: st.error("Instale o openpyxl"); st.stop()
                
                idx_header = None
                for i, row in df_pauta.iterrows():
                    if row.astype(str).str.contains('ESPECIFICAÇÃO', case=False, na=False).any() or row.astype(str).str.contains('LOTE', case=False, na=False).any():
                        idx_header = i; break
                if idx_header is not None:
                    df_pauta.columns = df_pauta.iloc[idx_header]
                    df_pauta = df_pauta.iloc[idx_header+1:].dropna(how='all')
                
                novas_colunas = []
                for c in df_pauta.columns:
                    nome_limpo = str(c).strip().upper()
                    if not nome_limpo or nome_limpo == 'NAN': nome_limpo = 'VAZIO'
                    base = nome_limpo; cont = 1
                    while nome_limpo in novas_colunas:
                        nome_limpo = f"{base}_{cont}"; cont += 1
                    novas_colunas.append(nome_limpo)
                df_pauta.columns = novas_colunas
                
                st.success("Planilha lida com sucesso! Configure a Cotação abaixo:")
                
                c_capa1, c_capa2 = st.columns(2)
                nome_solic_auto = c_capa1.text_input("Nome do Arquivo Interno:", value=f"PAUTA CONSOLIDADA - {arquivo_pauta.name.split('.')[0]}")
                desc_obj = c_capa1.text_area("Objeto da Compra (Para a Capa):", value="AQUISIÇÃO DE MATERIAIS")
                sec_solic = c_capa2.text_area("Órgãos Solicitantes (Um por linha):", value="SECRETARIA DE ADMINISTRAÇÃO")
                
                c_map3, c_map4, c_map5 = st.columns(3)
                idx_desc = list(df_pauta.columns).index("ESPECIFICAÇÃO") if "ESPECIFICAÇÃO" in df_pauta.columns else 0
                idx_unid = list(df_pauta.columns).index("UNID.") if "UNID." in df_pauta.columns else 0
                idx_total = len(df_pauta.columns) - 1
                for i, col in enumerate(df_pauta.columns):
                    if col == "TOTAL": idx_total = i; break
                
                col_desc = c_map3.selectbox("Coluna da Descrição do Item:", df_pauta.columns, index=idx_desc)
                col_unid = c_map4.selectbox("Coluna da Unidade de Medida:", df_pauta.columns, index=idx_unid)
                col_qtd = c_map5.selectbox("Coluna da Quantidade TOTAL:", df_pauta.columns, index=idx_total)
                
                if st.button("Processar Pauta", type="primary"):
                    conn = conectar_banco()
                    num_gerado = f"PAUTA-{datetime.now().strftime('%m%d%H%M')}"
                    
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO solicitacoes (numero_solic, secretaria, data_solic, status, objeto, secretarias) VALUES (?, ?, ?, ?, ?, ?)", 
                                   (num_gerado, nome_solic_auto.upper(), datetime.now().strftime('%d/%m/%Y'), 'ABERTA', desc_obj.upper(), sec_solic.upper()))
                    id_solic_master = cursor.lastrowid
                    
                    cursor.execute("INSERT INTO lotes_solicitacao (id_solicitacao, nome_lote, desc_lote) VALUES (?, ?, ?)", (id_solic_master, "LOTE ÚNICO", ""))
                    id_lote_master = cursor.lastrowid
                    
                    for _, row_item in df_pauta.iterrows():
                        desc_val = str(row_item[col_desc]).strip().upper()
                        unid_val = str(row_item[col_unid]).strip().upper()
                        qtd_val = pd.to_numeric(row_item[col_qtd], errors='coerce')
                        
                        if desc_val and desc_val != 'NAN' and 'VAZIO' not in desc_val and pd.notna(qtd_val) and qtd_val > 0:
                            try: cursor.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida, quantidade) VALUES (?, ?, ?, ?, ?)", (id_lote_master, id_solic_master, desc_val, unid_val, float(qtd_val)))
                            except: cursor.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida) VALUES (?, ?, ?, ?)", (id_lote_master, id_solic_master, desc_val, unid_val))
                    
                    conn.commit(); conn.close()
                    st.success("Pauta importada. Vá para o módulo 2.")
            except Exception as e: st.error(f"Erro no processamento: {e}")

    st.divider()

    with st.expander("Opção Manual (Inserção de Itens)", expanded=False):
        c_man1, c_man2 = st.columns(2)
        nome_sec = c_man1.text_input("Identificação do Processo")
        obj_man = c_man1.text_area("Objeto da Compra (Capa):", value="AQUISIÇÃO DE MATERIAIS")
        sec_man = c_man2.text_area("Órgãos Solicitantes:", value="SECRETARIA GERAL")
        
        if st.button("Criar Processo Manual"):
            if nome_sec:
                num_gerado = f"{datetime.now().strftime('%Y.%m%d%H%M')}"
                conn = conectar_banco()
                conn.execute("INSERT INTO solicitacoes (numero_solic, secretaria, data_solic, status, objeto, secretarias) VALUES (?, ?, ?, ?, ?, ?)", 
                             (num_gerado, nome_sec.upper(), datetime.now().strftime('%d/%m/%Y'), 'ABERTA', obj_man.upper(), sec_man.upper()))
                conn.commit(); conn.close(); st.success(f"Criado!"); st.rerun()

    conn = conectar_banco()
    try: df_solic = pd.read_sql_query("SELECT * FROM solicitacoes WHERE status='ABERTA'", conn)
    except: df_solic = pd.DataFrame() 
    
    if not df_solic.empty:
        st.markdown("### Visualizar Pauta e Adicionar Itens")
        solic_selecionada = st.selectbox("Pauta Aberta:", df_solic['id'].astype(str) + " - " + df_solic['secretaria'])
        id_solic = int(solic_selecionada.split(" - ")[0])
        
        c_lote, c_item = st.columns(2)
        with c_lote:
            with st.form("form_lote", clear_on_submit=True):
                nome_lote = st.text_input("Adicionar Lote")
                if st.form_submit_button("Salvar Lote"):
                    if nome_lote:
                        conn.execute("INSERT INTO lotes_solicitacao (id_solicitacao, nome_lote, desc_lote) VALUES (?, ?, ?)", (id_solic, nome_lote.upper(), ""))
                        conn.commit(); st.rerun()
        
        df_lotes = pd.read_sql_query(f"SELECT * FROM lotes_solicitacao WHERE id_solicitacao={id_solic}", conn)
        with c_item:
            if not df_lotes.empty:
                with st.form("form_item", clear_on_submit=True):
                    lote_selec = st.selectbox("Lote Alvo", df_lotes['id'].astype(str) + " - " + df_lotes['nome_lote'])
                    id_lote = int(lote_selec.split(" - ")[0])
                    desc_item = st.text_area("Item")
                    ci_1, ci_2 = st.columns(2)
                    unid_item = ci_1.text_input("Un.")
                    qtd_item = ci_2.number_input("Qtd.", min_value=1.0)
                    if st.form_submit_button("Inserir Item"):
                        if desc_item and unid_item:
                            try: conn.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida, quantidade) VALUES (?, ?, ?, ?, ?)", (id_lote, id_solic, desc_item.upper(), unid_item.upper(), qtd_item))
                            except sqlite3.OperationalError: conn.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida) VALUES (?, ?, ?, ?)", (id_lote, id_solic, desc_item.upper(), unid_item.upper()))
                            conn.commit(); st.rerun()
                            
        try: df_bruto = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.* FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_solic}", conn)
        except Exception: df_bruto = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_solic}", conn)
            
        df_itens = pd.DataFrame()
        if not df_bruto.empty:
            df_itens['Lote'] = df_bruto['Lote']; df_itens['Produto'] = df_bruto['Produto']; df_itens['Unid'] = df_bruto['Unid']; df_itens['Qtd'] = df_bruto['quantidade'] if 'quantidade' in df_bruto.columns else 1.0
            st.dataframe(df_itens, use_container_width=True, hide_index=True)
    conn.close()

# ==========================================
# TELA 2: COTAÇÃO E PESQUISA
# ==========================================
elif aba_selecionada == "2. Painel Central de Cotação (Pesquisa)":
    
    st.markdown("### Seleção de Pauta em Andamento")
    conn = conectar_banco()
    try: df_todas_solic = pd.read_sql_query("SELECT * FROM solicitacoes WHERE status='ABERTA'", conn)
    except: df_todas_solic = pd.DataFrame()
        
    if not df_todas_solic.empty:
        c_imp1, c_imp2 = st.columns([4, 1])
        solic_escolhida = c_imp1.selectbox("Pauta Ativa:", df_todas_solic['id'].astype(str) + " - " + df_todas_solic['secretaria'])
        id_solic_imp = int(solic_escolhida.split(" - ")[0])
        
        if c_imp2.button("Carregar Pauta", use_container_width=True):
            st.session_state['solic_importada'] = id_solic_imp
            df_cart = pd.read_sql_query(f"SELECT dados_json FROM cotacoes_salvas WHERE id_solicitacao={id_solic_imp}", conn)
            if not df_cart.empty and df_cart['dados_json'].iloc[0]:
                try: 
                    df_load = pd.read_json(StringIO(df_cart['dados_json'].iloc[0]), orient='records')
                    df_load.dropna(subset=['produto_mapa', 'valor_unitario'], inplace=True) 
                    df_load = df_load[df_load['produto_mapa'].str.strip() != ""]
                    if not df_load.empty: st.session_state.carrinho = df_load
                    else: st.session_state.carrinho = pd.DataFrame()
                except: st.session_state.carrinho = pd.DataFrame()
            else: st.session_state.carrinho = pd.DataFrame()
            st.rerun()

    df_itens_imp = pd.DataFrame()
    if 'solic_importada' in st.session_state and st.session_state['solic_importada'] is not None:
        id_imp = st.session_state['solic_importada']
        try: df_bruto_imp = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.* FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_imp}", conn)
        except Exception: df_bruto_imp = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_imp}", conn)
            
        if not df_bruto_imp.empty:
            df_itens_imp['Lote'] = df_bruto_imp['Lote']; df_itens_imp['Produto'] = df_bruto_imp['Produto']; df_itens_imp['Unid'] = df_bruto_imp['Unid']; df_itens_imp['Qtd'] = df_bruto_imp['quantidade'] if 'quantidade' in df_bruto_imp.columns else 1.0
            st.dataframe(df_itens_imp, use_container_width=True, hide_index=True)
            
            lista_produtos = df_itens_imp['Produto'].tolist()
            item_selecionado = st.selectbox("Selecione um item da planilha acima para consultar:", [""] + lista_produtos)
            
            if item_selecionado != st.session_state['ultimo_item_selecionado']:
                st.session_state['ultimo_item_selecionado'] = item_selecionado
                if item_selecionado:
                    stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS', 'SEM', 'TIPO', 'KG', 'UND', 'PCT', 'CX', 'UNID', 'LOTE']
                    texto_limpo = remover_acentos(item_selecionado).replace('-', ' ').replace(',', ' ').replace('.', ' ')
                    palavras = [p for p in texto_limpo.split() if p not in stopwords and len(p) > 1]
                    qtd_extraida = float(df_itens_imp[df_itens_imp['Produto'] == item_selecionado]['Qtd'].iloc[0])
                    
                    st.session_state['safe_nome_relatorio'] = item_selecionado
                    st.session_state['safe_qtd_relatorio'] = qtd_extraida
                    st.session_state['p1_busca_form'] = palavras[0] if len(palavras) > 0 else ""
                    st.session_state['p2_busca_form'] = palavras[1] if len(palavras) > 1 else ""
                    st.session_state['p3_busca_form'] = ""
                else:
                    st.session_state['safe_nome_relatorio'] = "ITEM DA COTAÇÃO"; st.session_state['safe_qtd_relatorio'] = 1.0; st.session_state['p1_busca_form'] = ""; st.session_state['p2_busca_form'] = ""; st.session_state['p3_busca_form'] = ""
                st.rerun() 
                
            if item_selecionado:
                st.success(f"Item Alvo: {item_selecionado} | Qtd: {st.session_state['safe_qtd_relatorio']}")
    
    conn.close(); st.divider()

    st.markdown("### Busca Central nos Cofres (CE e Nacional)")

    with st.form("form_consulta"):
        c1, c2, c3, c4 = st.columns(4)
        p1 = c1.text_input("Termo Principal", key="p1_busca_form")
        p2 = c2.text_input("Contendo também (1)", key="p2_busca_form")
        p3 = c3.text_input("Contendo também (2)", key="p3_busca_form")
        p_excluir = c4.text_input("Não pode conter")
        
        c5, c6, c7, c8, c9 = st.columns([2, 1.5, 1.5, 1.5, 1.5])
        modo_busca = c5.selectbox("Tipo de Filtro", ["Ampla (Qualquer parte)", "Inteligente (Início da Descrição)"])
        dt_ini = c6.date_input("Data inicial", value=datetime(2025, 1, 1), format="DD/MM/YYYY")
        dt_fim = c7.date_input("Data final", format="DD/MM/YYYY")
        val_ini = c8.number_input("Valor min. (R$)", min_value=0.0, step=1.0)
        val_fim = c9.number_input("Valor max. (R$)", min_value=0.0, step=1.0)
        
        c10, c11, c12 = st.columns([1.5, 3, 1.5])
        uf = c10.selectbox("Filtro de UF", ["TODAS", "CE", "AC", "AL", "AP", "AM", "BA", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"], index=15)
        relevancia = c11.text_input("Frase Exata (Obrigatória no texto)")
        ordem = c12.selectbox("Classificar por", ["DATA RECENTE", "MENOR PREÇO", "MAIOR PREÇO"])
        
        submit = st.form_submit_button("Consultar Base de Dados")

    if submit:
        st.session_state['search_id'] = str(time.time()) 
        
        def aplicar_busca(texto, qry, operador="AND"):
            termo = remover_acentos(texto).strip()
            stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS']
            if termo:
                palavras = [p for p in termo.split() if p not in stopwords]
                if not palavras: palavras = termo.split() 
                for p in palavras:
                    if operador == "AND": qry += f" AND descricao_item LIKE '%{p}%'"
                    elif operador == "NOT": qry += f" AND descricao_item NOT LIKE '%{p}%'"
            return qry

        filtros = ""
        filtros = aplicar_busca(p1, filtros)
        filtros = aplicar_busca(p2, filtros)
        filtros = aplicar_busca(p3, filtros)
        filtros = aplicar_busca(p_excluir, filtros, operador="NOT")
        if relevancia: filtros += f" AND descricao_item LIKE '%{remover_acentos(relevancia).strip()}%'"
        if val_ini > 0: filtros += f" AND valor_unitario >= {val_ini}"
        if val_fim > 0: filtros += f" AND valor_unitario <= {val_fim}"
        filtros += f" AND data_assinatura >= '{dt_ini.strftime('%Y-%m-%d')}' AND data_assinatura <= '{dt_fim.strftime('%Y-%m-%d')}'"
        
        ordem_sql = " ORDER BY data_assinatura DESC"
        if ordem == "MENOR PREÇO": ordem_sql = " ORDER BY valor_unitario ASC"
        elif ordem == "MAIOR PREÇO": ordem_sql = " ORDER BY valor_unitario DESC"
        
        query_local = "SELECT id_item, descricao_item, unid_medida, valor_unitario, municipio, estado, credor, data_assinatura, link_pncp, origem FROM itens_compras WHERE valor_unitario > 0" + filtros + ordem_sql + " LIMIT 1500"
        query_nacional = "SELECT id_item, descricao_item, unid_medida, valor_unitario, municipio, estado, credor, data_assinatura, link_pncp, origem FROM itens_nacionais WHERE valor_unitario > 0" + filtros + ordem_sql + " LIMIT 1500"

        def aplicar_filtro_estrito(df_alvo, texto):
            if not texto or df_alvo.empty: return df_alvo
            termo = remover_acentos(texto).strip()
            stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS']
            palavras = [p for p in termo.split() if p not in stopwords and len(p) > 1]
            for p in palavras:
                df_alvo = df_alvo[df_alvo['descricao_item'].str.contains(rf'\b{p}\b', regex=True, na=False)]
            return df_alvo
        
        with st.spinner("Lendo base de dados local e nacional..."):
            df_local = pd.DataFrame()
            df_nac = pd.DataFrame()
            
            try:
                conn_loc = conectar_banco()
                df_local = pd.read_sql_query(query_local, conn_loc)
                conn_loc.close()
            except: pass
            
            try:
                conn_nac = conectar_banco_nacional()
                df_nac = pd.read_sql_query(query_nacional, conn_nac)
                conn_nac.close()
            except: pass
            
            df_combinado = pd.concat([df_local, df_nac], ignore_index=True)

            if not df_combinado.empty:
                df_combinado = df_combinado.drop_duplicates(subset=['descricao_item', 'valor_unitario', 'credor', 'link_pncp'])
                df_combinado = aplicar_filtro_estrito(df_combinado, p1)
                df_combinado = aplicar_filtro_estrito(df_combinado, p2)
                df_combinado = aplicar_filtro_estrito(df_combinado, p3)
                
                if not df_combinado.empty:
                    df_final = pd.DataFrame()
                    if uf != "TODAS":
                        df_final = df_combinado[df_combinado['estado'] == uf]
                        if not df_final.empty:
                            df_final.insert(0, 'Selecionar', False)
                            df_final['municipio'] = df_final['municipio'].fillna('Não Informado')
                            df_final['data_assinatura'] = pd.to_datetime(df_final['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                            st.session_state.df_resultados = df_final.head(150)
                        else:
                            st.session_state.df_resultados = pd.DataFrame()
                            st.error(f"Nenhum item exato encontrado na UF '{uf}'. Tente alterar a UF para 'TODAS' ou ajustar os termos.")
                    else:
                        df_final = df_combinado
                        df_final.insert(0, 'Selecionar', False)
                        df_final['municipio'] = df_final['municipio'].fillna('Não Informado')
                        df_final['data_assinatura'] = pd.to_datetime(df_final['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                        st.session_state.df_resultados = df_final.head(150)
                else:
                    st.session_state.df_resultados = pd.DataFrame()
                    st.error("Termo encontrado, mas retido pelo Filtro Estrito (ex: procurou 'cimento', achou 'fornecimento').")
            else:
                st.session_state.df_resultados = pd.DataFrame()
                st.error("Nenhum registro encontrado em nenhum dos bancos.")

    if not st.session_state.df_resultados.empty:
        st.markdown("### Preenchimento do Relatório PDF"); c_add1, c_add2, c_add3 = st.columns([3, 1.5, 2])
        nome_grupo = c_add1.text_input("Descrição Oficial do Item:", value=st.session_state['safe_nome_relatorio']); qtd_grupo = c_add2.number_input("Qtd. Total:", value=float(st.session_state['safe_qtd_relatorio']), step=1.0)
        st.session_state['safe_nome_relatorio'] = nome_grupo; st.session_state['safe_qtd_relatorio'] = qtd_grupo
        
        colunas_mostrar = ['Selecionar', 'descricao_item', 'unid_medida', 'valor_unitario', 'municipio', 'estado', 'credor', 'data_assinatura', 'id_item', 'link_pncp', 'origem']
        df_exibicao = st.session_state.df_resultados[colunas_mostrar]
        chave_dinamica = f"editor_busca_{st.session_state.get('search_id', 'default')}"
        df_editado = st.data_editor(
            df_exibicao, key=chave_dinamica, use_container_width=True, hide_index=True, height=350,
            column_config={"Selecionar": st.column_config.CheckboxColumn("X", required=True), "descricao_item": st.column_config.TextColumn("Descrição da Nota", width="large"), "valor_unitario": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"), "data_assinatura": st.column_config.TextColumn("Data", width="small"), "id_item": None, "origem": None, "link_pncp": st.column_config.LinkColumn("Edital", display_text="Visualizar")}
        )
        
        if c_add3.button("INCLUIR COTAÇÕES", type="primary", use_container_width=True):
            if not st.session_state['safe_nome_relatorio'].strip(): st.error("Insira o nome oficial do relatório.")
            else:
                selecionados = df_editado[df_editado['Selecionar'] == True].copy()
                selecionados = selecionados.drop(columns=['Selecionar'])
                if not selecionados.empty:
                    selecionados['produto_mapa'] = remover_acentos(st.session_state['safe_nome_relatorio']).strip(); selecionados['quantidade'] = float(st.session_state['safe_qtd_relatorio']) 
                    selecionados['id_item'] = [f"CART-{time.time()}-{i}" for i in range(len(selecionados))]
                    st.session_state.carrinho = pd.concat([st.session_state.carrinho, selecionados], ignore_index=True)
                    
                    # --- APAGADOR DE MEMÓRIA E REINÍCIO DA TABELA ---
                    st.session_state.df_resultados['Selecionar'] = False
                    st.session_state['search_id'] = str(time.time())
                    # ------------------------------------------------
                    
                    st.session_state['ultimo_item_selecionado'] = ""; salvar_carrinho_no_banco(); st.success("Incluído!"); time.sleep(1); st.rerun()
                else: st.warning("Marque o 'X' na tabela.")
                
    st.divider(); st.markdown("### Resumo do Mapa de Preços")
    if not st.session_state.carrinho.empty:
        df_raiox = st.session_state.carrinho[['produto_mapa', 'descricao_item', 'credor', 'valor_unitario', 'origem']].copy()
        st.dataframe(df_raiox, use_container_width=True, hide_index=True, height=300, column_config={"produto_mapa": st.column_config.TextColumn("Grupo PDF", width="medium"), "descricao_item": st.column_config.TextColumn("Descrição", width="large"), "credor": "Fornecedor", "valor_unitario": st.column_config.NumberColumn("Valor Un.", format="R$ %.2f"), "origem": "Fonte"})
    else: st.info("Vazio.")

    st.divider(); st.markdown("### Cotação Avulsa (Internet)")
    with st.form("form_internet"):
        c_int1, c_int2, c_int5 = st.columns([2.5, 1, 1]); desc_int = c_int1.text_input("Descrição Web"); unid_int = c_int2.text_input("Un."); qtd_int = c_int5.number_input("Qtd.", step=1.0, key="input_qtd_internet_form") 
        c_int3, c_int4 = st.columns([2, 1]); forn_int = c_int3.text_input("Fornecedor / CNPJ"); val_int = c_int4.number_input("Valor Final (R$)", min_value=0.0, step=0.1); link_int = st.text_input("URL")
        if st.form_submit_button("Inserir no Carrinho"):
            if desc_int and forn_int and val_int > 0:
                novo_item = pd.DataFrame([{'descricao_item': remover_acentos(desc_int), 'produto_mapa': remover_acentos(desc_int).strip(), 'unid_medida': remover_acentos(unid_int), 'valor_unitario': float(val_int), 'municipio': 'LOJA VIRTUAL', 'estado': '-', 'credor': forn_int.upper(), 'data_assinatura': datetime.now().strftime('%d/%m/%Y'), 'id_item': f"INT-{time.time()}", 'link_pncp': link_int, 'origem': 'INTERNET', 'quantidade': float(qtd_int)}])
                st.session_state.carrinho = pd.concat([st.session_state.carrinho, novo_item], ignore_index=True); salvar_carrinho_no_banco(); st.success("Salvo!")
            else: st.error("Dados incompletos.")
                
    st.divider(); st.markdown("### Fechamento de Processo")
    if st.button("Finalizar e Arquivar Pauta", type="primary", use_container_width=True):
        if 'solic_importada' in st.session_state and st.session_state['solic_importada'] is not None:
            id_imp = st.session_state['solic_importada']
            conn = conectar_banco(); conn.execute("UPDATE solicitacoes SET status='FINALIZADA', data_solic=? WHERE id=?", (datetime.now().strftime('%d/%m/%Y %H:%M'), id_imp)); conn.commit(); conn.close()
            st.session_state['solic_importada'] = None; st.session_state.carrinho = pd.DataFrame(); st.session_state['menu_option'] = "3. Histórico e Relatórios"
            time.sleep(0.5); st.rerun()

# ==========================================
# TELA 3: HISTÓRICO E RELATÓRIOS
# ==========================================
elif aba_selecionada == "3. Histórico e Relatórios":
    st.subheader("Processos Concluídos e Relatórios Oficiais")
    conn = conectar_banco()
    try: df_hist = pd.read_sql_query("SELECT * FROM solicitacoes WHERE status='FINALIZADA' ORDER BY id DESC", conn)
    except: df_hist = pd.DataFrame()
        
    if df_hist.empty: st.info("Arquivo vazio.")
    else:
        for _, row in df_hist.iterrows():
            with st.expander(f"Processo: {row['numero_solic']} | Concluído: {row['data_solic']}"):
                st.write(row['secretarias']); c_hist1, c_hist2 = st.columns(2)
                if c_hist1.button("Reabrir Processo", key=f"edit_{row['id']}"):
                    conn.execute("UPDATE solicitacoes SET status='ABERTA' WHERE id=?", (row['id'],)); conn.commit(); st.session_state['solic_importada'] = row['id']; st.session_state['menu_option'] = "2. Painel Central de Cotação (Pesquisa)"; st.rerun()
                if c_hist2.button("Processar PDFs", key=f"pdf_{row['id']}"):
                    df_cart_hist = pd.read_sql_query(f"SELECT dados_json FROM cotacoes_salvas WHERE id_solicitacao={row['id']}", conn)
                    if not df_cart_hist.empty and df_cart_hist['dados_json'].iloc[0]:
                        df_print = pd.read_json(StringIO(df_cart_hist['dados_json'].iloc[0]), orient='records'); config_entidade = get_config_entidade(); lista_sec = row['secretarias'].split('\n')
                        st.session_state[f'pdf_capa_{row["id"]}'] = gerar_pdf_capa(config_entidade, row['numero_solic'], row['objeto'], lista_sec); st.session_state[f'pdf_mapa_{row["id"]}'] = gerar_pdf_mapa(df_print, config_entidade, row['numero_solic'], row['objeto']); st.session_state[f'pdf_pncp_{row["id"]}'] = gerar_pdf_detalhado_pncp(df_print, config_entidade, row['numero_solic'], row['objeto']); st.session_state[f'pdf_link_{row["id"]}'] = gerar_pdf_detalhado_links(df_print, config_entidade, row['numero_solic'], row['objeto'])
                    else: st.error("Sem dados.")
                        
                if f'pdf_capa_{row["id"]}' in st.session_state:
                    dl1, dl2, dl3, dl4 = st.columns(4)
                    dl1.download_button("CAPA", st.session_state[f'pdf_capa_{row["id"]}'], f"Capa_{row['numero_solic']}.pdf", "application/pdf", key=f"dl_capa_{row['id']}")
                    dl2.download_button("MAPA DE PREÇO", st.session_state[f'pdf_mapa_{row["id"]}'], f"Mapa_{row['numero_solic']}.pdf", "application/pdf", type="primary", key=f"dl_mapa_{row['id']}")
                    dl3.download_button("DETALHAMENTO PNCP", st.session_state[f'pdf_pncp_{row["id"]}'], f"PNCP_{row['numero_solic']}.pdf", "application/pdf", key=f"dl_pncp_{row['id']}")
                    dl4.download_button("DETALHAMENTO WEB", st.session_state[f'pdf_link_{row["id"]}'], f"WEB_{row['numero_solic']}.pdf", "application/pdf", key=f"dl_link_{row['id']}")
    conn.close()
