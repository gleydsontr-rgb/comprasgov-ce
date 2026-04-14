import streamlit as st
import sqlite3
import pandas as pd
import unicodedata
import time
import os
import sys
import urllib.parse
from datetime import datetime
from io import BytesIO

# ==========================================
# BIBLIOTECAS EXTERNAS (PDF E VAREJADOR)
# ==========================================
try:
    from fpdf import FPDF
except ImportError:
    st.error("⚠️ Atenção: A biblioteca de PDFs não está instalada. Abra o terminal e digite: pip install fpdf")

try:
    import requests
except ImportError:
    st.error("⚠️ Atenção: A biblioteca requests não está instalada. Abra o terminal e digite: pip install requests")

# ==========================================
# CONFIGURAÇÃO DA PÁGINA E MEMÓRIA
# ==========================================
st.set_page_config(page_title="Sistema Central | ComprasGov", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp { background-color: #f4f6f9; }
    .portal-header {
        background-color: #003366; 
        color: white;
        padding: 15px 20px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        border-bottom: 5px solid #F2A900; 
        margin-top: -80px; 
        margin-bottom: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .portal-title {
        font-size: 26px;
        font-weight: 800;
        margin: 0;
        letter-spacing: 1px;
    }
    .portal-subtitle {
        font-size: 13px;
        font-weight: 400;
        color: #d1e0e0;
        margin-top: 2px;
    }
    h1, h2, h3 { color: #003366 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
</style>
<div class="portal-header">
    <p class="portal-title">SISTEMA INTEGRADO DE GESTÃO DE COMPRAS E LICITAÇÕES</p>
    <p class="portal-subtitle">Painel Administrativo | v4.5 Sincronização Direta de Keys</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# INICIALIZAÇÃO DE VARIÁVEIS DE MEMÓRIA (CHAVES BLINDADAS)
# ==========================================
if 'carrinho' not in st.session_state:
    st.session_state.carrinho = pd.DataFrame()
else:
    if not st.session_state.carrinho.empty and 'origem' not in st.session_state.carrinho.columns:
        st.session_state.carrinho['origem'] = 'PNCP'
    if not st.session_state.carrinho.empty and 'quantidade' not in st.session_state.carrinho.columns:
        st.session_state.carrinho['quantidade'] = 1.0

if 'df_resultados' not in st.session_state: st.session_state.df_resultados = pd.DataFrame()

# A Injeção Direta: Criação das chaves antes dos formulários existirem
if 'p1_busca_form' not in st.session_state: st.session_state['p1_busca_form'] = ""
if 'p2_busca_form' not in st.session_state: st.session_state['p2_busca_form'] = ""
if 'p3_busca_form' not in st.session_state: st.session_state['p3_busca_form'] = ""
if 'input_nome_relatorio_form' not in st.session_state: st.session_state['input_nome_relatorio_form'] = "ITEM DA COTAÇÃO"
if 'input_qtd_relatorio_form' not in st.session_state: st.session_state['input_qtd_relatorio_form'] = 1.0
if 'input_qtd_internet_form' not in st.session_state: st.session_state['input_qtd_internet_form'] = 1.0
if 'ultimo_item_selecionado' not in st.session_state: st.session_state['ultimo_item_selecionado'] = ""

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

def tratar_texto(texto):
    if not texto: return ""
    return str(texto).encode('latin-1', 'replace').decode('latin-1')

# ==========================================
# 📡 BANCO DE DADOS (COM INSPEÇÃO SEGURA)
# ==========================================
def obter_caminho_banco():
    if getattr(sys, 'frozen', False):
        diretorio_base = os.path.dirname(sys.executable)
    else:
        diretorio_base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(diretorio_base, 'banco_compras.db')

def conectar_banco():
    caminho_banco = obter_caminho_banco()
    conn = sqlite3.connect(caminho_banco, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS solicitacoes (id INTEGER PRIMARY KEY AUTOINCREMENT, secretaria TEXT, data_solic TEXT, status TEXT, numero_solic TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS lotes_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_solicitacao INTEGER, nome_lote TEXT, desc_lote TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_lote INTEGER, id_solicitacao INTEGER, descricao TEXT, unid_medida TEXT, quantidade REAL DEFAULT 1.0)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_compras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_item TEXT, descricao_item TEXT, unid_medida TEXT, 
        valor_unitario REAL, municipio TEXT, estado TEXT, 
        credor TEXT, data_assinatura TEXT, link_pncp TEXT, origem TEXT
    )''')
    
    cursor.execute("PRAGMA table_info(solicitacoes)")
    if 'numero_solic' not in [col[1] for col in cursor.fetchall()]:
        try: cursor.execute("ALTER TABLE solicitacoes ADD COLUMN numero_solic TEXT")
        except: pass 

    cursor.execute("PRAGMA table_info(itens_solicitacao)")
    if 'quantidade' not in [col[1] for col in cursor.fetchall()]:
        try: cursor.execute("ALTER TABLE itens_solicitacao ADD COLUMN quantidade REAL DEFAULT 1.0")
        except: pass 
        
    conn.commit()
    return conn

# ==========================================
# 📄 FÁBRICA DE PDFs (COM QUANTIDADES REAIS)
# ==========================================
class RelatorioPDF(FPDF):
    def __init__(self, orgao, processo, tipo_relatorio):
        super().__init__()
        self.orgao = orgao
        self.processo = processo
        self.tipo_relatorio = tipo_relatorio
        
    def header(self):
        self.set_font('Arial', 'B', 14)
        self.cell(0, 8, tratar_texto(self.orgao), 0, 1, 'C')
        self.set_font('Arial', 'B', 10)
        self.cell(0, 5, tratar_texto(self.tipo_relatorio), 0, 1, 'C')
        self.set_font('Arial', '', 9)
        self.cell(0, 5, tratar_texto(f"Processo Nº: {self.processo} - Gerado pelo Sistema ComprasGov CE"), 0, 1, 'C')
        self.line(10, 30, 200, 30)
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, tratar_texto(f'Página {self.page_no()}'), 0, 0, 'C')

def gerar_pdf_capa(orgao, processo, objeto):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 15, tratar_texto(orgao), 0, 1, 'C')
    pdf.set_y(40)
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, tratar_texto("COTAÇÃO DE PREÇOS"), border=1, ln=1, align='C', fill=False)
    pdf.ln(5)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(95, 8, tratar_texto("Nº DO PROCESSO:"), 'L T R', 0, 'L')
    pdf.cell(95, 8, tratar_texto("DATA DO PROCESSO:"), 'L T R', 1, 'L')
    pdf.set_font('Arial', '', 11)
    pdf.cell(95, 8, tratar_texto(processo), 'L B R', 0, 'L')
    pdf.cell(95, 8, tratar_texto(datetime.now().strftime('%d/%m/%Y')), 'L B R', 1, 'L')
    pdf.ln(5)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, tratar_texto("DESCRIÇÃO:"), 'L T R', 1, 'L')
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 6, tratar_texto(objeto), border='L B R', align='L')
    pdf.ln(5)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, tratar_texto("HISTÓRICO:"), 'L T R', 1, 'L')
    pdf.set_font('Arial', '', 11)
    texto_hist = "Contratação para fornecimento de produtos/serviços destinados ao atendimento das necessidades das diversas Secretarias, com pesquisa mercadológica em bancos de preços públicos e privados, em conformidade com a Lei de Licitações."
    pdf.multi_cell(0, 6, tratar_texto(texto_hist), border='L B R', align='J')
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 11)
    pdf.cell(0, 8, tratar_texto("ÓRGÃOS DO PROCESSO:"), 0, 1, 'L')
    pdf.set_font('Arial', '', 10)
    orgaos = ["DEPARTAMENTO MUNICIPAL DE TRÂNSITO", "GABINETE DO PREFEITO", "SECRETARIA MUNICIPAL DE ADMINISTRAÇÃO E FINANÇAS", "SECRETARIA MUNICIPAL DE EDUCAÇÃO", "SECRETARIA MUNICIPAL DE INFRAESTRUTURA", "SECRETARIA MUNICIPAL DE SAÚDE", "SECRETARIA MUNICIPAL DE TRABALHO E ASSISTÊNCIA SOCIAL"]
    for org in orgaos:
        pdf.cell(5, 6, "-", 0, 0, 'R')
        pdf.cell(0, 6, tratar_texto(org), 0, 1, 'L')
    pdf.set_y(-50)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(2)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(0, 6, tratar_texto("Responsável pelo Setor de Compras"), 0, 1, 'C')
    pdf.set_font('Arial', '', 9)
    pdf.cell(0, 5, tratar_texto("Assinatura e Carimbo"), 0, 1, 'C')
    return pdf.output(dest='S').encode('latin-1')

class RelatorioMapaPDF(FPDF):
    def __init__(self, orgao, processo, objeto):
        super().__init__()
        self.orgao = orgao
        self.processo = processo
        self.objeto = objeto
        self.is_resumo = True
        
    def header(self):
        self.set_font('Arial', 'B', 12)
        self.cell(0, 5, tratar_texto(self.orgao), 0, 1, 'C')
        self.set_font('Arial', '', 8)
        self.cell(0, 4, tratar_texto("RUA DR. PAIVA, 415 - VILA MOTA - CEP: 63140-000-ASSARÉ/CE CNPJ: 07.587.983/0001-53"), 0, 1, 'C')
        self.cell(0, 4, tratar_texto("Tel: (88) 9.94194047 - Email: comprasassarece@gmail.com - Site: www.assare.ce.gov.br"), 0, 1, 'C')
        self.ln(2)
        self.set_font('Arial', 'B', 10)
        if self.is_resumo:
            self.cell(0, 5, tratar_texto("RESUMO GERAL DO MAPA DE PREÇO"), 0, 1, 'C')
        else:
            self.cell(0, 5, tratar_texto("MAPA DE PREÇO - DETALHAMENTO POR COLETA"), 0, 1, 'C')
        self.set_font('Arial', 'B', 9)
        self.cell(0, 5, tratar_texto(f"N°: {self.processo} - DATA: {datetime.now().strftime('%d/%m/%Y')}"), 0, 1, 'L')
        if self.is_resumo:
            self.multi_cell(0, 5, tratar_texto(f"ESPECIFICAÇÃO/OBJETO: {self.objeto}"))
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, tratar_texto(f'Página(s): {self.page_no()}'), 0, 0, 'R')

def gerar_pdf_mapa(df_carrinho, orgao, processo, objeto):
    pdf = RelatorioMapaPDF(orgao, processo, objeto)
    if 'produto_mapa' not in df_carrinho.columns:
        df_carrinho['produto_mapa'] = df_carrinho['descricao_item']
        
    grupos = df_carrinho.groupby('produto_mapa')
    
    # --- PÁGINA 1: RESUMO ---
    pdf.is_resumo = True
    pdf.add_page()
    pdf.set_font('Arial', 'B', 8)
    pdf.cell(10, 6, "Item", border=1, align='C')
    pdf.cell(100, 6, tratar_texto("Descrição do item"), border=1, align='C')
    pdf.cell(25, 6, "Unid.", border=1, align='C')
    pdf.cell(15, 6, "Quant.", border=1, align='C')
    pdf.cell(20, 6, tratar_texto("V. Médio"), border=1, align='C')
    pdf.cell(20, 6, "V. Total", border=1, align='C')
    pdf.ln()

    pdf.set_font('Arial', '', 7)
    item_count = 1
    total_geral = 0
    
    for desc_mapa, df_grupo in grupos:
        unid_comum = df_grupo['unid_medida'].mode()[0] if not df_grupo.empty else "UN"
        qtd_item = df_grupo['quantidade'].max() if 'quantidade' in df_grupo.columns else 1.0
        
        media_preco = df_grupo['valor_unitario'].mean()
        valor_total = media_preco * float(qtd_item)
        total_geral += valor_total
        
        desc_limpa = str(desc_mapa)[:65] + "..." if len(str(desc_mapa)) > 65 else str(desc_mapa)
        
        pdf.cell(10, 6, str(item_count), border=1, align='C')
        pdf.cell(100, 6, tratar_texto(desc_limpa), border=1, align='L')
        pdf.cell(25, 6, tratar_texto(unid_comum[:15]), border=1, align='C')
        
        qtd_str = f"{int(qtd_item)}" if float(qtd_item).is_integer() else f"{qtd_item:.2f}"
        
        pdf.cell(15, 6, qtd_str, border=1, align='C')
        pdf.cell(20, 6, f"{media_preco:,.2f}".replace('.', ','), border=1, align='R')
        pdf.cell(20, 6, f"{valor_total:,.2f}".replace('.', ','), border=1, align='R')
        pdf.ln()
        item_count += 1
        
    pdf.set_font('Arial', 'B', 8)
    pdf.cell(170, 6, "TOTAL GERAL DA PAUTA:", border=0, align='R')
    pdf.cell(20, 6, f"{total_geral:,.2f}".replace('.', ','), border=0, align='R')
    
    # --- PÁGINA 2: DETALHES ---
    pdf.is_resumo = False
    pdf.add_page()
    
    for desc_mapa, df_grupo in grupos:
        unid_comum = df_grupo['unid_medida'].mode()[0] if not df_grupo.empty else "UN"
        qtd_item = df_grupo['quantidade'].max() if 'quantidade' in df_grupo.columns else 1.0
        qtd_str = f"{int(qtd_item)}" if float(qtd_item).is_integer() else f"{qtd_item:.2f}"
        
        pdf.set_font('Arial', 'B', 8)
        titulo = f"ITEM: {desc_mapa} - UNID. MEDIDA: {unid_comum} - QUANTIDADE TOTAL: {qtd_str}"
        pdf.multi_cell(0, 5, tratar_texto(titulo))
        pdf.ln(1)
        
        pdf.set_font('Arial', 'B', 7)
        pdf.cell(10, 5, "Pesq.", border=1, align='C')
        pdf.cell(55, 5, "Coleta", border=1, align='C')
        pdf.cell(85, 5, "Fornecedor", border=1, align='L')
        pdf.cell(20, 5, "V. Unit. R$", border=1, align='C')
        pdf.cell(20, 5, "V. Total R$", border=1, align='C')
        pdf.ln()
        
        pdf.set_font('Arial', '', 7)
        pesq_num = 1
        for _, row in df_grupo.iterrows():
            coleta = "LINK DA WEB" if row.get('origem') == 'INTERNET' else "CESTA PREÇOS GOVERNO"
            forn = row['credor'][:48] 
            v_unit = row['valor_unitario']
            v_total = v_unit * float(qtd_item)
            
            pdf.cell(10, 5, str(pesq_num), border=1, align='C')
            pdf.cell(55, 5, tratar_texto(coleta), border=1, align='C')
            pdf.cell(85, 5, tratar_texto(forn), border=1, align='L')
            pdf.cell(20, 5, f"{v_unit:,.2f}".replace('.', ','), border=1, align='R')
            pdf.cell(20, 5, f"{v_total:,.2f}".replace('.', ','), border=1, align='R')
            pdf.ln()
            pesq_num += 1
            
        qtd_pesq = len(df_grupo)
        media_preco = df_grupo['valor_unitario'].mean()
        media_total = media_preco * float(qtd_item)
        
        pdf.ln(1)
        pdf.set_font('Arial', 'B', 7)
        pdf.cell(130, 5, tratar_texto(f"Quantidade de pesquisas válidas: {qtd_pesq}"), align='L')
        pdf.cell(40, 5, tratar_texto(f"Média Unitária: R$ {media_preco:,.2f}".replace('.', ',')), align='L')
        pdf.cell(20, 5, tratar_texto(f"Total: R$ {media_total:,.2f}".replace('.', ',')), align='R')
        pdf.ln(8)
        
    return pdf.output(dest='S').encode('latin-1')

def gerar_pdf_detalhado_pncp(df_carrinho, orgao, processo, objeto):
    pdf = RelatorioPDF(orgao, processo, "RELATÓRIO DETALHADO DE PREÇOS - GOVERNO")
    pdf.add_page()
    pdf.set_font('Arial', 'B', 10)
    pdf.multi_cell(0, 6, tratar_texto(f"OBJETO DA COMPRA: {objeto}"))
    pdf.ln(5)
    df_pncp = df_carrinho[df_carrinho['origem'] != 'INTERNET']
    if df_pncp.empty:
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 10, tratar_texto("Nenhuma cotação do banco público adicionada."), 0, 1, 'C')
        return pdf.output(dest='S').encode('latin-1')
    for index, row in df_pncp.iterrows():
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(230, 230, 230)
        pdf.multi_cell(0, 6, tratar_texto(f"ITEM: {row['descricao_item']} (Unid: {row['unid_medida']})"), 1, 'L', fill=True)
        pdf.set_font('Arial', '', 8)
        info_texto = f"Fornecedor: {row['credor']}\nLocalidade: {row['municipio']} - {row['estado']}\nData: {row['data_assinatura']}\nValor: R$ {row['valor_unitario']:.2f}\nLink PNCP: {row['link_pncp']}"
        pdf.multi_cell(0, 5, tratar_texto(info_texto), 1, 'L')
        pdf.ln(2)
    return pdf.output(dest='S').encode('latin-1')

def gerar_pdf_detalhado_links(df_carrinho, orgao, processo, objeto):
    pdf = RelatorioPDF(orgao, processo, "RELATÓRIO DETALHADO DE PREÇOS - LINKS DA INTERNET")
    pdf.add_page()
    pdf.set_font('Arial', 'B', 10)
    pdf.multi_cell(0, 6, tratar_texto(f"OBJETO DA COMPRA: {objeto}"))
    pdf.ln(5)
    df_int = df_carrinho[df_carrinho['origem'] == 'INTERNET']
    if df_int.empty:
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 10, tratar_texto("Nenhuma cotação da internet adicionada."), 0, 1, 'C')
        return pdf.output(dest='S').encode('latin-1')
    for index, row in df_int.iterrows():
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(230, 230, 230)
        pdf.multi_cell(0, 6, tratar_texto(f"ITEM: {row['descricao_item']} (Unid: {row['unid_medida']})"), 1, 'L', fill=True)
        pdf.set_font('Arial', '', 8)
        info_texto = f"Fornecedor (Loja Virtual): {row['credor']}\nValor Total c/ Frete: R$ {row['valor_unitario']:.2f}\nLink do Anúncio: {row['link_pncp']}"
        pdf.multi_cell(0, 5, tratar_texto(info_texto), 1, 'L')
        pdf.ln(2)
    return pdf.output(dest='S').encode('latin-1')

# ==========================================
# 🛒 MENU LATERAL E CARRINHO
# ==========================================
st.sidebar.title("🛒 Carrinho de Cotação")

if not st.session_state.carrinho.empty:
    st.sidebar.success(f"Você tem {len(st.session_state.carrinho)} cotações separadas.")
    st.sidebar.dataframe(st.session_state.carrinho[['produto_mapa', 'valor_unitario', 'quantidade']], hide_index=True)
    if st.sidebar.button("🗑️ Esvaziar Carrinho", key="btn_esvaziar"):
        st.session_state.carrinho = pd.DataFrame()
        if 'pdfs_prontos' in st.session_state: del st.session_state['pdfs_prontos']
        st.rerun()
    
    st.sidebar.divider()
    st.sidebar.subheader("📄 Gerar Processo Oficial")
    with st.sidebar.form("form_pdf"):
        nome_orgao = st.text_input("Órgão Comprador", value="PREFEITURA MUNICIPAL DE ASSARÉ")
        numero_proc = st.text_input("Nº do Processo", value="2026.03.23-0001")
        desc_objeto = st.text_area("Descrição do Objeto", value="FORNECIMENTO DE GÊNEROS ALIMENTÍCIOS")
        preparar_doc = st.form_submit_button("🔨 Preparar Documentos (Com Quantidades)")
        
    if preparar_doc:
        try:
            st.session_state['pdfs_prontos'] = {
                'capa': gerar_pdf_capa(nome_orgao, numero_proc, desc_objeto),
                'mapa': gerar_pdf_mapa(st.session_state.carrinho, nome_orgao, numero_proc, desc_objeto),
                'pncp': gerar_pdf_detalhado_pncp(st.session_state.carrinho, nome_orgao, numero_proc, desc_objeto),
                'link': gerar_pdf_detalhado_links(st.session_state.carrinho, nome_orgao, numero_proc, desc_objeto),
                'numero_proc': numero_proc
            }
        except Exception as e:
            st.sidebar.error(f"Erro ao gerar PDFs: {e}")

    if 'pdfs_prontos' in st.session_state:
        st.sidebar.success("✅ Kit de Licitação Pronto!")
        proc_salvo = st.session_state['pdfs_prontos']['numero_proc']
        st.sidebar.download_button("1️⃣ CAPA DO PROCESSO", data=st.session_state['pdfs_prontos']['capa'], file_name=f"1_Capa_{proc_salvo}.pdf", mime="application/pdf", key="dl_capa")
        st.sidebar.download_button("2️⃣ MAPA DE PREÇOS", data=st.session_state['pdfs_prontos']['mapa'], file_name=f"2_Mapa_{proc_salvo}.pdf", mime="application/pdf", type="primary", key="dl_mapa")
        st.sidebar.download_button("3️⃣ RELATÓRIO PNCP", data=st.session_state['pdfs_prontos']['pncp'], file_name=f"3_Rel_PNCP_{proc_salvo}.pdf", mime="application/pdf", key="dl_pncp")
        st.sidebar.download_button("4️⃣ RELATÓRIO INTERNET", data=st.session_state['pdfs_prontos']['link'], file_name=f"4_Rel_Internet_{proc_salvo}.pdf", mime="application/pdf", key="dl_link")
else:
    st.sidebar.info("Carrinho vazio. Pesquise e adicione cotações para gerar os relatórios oficiais.")

# ==========================================
# 🗂️ MÓDULOS DE NAVEGAÇÃO
# ==========================================
aba_selecionada = st.radio("Escolha o Módulo:", ["📝 1. Cadastro de Solicitação (Planejamento)", "📊 2. Painel Central de Cotação (Pesquisa)"], horizontal=True, label_visibility="collapsed")

# ==========================================
# TELA 1: SOLICITAÇÃO E IMPORTADOR
# ==========================================
if aba_selecionada == "📝 1. Cadastro de Solicitação (Planejamento)":
    
    c_z1, c_z2 = st.columns([4, 1])
    if c_z2.button("⚠️ Zerar Banco de Solicitações", key="btn_zerar_banco"):
        conn = conectar_banco()
        conn.execute("DROP TABLE IF EXISTS solicitacoes")
        conn.execute("DROP TABLE IF EXISTS lotes_solicitacao")
        conn.execute("DROP TABLE IF EXISTS itens_solicitacao")
        conn.commit()
        conn.close()
        conectar_banco()
        if 'solic_importada' in st.session_state: del st.session_state['solic_importada']
        st.success("✅ Estrutura do banco corrigida e limpa! Pode importar a pauta novamente.")
        st.rerun()

    st.markdown("### 📥 Importação Automática de Pautas Consolidadas")
    with st.expander("Clique aqui para enviar uma Planilha (Excel/CSV) e extrair os itens", expanded=False):
        arquivo_pauta = st.file_uploader("Selecione o arquivo da Pauta (Ex: PAUTA.csv)", type=["csv", "xlsx"], key="file_up_pauta")
        
        if arquivo_pauta:
            try:
                if arquivo_pauta.name.endswith('.csv'):
                    try:
                        df_pauta = pd.read_csv(arquivo_pauta, sep=None, engine='python', on_bad_lines='skip', encoding='utf-8')
                    except Exception:
                        arquivo_pauta.seek(0)
                        df_pauta = pd.read_csv(arquivo_pauta, sep=None, engine='python', on_bad_lines='skip', encoding='latin-1')
                else:
                    try:
                        df_pauta = pd.read_excel(arquivo_pauta)
                    except ImportError:
                        st.error("⚠️ Falta a biblioteca para ler Excel. O openpyxl deve ser instalado.")
                        st.stop()
                
                idx_header = None
                for i, row in df_pauta.iterrows():
                    if row.astype(str).str.contains('ESPECIFICAÇÃO', case=False, na=False).any() or row.astype(str).str.contains('LOTE', case=False, na=False).any():
                        idx_header = i
                        break
                
                if idx_header is not None:
                    df_pauta.columns = df_pauta.iloc[idx_header]
                    df_pauta = df_pauta.iloc[idx_header+1:].dropna(how='all')
                
                novas_colunas = []
                for c in df_pauta.columns:
                    nome_limpo = str(c).strip().upper()
                    if nome_limpo == 'NAN' or nome_limpo == '':
                        nome_limpo = 'VAZIO'
                    base = nome_limpo
                    contador = 1
                    while nome_limpo in novas_colunas:
                        nome_limpo = f"{base}_{contador}"
                        contador += 1
                    novas_colunas.append(nome_limpo)
                        
                df_pauta.columns = novas_colunas
                
                st.success("✅ Planilha lida com sucesso! Mapeie as colunas abaixo:")
                
                c_map1, c_map2 = st.columns(2)
                nome_solic_auto = c_map1.text_input("Nome desta Solicitação Geral:", value=f"PAUTA CONSOLIDADA - {arquivo_pauta.name.split('.')[0]}", key="input_nome_pauta")
                col_lote = c_map2.selectbox("Coluna do Lote (Opcional):", ["Sem Lote"] + list(df_pauta.columns), index=1 if "LOTE" in df_pauta.columns else 0, key="sel_col_lote")
                
                c_map3, c_map4, c_map5 = st.columns(3)
                idx_desc = list(df_pauta.columns).index("ESPECIFICAÇÃO") if "ESPECIFICAÇÃO" in df_pauta.columns else 0
                idx_unid = list(df_pauta.columns).index("UNID.") if "UNID." in df_pauta.columns else 0
                
                idx_total = len(df_pauta.columns) - 1
                for i, col in enumerate(df_pauta.columns):
                    if col == "TOTAL":
                        idx_total = i
                        break
                
                col_desc = c_map3.selectbox("Coluna da Descrição do Item:", df_pauta.columns, index=idx_desc, key="sel_col_desc")
                col_unid = c_map4.selectbox("Coluna da Unidade de Medida:", df_pauta.columns, index=idx_unid, key="sel_col_unid")
                col_qtd = c_map5.selectbox("Coluna da Quantidade TOTAL:", df_pauta.columns, index=idx_total, key="sel_col_qtd")
                
                if st.button("🚀 Processar Pauta e Salvar no Banco", key="btn_proc_pauta"):
                    conn = conectar_banco()
                    num_gerado = f"PAUTA-{datetime.now().strftime('%m%d%H%M')}"
                    
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO solicitacoes (numero_solic, secretaria, data_solic, status) VALUES (?, ?, ?, ?)", (num_gerado, nome_solic_auto.upper(), datetime.now().strftime('%d/%m/%Y'), 'ABERTA'))
                    id_solic_master = cursor.lastrowid
                    
                    if col_lote != "Sem Lote":
                        grupos_lote = df_pauta.groupby(col_lote)
                    else:
                        grupos_lote = [("LOTE ÚNICO", df_pauta)]
                        
                    for nome_lote, df_grupo in grupos_lote:
                        lote_limpo = str(nome_lote).upper().strip()
                        if not lote_limpo or lote_limpo == 'NAN' or 'VAZIO' in lote_limpo: continue
                        
                        cursor.execute("INSERT INTO lotes_solicitacao (id_solicitacao, nome_lote, desc_lote) VALUES (?, ?, ?)", (id_solic_master, lote_limpo, ""))
                        id_lote_master = cursor.lastrowid
                        
                        for _, row_item in df_grupo.iterrows():
                            desc_val = str(row_item[col_desc]).strip().upper()
                            unid_val = str(row_item[col_unid]).strip().upper()
                            qtd_val = pd.to_numeric(row_item[col_qtd], errors='coerce')
                            
                            if desc_val and desc_val != 'NAN' and 'VAZIO' not in desc_val and pd.notna(qtd_val) and qtd_val > 0:
                                try:
                                    cursor.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida, quantidade) VALUES (?, ?, ?, ?, ?)", (id_lote_master, id_solic_master, desc_val, unid_val, float(qtd_val)))
                                except sqlite3.OperationalError:
                                    cursor.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida) VALUES (?, ?, ?, ?)", (id_lote_master, id_solic_master, desc_val, unid_val))
                    
                    conn.commit()
                    conn.close()
                    st.success("✅ Pauta Consolidada importada com sucesso! Você já pode ir para a Aba 2 (Painel Central) cotar os itens.")
                    
            except Exception as e:
                st.error(f"Erro ao processar o arquivo. Detalhe técnico: {e}")

    st.divider()

    with st.expander("✏️ OPÇÃO MANUAL: Cadastrar Solicitação Secretaria por Secretaria", expanded=False):
        nome_sec = st.text_input("Nome da Secretaria (Ex: SECRETARIA DE EDUCAÇÃO)", key="input_nome_sec_man")
        if st.button("Criar Nova Solicitação", key="btn_nova_solic_man"):
            if nome_sec:
                num_gerado = f"{datetime.now().strftime('%Y.%m%d%H%M')}"
                conn = conectar_banco()
                conn.execute("INSERT INTO solicitacoes (numero_solic, secretaria, data_solic, status) VALUES (?, ?, ?, ?)", (num_gerado, nome_sec.upper(), datetime.now().strftime('%d/%m/%Y'), 'ABERTA'))
                conn.commit()
                conn.close()
                st.success(f"Solicitação criada!")
                st.rerun()

    conn = conectar_banco()
    try: df_solic = pd.read_sql_query("SELECT * FROM solicitacoes WHERE status='ABERTA'", conn)
    except: df_solic = pd.DataFrame() 
    
    if not df_solic.empty:
        st.subheader("📋 VISUALIZAR E ADICIONAR ITENS")
        solic_selecionada = st.selectbox("Selecione a Solicitação no Banco", df_solic['id'].astype(str) + " - " + df_solic['secretaria'], key="sel_solic_banco")
        id_solic = int(solic_selecionada.split(" - ")[0])
        
        c_lote, c_item = st.columns(2)
        with c_lote:
            with st.form("form_lote", clear_on_submit=True):
                nome_lote = st.text_input("Nome do Lote")
                if st.form_submit_button("Criar Lote Manual"):
                    if nome_lote:
                        conn.execute("INSERT INTO lotes_solicitacao (id_solicitacao, nome_lote, desc_lote) VALUES (?, ?, ?)", (id_solic, nome_lote.upper(), ""))
                        conn.commit()
                        st.rerun()
        
        df_lotes = pd.read_sql_query(f"SELECT * FROM lotes_solicitacao WHERE id_solicitacao={id_solic}", conn)
        with c_item:
            if not df_lotes.empty:
                with st.form("form_item", clear_on_submit=True):
                    lote_selec = st.selectbox("Lote", df_lotes['id'].astype(str) + " - " + df_lotes['nome_lote'])
                    id_lote = int(lote_selec.split(" - ")[0])
                    desc_item = st.text_area("Descrição")
                    ci_1, ci_2 = st.columns(2)
                    unid_item = ci_1.text_input("Unid.")
                    qtd_item = ci_2.number_input("Qtd", min_value=1.0)
                    if st.form_submit_button("Inserir Item Manual"):
                        if desc_item and unid_item:
                            try:
                                conn.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida, quantidade) VALUES (?, ?, ?, ?, ?)", (id_lote, id_solic, desc_item.upper(), unid_item.upper(), qtd_item))
                            except sqlite3.OperationalError:
                                conn.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida) VALUES (?, ?, ?, ?)", (id_lote, id_solic, desc_item.upper(), unid_item.upper()))
                            conn.commit()
                            st.rerun()
                            
        try:
            df_bruto = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.* FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_solic}", conn)
        except Exception:
            df_bruto = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_solic}", conn)
            
        df_itens = pd.DataFrame()
        if not df_bruto.empty:
            df_itens['Lote'] = df_bruto['Lote']
            df_itens['Produto'] = df_bruto['Produto']
            df_itens['Unid'] = df_bruto['Unid']
            df_itens['Qtd'] = df_bruto['quantidade'] if 'quantidade' in df_bruto.columns else 1.0
            st.dataframe(df_itens, use_container_width=True, hide_index=True)
    conn.close()

# ==========================================
# TELA 2: COTAÇÃO E PESQUISA
# ==========================================
elif aba_selecionada == "📊 2. Painel Central de Cotação (Pesquisa)":
    
    st.subheader("📥 1. Escolher Itens da Pauta")
    conn = conectar_banco()
    try: df_todas_solic = pd.read_sql_query("SELECT * FROM solicitacoes", conn)
    except: df_todas_solic = pd.DataFrame()
        
    if not df_todas_solic.empty:
        c_imp1, c_imp2 = st.columns([4, 1])
        solic_escolhida = c_imp1.selectbox("Selecione a Pauta/Solicitação para Cotar:", df_todas_solic['id'].astype(str) + " - " + df_todas_solic['secretaria'], key="sel_pauta_cotar")
        id_solic_imp = int(solic_escolhida.split(" - ")[0])
        
        if c_imp2.button("📥 Carregar Planilha", use_container_width=True, key="btn_load_pauta"):
            st.session_state['solic_importada'] = id_solic_imp
            st.rerun()

    df_itens_imp = pd.DataFrame()
    if 'solic_importada' in st.session_state:
        id_imp = st.session_state['solic_importada']
        
        try:
            df_bruto_imp = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.* FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_imp}", conn)
        except Exception:
            df_bruto_imp = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_imp}", conn)
            
        if not df_bruto_imp.empty:
            df_itens_imp['Lote'] = df_bruto_imp['Lote']
            df_itens_imp['Produto'] = df_bruto_imp['Produto']
            df_itens_imp['Unid'] = df_bruto_imp['Unid']
            df_itens_imp['Qtd'] = df_bruto_imp['quantidade'] if 'quantidade' in df_bruto_imp.columns else 1.0

            st.markdown("### 📋 Planilha Extraída")
            st.dataframe(df_itens_imp, use_container_width=True, hide_index=True)
            
            lista_produtos = df_itens_imp['Produto'].tolist()
            item_selecionado = st.selectbox("🎯 Selecione um item da planilha acima para Cotar Preços:", [""] + lista_produtos, key="sel_item_pauta")
            
            # --- O HACK DA SINCRONIZAÇÃO DIRETA NA MEMÓRIA ---
            if item_selecionado != st.session_state['ultimo_item_selecionado']:
                st.session_state['ultimo_item_selecionado'] = item_selecionado
                if item_selecionado:
                    stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS', 'SEM', 'TIPO', 'KG', 'UND', 'PCT', 'CX', 'UNID', 'LOTE']
                    texto_limpo = remover_acentos(item_selecionado).replace('-', ' ').replace(',', ' ').replace('.', ' ')
                    palavras = [p for p in texto_limpo.split() if p not in stopwords and len(p) > 1]
                    
                    # Injeção DIRETA na Key da caixa de texto
                    st.session_state['p1_busca_form'] = palavras[0] if len(palavras) > 0 else ""
                    st.session_state['p2_busca_form'] = palavras[1] if len(palavras) > 1 else ""
                    st.session_state['p3_busca_form'] = ""
                    
                    st.session_state['input_nome_relatorio_form'] = item_selecionado
                    qtd_extraida = float(df_itens_imp[df_itens_imp['Produto'] == item_selecionado]['Qtd'].iloc[0])
                    st.session_state['input_qtd_relatorio_form'] = qtd_extraida
                    st.session_state['input_qtd_internet_form'] = qtd_extraida
                else:
                    st.session_state['p1_busca_form'] = ""
                    st.session_state['p2_busca_form'] = ""
                    st.session_state['p3_busca_form'] = ""
                    st.session_state['input_nome_relatorio_form'] = "ITEM DA COTAÇÃO"
                    st.session_state['input_qtd_relatorio_form'] = 1.0
                    st.session_state['input_qtd_internet_form'] = 1.0
                st.rerun() # Força a tela a piscar e puxar os dados injetados
                
            if item_selecionado:
                st.success(f"✔️ Item: **{item_selecionado}** | 📦 Quantidade Total: **{st.session_state['input_qtd_relatorio_form']}**")
    
    conn.close()
    st.divider()

    st.subheader("2. Buscar no Banco do Governo (PNCP)")

    with st.form("form_consulta"):
        c1, c2, c3, c4 = st.columns(4)
        
        # Caixas cegas: Elas não têm o parâmetro 'value', elas obedecem unicamente à 'key' injetada acima
        p1 = c1.text_input("Palavra Principal", key="p1_busca_form")
        p2 = c2.text_input("Contendo também (1)", key="p2_busca_form")
        p3 = c3.text_input("Contendo também (2)", key="p3_busca_form")
        p_excluir = c4.text_input("🚫 NÃO pode conter", key="pex_busca")
        
        c5, c6, c7, c8, c9 = st.columns([2, 1.5, 1.5, 1.5, 1.5])
        modo_busca = c5.selectbox("🧠 Inteligência da Busca", ["🔍 Ampla (Qualquer parte do texto)", "🎯 Inteligente (Focar no Nome do Produto)"], key="sel_int_busca")
        dt_ini = c6.date_input("Data inicial", value=datetime(2025, 1, 1), format="DD/MM/YYYY", key="dt_ini_busca")
        dt_fim = c7.date_input("Data final", format="DD/MM/YYYY", key="dt_fim_busca")
        val_ini = c8.number_input("Valor mínimo (R$)", min_value=0.0, step=1.0, key="val_min_busca")
        val_fim = c9.number_input("Valor máximo (R$)", min_value=0.0, step=1.0, key="val_max_busca")
        
        c10, c11, c12 = st.columns([1.5, 3, 1.5])
        uf = c10.selectbox("UF", ["TODAS", "CE", "AC", "AL", "AP", "AM", "BA", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"], index=1, key="sel_uf_busca")
        relevancia = c11.text_input("Busca Exata (A frase exata precisa estar no texto)", key="input_exata_busca")
        ordem = c12.selectbox("Ordenar por", ["DATA RECENTE", "MENOR PREÇO", "MAIOR PREÇO"], key="sel_ordem_busca")
        
        submit = st.form_submit_button("🔎 Consultar Banco")

    # ==========================================
    # VAREJADOR IA (BLINDADO CONTRA ACENTOS DO GOVERNO)
    # ==========================================
    def acionar_varejador(termo_busca, df_local_existente):
        with st.spinner(f"🌐 Varejador IA trabalhando para: '{termo_busca}'..."):
            time.sleep(1.5) 
            df_varejador = pd.DataFrame()
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'pt-BR,pt;q=0.9'}
            
            stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS', 'SEM']
            palavras = [p for p in remover_acentos(termo_busca).split() if p not in stopwords]
            if not palavras: return
            
            termo_url = urllib.parse.quote_plus(" ".join(palavras))
            
            try:
                url_api = f"https://pncp.gov.br/api/search/?q={termo_url}&tipos_documento=item"
                resposta = requests.get(url_api, headers=headers, timeout=15)
                itens_api = []
                if resposta.status_code == 200:
                    dados = resposta.json()
                    itens_api = dados.get('items', [])
                
                lista_vars = []
                for i, it in enumerate(itens_api[:100]):
                    titulo_bruto = str(it.get('title', '')).upper()
                    # A MÁGICA: Limpa o acento do Governo ANTES de verificar
                    titulo_limpo = remover_acentos(titulo_bruto)
                    
                    if all(p in titulo_limpo for p in palavras):
                        valor_est = float(it.get('valorUnitarioEstimado', 0))
                        if valor_est > 0:
                            lista_vars.append({'descricao_item': titulo_bruto, 'unid_medida': 'UN', 'valor_unitario': valor_est, 'municipio': 'DADOS NACIONAIS', 'estado': 'BR', 'credor': 'FORNECEDOR VIA VAREJADOR', 'data_assinatura': datetime.now().strftime('%d/%m/%Y'), 'id_item': f"VAR-{int(time.time())}-{i}", 'link_pncp': str(it.get('linkSistemaOrigem', 'https://pncp.gov.br')), 'origem': 'VAREJADOR NACIONAL'})
                if lista_vars: df_varejador = pd.DataFrame(lista_vars)
            except Exception: pass
                
            if not df_varejador.empty:
                if not df_local_existente.empty:
                    df_local_existente['municipio'] = df_local_existente['municipio'].fillna('Não Informado')
                    df_local_existente['data_assinatura'] = pd.to_datetime(df_local_existente['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                    df_final = pd.concat([df_local_existente, df_varejador], ignore_index=True)
                else: df_final = df_varejador
                df_final.insert(0, 'Selecionar', False)
                st.session_state.df_resultados = df_final
                st.success("✅ Varejador IA completou a lista.")
            else:
                if not df_local_existente.empty:
                    df_local_existente.insert(0, 'Selecionar', False)
                    df_local_existente['municipio'] = df_local_existente['municipio'].fillna('Não Informado')
                    df_local_existente['data_assinatura'] = pd.to_datetime(df_local_existente['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                    st.session_state.df_resultados = df_local_existente
                    st.warning("⚠️ O Varejador IA não achou correspondência exata nacional.")
                else:
                    st.session_state.df_resultados = pd.DataFrame()
                    st.error("❌ Nada encontrado.")

    if submit:
        conn = conectar_banco()
        query = "SELECT id_item, descricao_item, unid_medida, valor_unitario, municipio, estado, credor, data_assinatura, link_pncp, origem FROM itens_compras WHERE valor_unitario > 0"
        
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

        query = aplicar_busca(p1, query)
        query = aplicar_busca(p2, query)
        query = aplicar_busca(p3, query)
        query = aplicar_busca(p_excluir, query, operador="NOT")
        if relevancia: query += f" AND descricao_item LIKE '%{remover_acentos(relevancia).strip()}%'"
        if uf != "TODAS": query += f" AND estado = '{uf}'"
        if val_ini > 0: query += f" AND valor_unitario >= {val_ini}"
        if val_fim > 0: query += f" AND valor_unitario <= {val_fim}"
        query += f" AND data_assinatura >= '{dt_ini.strftime('%Y-%m-%d')}' AND data_assinatura <= '{dt_fim.strftime('%Y-%m-%d')}'"
        
        if ordem == "MENOR PREÇO": query += " ORDER BY valor_unitario ASC"
        elif ordem == "MAIOR PREÇO": query += " ORDER BY valor_unitario DESC"
        else: query += " ORDER BY data_assinatura DESC"
        query += " LIMIT 1500"
        
        try:
            df = pd.read_sql_query(query, conn)
            if not df.empty:
                df = df.drop_duplicates(subset=['descricao_item', 'valor_unitario', 'credor', 'link_pncp'])
                if p1 and "Inteligente" in modo_busca:
                    termo_principal = remover_acentos(p1).strip()
                    stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS']
                    palavras = [p for p in termo_principal.split() if p not in stopwords]
                    if palavras:
                        primeira_palavra = palavras[0]
                        df = df[df['descricao_item'].str[:40].str.contains(primeira_palavra, na=False)]
                        for p in palavras[1:]:
                            if len(p) <= 3: df = df[df['descricao_item'].str.contains(rf'\b{p}\b', regex=True, na=False)]
                            else: df = df[df['descricao_item'].str.contains(p, na=False)]
            
            if not df.empty and len(df) >= 3:
                df.insert(0, 'Selecionar', False)
                df['municipio'] = df['municipio'].fillna('Não Informado')
                df['data_assinatura'] = pd.to_datetime(df['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                st.session_state.df_resultados = df
            else:
                # O VAREJADOR AGORA JUNTA AS 3 CAIXAS DE BUSCA
                termo_completo = f"{p1} {p2} {p3}".strip()
                termo_varejo = remover_acentos(termo_completo) if termo_completo else "ITEM"
                acionar_varejador(termo_varejo, df)

        except Exception as e:
            st.error(f"Erro no banco: {e}")
        conn.close()

    if not st.session_state.df_resultados.empty:
        colunas_mostrar = ['Selecionar', 'descricao_item', 'unid_medida', 'valor_unitario', 'municipio', 'estado', 'credor', 'data_assinatura', 'id_item', 'link_pncp', 'origem']
        df_exibicao = st.session_state.df_resultados[colunas_mostrar]
        
        df_editado = st.data_editor(
            df_exibicao,
            use_container_width=True, hide_index=True, height=350,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("✅", required=True),
                "descricao_item": st.column_config.TextColumn("Descrição", width="large"),
                "valor_unitario": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f"),
                "data_assinatura": st.column_config.TextColumn("Data", width="small"),
                "id_item": None, "origem": None, 
                "link_pncp": st.column_config.LinkColumn("Link do Edital", display_text="🔗 Acessar")
            }
        )
        
        c_add1, c_add2, c_add3 = st.columns([3, 1.5, 2])
        nome_grupo = c_add1.text_input("📝 Nome para o Relatório:", key="input_nome_relatorio_form")
        qtd_grupo = c_add2.number_input("📦 Quantidade Final:", step=1.0, key="input_qtd_relatorio_form")
        
        if c_add3.button("➕ ADICIONAR SELECIONADOS AO CARRINHO", type="primary", use_container_width=True, key="btn_add_carrinho"):
            selecionados = df_editado[df_editado['Selecionar'] == True].copy()
            selecionados = selecionados.drop(columns=['Selecionar'])
            if not selecionados.empty:
                selecionados['produto_mapa'] = remover_acentos(nome_grupo).strip()
                selecionados['quantidade'] = float(qtd_grupo) 
                st.session_state.carrinho = pd.concat([st.session_state.carrinho, selecionados]).drop_duplicates(subset=['id_item'])
                st.session_state['ultimo_item_selecionado'] = ""
                st.rerun()
            else:
                st.warning("Selecione pelo menos um item.")

    st.divider()
    st.subheader("3. Adicionar Cotação da Internet (Manual)")
    with st.form("form_internet"):
        c_int1, c_int2, c_int5 = st.columns([2.5, 1, 1])
        desc_int = c_int1.text_input("Descrição")
        unid_int = c_int2.text_input("Unidade")
        qtd_int = c_int5.number_input("Quantidade", step=1.0, key="input_qtd_internet_form") 
        
        c_int3, c_int4 = st.columns([2, 1])
        forn_int = c_int3.text_input("Loja e CNPJ")
        val_int = c_int4.number_input("Valor Final (R$)", min_value=0.0, step=0.1)
        link_int = st.text_input("Link da Internet")
        add_int = st.form_submit_button("➕ Adicionar Cotação Web")

        if add_int:
            if desc_int and forn_int and val_int > 0:
                novo_item = pd.DataFrame([{
                    'descricao_item': remover_acentos(desc_int), 'produto_mapa': remover_acentos(desc_int).strip(), 
                    'unid_medida': remover_acentos(unid_int), 'valor_unitario': float(val_int),
                    'municipio': 'LOJA VIRTUAL', 'estado': '-', 'credor': forn_int.upper(),
                    'data_assinatura': datetime.now().strftime('%d/%m/%Y'), 'id_item': f"INT-{int(time.time())}",
                    'link_pncp': link_int, 'origem': 'INTERNET', 'quantidade': float(qtd_int)
                }])
                st.session_state.carrinho = pd.concat([st.session_state.carrinho, novo_item], ignore_index=True)
                st.success("✅ Cotação da internet adicionada!")
