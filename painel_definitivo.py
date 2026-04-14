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

# ==========================================
# 🎨 INJEÇÃO DE CSS (O VISUAL CORPORATIVO TIPO SIGAA)
# ==========================================
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
    div[role="radiogroup"] {
        flex-direction: row;
        background-color: #e0e4e8;
        padding: 10px;
        border-radius: 4px;
        border: 1px solid #c0c8d1;
        justify-content: center;
        gap: 20px;
    }
    h1, h2, h3 { color: #003366 !important; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
    .stButton>button[data-baseweb="button"] { border-radius: 4px; font-weight: bold; }
</style>
<div class="portal-header">
    <p class="portal-title">SISTEMA INTEGRADO DE GESTÃO DE COMPRAS E LICITAÇÕES</p>
    <p class="portal-subtitle">Painel Administrativo | Controle de Cotações e Planejamento | v2.3 Varejador Camuflado</p>
</div>
""", unsafe_allow_html=True)

# ==========================================
# INICIALIZAÇÃO DE VARIÁVEIS DE MEMÓRIA
# ==========================================
if 'carrinho' not in st.session_state:
    st.session_state.carrinho = pd.DataFrame()
else:
    if not st.session_state.carrinho.empty and 'origem' not in st.session_state.carrinho.columns:
        st.session_state.carrinho['origem'] = 'PNCP'

if 'df_resultados' not in st.session_state:
    st.session_state.df_resultados = pd.DataFrame()

def remover_acentos(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').upper()

def tratar_texto(texto):
    if not texto: return ""
    return str(texto).encode('latin-1', 'replace').decode('latin-1')

# ==========================================
# 📡 GPS DE DIRETÓRIO E BANCO DE DADOS
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
    cursor.execute('''CREATE TABLE IF NOT EXISTS solicitacoes (id INTEGER PRIMARY KEY AUTOINCREMENT, secretaria TEXT, data_solic TEXT, status TEXT)''')
    try:
        cursor.execute("ALTER TABLE solicitacoes ADD COLUMN numero_solic TEXT")
    except sqlite3.OperationalError:
        pass 
        
    cursor.execute('''CREATE TABLE IF NOT EXISTS lotes_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_solicitacao INTEGER, nome_lote TEXT, desc_lote TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS itens_solicitacao (id INTEGER PRIMARY KEY AUTOINCREMENT, id_lote INTEGER, id_solicitacao INTEGER, descricao TEXT, unid_medida TEXT, quantidade REAL)''')
    conn.commit()
    
    return conn

# ==========================================
# 📄 FÁBRICA DE PDFs (INTACTA)
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
    texto_hist = "Contratação para fornecimento de produtos/serviços destinados ao atendimento das necessidades das diversas Secretarias do Município, com pesquisa mercadológica em bancos de preços públicos e privados, em conformidade com a Lei de Licitações."
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
        data_atual = datetime.now().strftime('%d/%m/%Y')
        self.cell(0, 5, tratar_texto(f"N°: {self.processo} - DATA: {data_atual}"), 0, 1, 'L')
        
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
    
    pdf.is_resumo = True
    pdf.add_page()
    
    pdf.set_font('Arial', 'B', 8)
    pdf.cell(10, 6, "Item", border=1, align='C')
    pdf.cell(100, 6, tratar_texto("Descrição do item"), border=1, align='C')
    pdf.cell(25, 6, "Unid. de medida", border=1, align='C')
    pdf.cell(15, 6, "Quantidade", border=1, align='C')
    pdf.cell(20, 6, tratar_texto("Valor médio"), border=1, align='C')
    pdf.cell(20, 6, "Valor total", border=1, align='C')
    pdf.ln()

    pdf.set_font('Arial', '', 7)
    item_count = 1
    total_geral = 0
    
    for desc_mapa, df_grupo in grupos:
        unid_comum = df_grupo['unid_medida'].mode()[0] if not df_grupo.empty else "UN"
        media_preco = df_grupo['valor_unitario'].mean()
        valor_total = media_preco * 1 
        total_geral += valor_total
        
        desc_limpa = str(desc_mapa)[:65] + "..." if len(str(desc_mapa)) > 65 else str(desc_mapa)
        
        pdf.cell(10, 6, str(item_count), border=1, align='C')
        pdf.cell(100, 6, tratar_texto(desc_limpa), border=1, align='L')
        pdf.cell(25, 6, tratar_texto(unid_comum[:15]), border=1, align='C')
        pdf.cell(15, 6, "1", border=1, align='C')
        pdf.cell(20, 6, f"{media_preco:,.2f}".replace('.', ','), border=1, align='R')
        pdf.cell(20, 6, f"{valor_total:,.2f}".replace('.', ','), border=1, align='R')
        pdf.ln()
        item_count += 1
        
    pdf.set_font('Arial', 'B', 8)
    pdf.cell(170, 6, "TOTAL GERAL:", border=0, align='R')
    pdf.cell(20, 6, f"{total_geral:,.2f}".replace('.', ','), border=0, align='R')
    
    pdf.is_resumo = False
    pdf.add_page()
    
    for desc_mapa, df_grupo in grupos:
        unid_comum = df_grupo['unid_medida'].mode()[0] if not df_grupo.empty else "UN"
        
        pdf.set_font('Arial', 'B', 8)
        titulo = f"ITEM: {desc_mapa} - UNID. MEDIDA.: {unid_comum}"
        pdf.multi_cell(0, 5, tratar_texto(titulo))
        pdf.ln(1)
        
        pdf.set_font('Arial', 'B', 7)
        pdf.cell(10, 5, "Pesq.", border=1, align='C')
        pdf.cell(55, 5, "Coleta", border=1, align='C')
        pdf.cell(70, 5, "Fornecedor", border=1, align='L')
        pdf.cell(15, 5, "Quant.", border=1, align='C')
        pdf.cell(20, 5, "Valor Unit. R$", border=1, align='C')
        pdf.cell(20, 5, "Valor total R$", border=1, align='C')
        pdf.ln()
        
        pdf.set_font('Arial', '', 7)
        pesq_num = 1
        for _, row in df_grupo.iterrows():
            coleta = "LINK DA WEB" if row.get('origem') == 'INTERNET' else "CESTA DE PREÇOS ACEITÁVEIS"
            forn = row['credor'][:40] 
            
            v_unit = row['valor_unitario']
            v_total = v_unit * 1 
            
            pdf.cell(10, 5, str(pesq_num), border=1, align='C')
            pdf.cell(55, 5, tratar_texto(coleta), border=1, align='C')
            pdf.cell(70, 5, tratar_texto(forn), border=1, align='L')
            pdf.cell(15, 5, "1", border=1, align='C')
            pdf.cell(20, 5, f"{v_unit:,.2f}".replace('.', ','), border=1, align='R')
            pdf.cell(20, 5, f"{v_total:,.2f}".replace('.', ','), border=1, align='R')
            pdf.ln()
            pesq_num += 1
            
        qtd = len(df_grupo)
        media_preco = df_grupo['valor_unitario'].mean()
        media_total = media_preco * 1
        
        pdf.ln(1)
        pdf.set_font('Arial', 'B', 7)
        pdf.cell(135, 5, tratar_texto(f"Quantidade de pesquisas: {qtd}"), align='L')
        pdf.cell(35, 5, tratar_texto(f"Média de preço unit: {media_preco:,.2f}".replace('.', ',')), align='L')
        pdf.cell(20, 5, tratar_texto(f"Média de preço unit: {media_total:,.2f}".replace('.', ',')), align='R')
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
# 🛒 MENU LATERAL E PDF DOWNLOADS
# ==========================================
st.sidebar.title("🛒 Carrinho de Cotação")

if not st.session_state.carrinho.empty:
    st.sidebar.success(f"Você tem {len(st.session_state.carrinho)} itens separados.")
    st.sidebar.dataframe(st.session_state.carrinho[['produto_mapa', 'valor_unitario']], hide_index=True)
    if st.sidebar.button("🗑️ Esvaziar Carrinho"):
        st.session_state.carrinho = pd.DataFrame()
        if 'pdfs_prontos' in st.session_state:
            del st.session_state['pdfs_prontos']
        st.rerun()
    
    st.sidebar.divider()
    st.sidebar.subheader("📄 Gerar Processo Oficial")
    with st.sidebar.form("form_pdf"):
        nome_orgao = st.text_input("Órgão Comprador", value="PREFEITURA MUNICIPAL DE ASSARÉ")
        numero_proc = st.text_input("Nº do Processo", value="2026.03.23-0001")
        desc_objeto = st.text_area("Descrição do Objeto", value="FORNECIMENTO DE GÊNEROS ALIMENTÍCIOS")
        preparar_doc = st.form_submit_button("🔨 Preparar os 4 Documentos")
        
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
        
        st.sidebar.download_button("1️⃣ Baixar CAPA DO PROCESSO", data=st.session_state['pdfs_prontos']['capa'], file_name=f"1_Capa_{proc_salvo}.pdf", mime="application/pdf")
        st.sidebar.download_button("2️⃣ Baixar MAPA DE PREÇOS", data=st.session_state['pdfs_prontos']['mapa'], file_name=f"2_Mapa_{proc_salvo}.pdf", mime="application/pdf", type="primary")
        st.sidebar.download_button("3️⃣ Baixar RELATÓRIO PNCP", data=st.session_state['pdfs_prontos']['pncp'], file_name=f"3_Rel_PNCP_{proc_salvo}.pdf", mime="application/pdf")
        st.sidebar.download_button("4️⃣ Baixar RELATÓRIO INTERNET", data=st.session_state['pdfs_prontos']['link'], file_name=f"4_Rel_Internet_{proc_salvo}.pdf", mime="application/pdf")

else:
    st.sidebar.info("Carrinho vazio. Pesquise ou adicione links para gerar relatórios.")

# ==========================================
# 🗂️ O MENU DE NAVEGAÇÃO "CORPORATIVO"
# ==========================================
aba_selecionada = st.radio(
    "Escolha o Módulo:",
    ["📝 1. Cadastro de Solicitação (Planejamento)", "📊 2. Painel Central de Cotação (Pesquisa)"],
    horizontal=True,
    label_visibility="collapsed"
)

# ==========================================
# TELA 1: SOLICITAÇÃO (PLANEJAMENTO)
# ==========================================
if aba_selecionada == "📝 1. Cadastro de Solicitação (Planejamento)":
    
    c_z1, c_z2 = st.columns([4, 1])
    if c_z2.button("⚠️ Zerar Solicitações"):
        conn = conectar_banco()
        conn.execute("DELETE FROM solicitacoes")
        conn.execute("DELETE FROM lotes_solicitacao")
        conn.execute("DELETE FROM itens_solicitacao")
        conn.commit()
        conn.close()
        if 'solic_importada' in st.session_state: del st.session_state['solic_importada']
        st.success("Tudo zerado! Vamos recomeçar.")
        st.rerun()
    
    with st.expander("1️⃣ CADASTRAR NOVA SOLICITAÇÃO DA SECRETARIA", expanded=True):
        nome_sec = st.text_input("Nome da Secretaria Solicitante (Ex: SECRETARIA DE EDUCAÇÃO)")
        if st.button("Criar Nova Solicitação"):
            if nome_sec:
                num_gerado = f"{datetime.now().strftime('%Y.%m%d%H%M')}"
                conn = conectar_banco()
                conn.execute("INSERT INTO solicitacoes (numero_solic, secretaria, data_solic, status) VALUES (?, ?, ?, ?)", (num_gerado, nome_sec.upper(), datetime.now().strftime('%d/%m/%Y'), 'ABERTA'))
                conn.commit()
                conn.close()
                st.success(f"Solicitação Nº {num_gerado} criada com sucesso!")
                st.rerun()

    conn = conectar_banco()
    try:
        df_solic = pd.read_sql_query("SELECT * FROM solicitacoes WHERE status='ABERTA'", conn)
    except:
        df_solic = pd.DataFrame() 
    
    if not df_solic.empty:
        st.divider()
        st.subheader("2️⃣ ADICIONAR LOTES E ITENS")
        solic_selecionada = st.selectbox("Selecione a Solicitação Ativa", df_solic['id'].astype(str) + " - Nº " + df_solic.get('numero_solic', '') + " (" + df_solic['secretaria'] + ")")
        id_solic = int(solic_selecionada.split(" - ")[0])
        
        c_lote, c_item = st.columns(2)
        
        with c_lote:
            st.markdown("### Criar Novo Lote")
            with st.form("form_lote", clear_on_submit=True):
                nome_lote = st.text_input("Nome do Lote (Ex: LOTE 01 - MATERIAL DE LIMPEZA)")
                desc_lote = st.text_area("Descrição Opcional do Lote")
                if st.form_submit_button("Salvar Lote"):
                    if nome_lote:
                        conn.execute("INSERT INTO lotes_solicitacao (id_solicitacao, nome_lote, desc_lote) VALUES (?, ?, ?)", (id_solic, nome_lote.upper(), desc_lote.upper()))
                        conn.commit()
                        st.success("Lote adicionado!")
                        st.rerun()
        
        df_lotes = pd.read_sql_query(f"SELECT * FROM lotes_solicitacao WHERE id_solicitacao={id_solic}", conn)
        with c_item:
            st.markdown("### Adicionar Item ao Lote")
            if not df_lotes.empty:
                with st.form("form_item", clear_on_submit=True):
                    lote_selec = st.selectbox("Selecione o Lote", df_lotes['id'].astype(str) + " - " + df_lotes['nome_lote'])
                    id_lote = int(lote_selec.split(" - ")[0])
                    
                    desc_item = st.text_area("Descrição Completa do Item")
                    ci_1, ci_2 = st.columns(2)
                    unid_item = ci_1.text_input("Unid. Medida (Ex: UN, PCT)")
                    qtd_item = ci_2.number_input("Quantidade Solicitada", min_value=1.0, step=1.0)
                    
                    if st.form_submit_button("Salvar Item"):
                        if desc_item and unid_item:
                            conn.execute("INSERT INTO itens_solicitacao (id_lote, id_solicitacao, descricao, unid_medida, quantidade) VALUES (?, ?, ?, ?, ?)", (id_lote, id_solic, desc_item.upper(), unid_item.upper(), qtd_item))
                            conn.commit()
                            st.success("Item cadastrado!")
                            st.rerun()
            else:
                st.info("Crie um Lote primeiro para poder adicionar itens.")
                
        st.divider()
        st.subheader("3️⃣ ITENS DA SOLICITAÇÃO (RESUMO)")
        df_itens = pd.read_sql_query(f"""
            SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.quantidade as Qtd 
            FROM itens_solicitacao i 
            JOIN lotes_solicitacao l ON i.id_lote = l.id 
            WHERE i.id_solicitacao={id_solic}
        """, conn)
        
        if not df_itens.empty:
            st.dataframe(df_itens, use_container_width=True, hide_index=True)
            st.success("✅ Planejamento salvo com sucesso. Suba e clique na Aba '2. Painel Central' para pesquisar.")
        else:
            st.info("Nenhum item cadastrado nesta solicitação.")
            
    conn.close()

# ==========================================
# TELA 2: COTAÇÃO E PESQUISA (COM VAREJADOR NACIONAL AUTOMÁTICO)
# ==========================================
elif aba_selecionada == "📊 2. Painel Central de Cotação (Pesquisa)":
    
    st.subheader("📥 1. Importar Itens da Solicitação")
    conn = conectar_banco()
    try:
        df_todas_solic = pd.read_sql_query("SELECT * FROM solicitacoes", conn)
    except:
        df_todas_solic = pd.DataFrame()
        
    if not df_todas_solic.empty:
        c_imp1, c_imp2 = st.columns([4, 1])
        solic_escolhida = c_imp1.selectbox("Selecione a Solicitação para importar:", df_todas_solic['id'].astype(str) + " - Nº " + df_todas_solic.get('numero_solic', '') + " (" + df_todas_solic['secretaria'] + ")")
        id_solic_imp = int(solic_escolhida.split(" - ")[0])
        
        if c_imp2.button("📥 Importar Itens", use_container_width=True):
            st.session_state['solic_importada'] = id_solic_imp
            st.rerun()
    else:
        st.info("Nenhuma solicitação cadastrada na Aba 1.")

    item_para_cotar = ""
    if 'solic_importada' in st.session_state:
        id_imp = st.session_state['solic_importada']
        df_itens_imp = pd.read_sql_query(f"SELECT l.nome_lote as Lote, i.descricao as Produto, i.unid_medida as Unid, i.quantidade as Qtd FROM itens_solicitacao i JOIN lotes_solicitacao l ON i.id_lote = l.id WHERE i.id_solicitacao={id_imp}", conn)
        
        if not df_itens_imp.empty:
            st.markdown("### 📋 Planilha de Itens da Solicitação")
            st.dataframe(df_itens_imp, use_container_width=True, hide_index=True)
            
            lista_produtos = df_itens_imp['Produto'].tolist()
            item_para_cotar = st.selectbox("🎯 Selecione um item da planilha acima para Cotar Preços:", [""] + lista_produtos)
            if item_para_cotar:
                st.success(f"✔️ Item selecionado! O Painel de Pesquisa abaixo foi preenchido automaticamente com: **{item_para_cotar}**")
    
    conn.close()
    st.divider()

    st.subheader("2. Buscar no Banco do Governo (PNCP)")

    with st.form("form_consulta"):
        valor_padrao_p1 = ""
        if item_para_cotar:
            stopwords = ['DE', 'DO', 'DA', 'EM', 'COM', 'PARA', 'E', 'OU', 'A', 'O', 'AS', 'OS']
            palavras = [p for p in remover_acentos(item_para_cotar).split() if p not in stopwords]
            valor_padrao_p1 = palavras[0] if palavras else remover_acentos(item_para_cotar)
            
        c1, c2, c3, c4 = st.columns(4)
        p1 = c1.text_input("Palavra Principal", value=valor_padrao_p1, placeholder="Ex: DETERGENTE")
        p2 = c2.text_input("Contendo também", placeholder="Ex: NEUTRO")
        p3 = c3.text_input("Contendo também", placeholder="Ex: 500ML")
        p_excluir = c4.text_input("🚫 NÃO pode conter", placeholder="Ex: AGENTE")
        
        c5, c6, c7, c8, c9 = st.columns([2, 1.5, 1.5, 1.5, 1.5])
        modo_busca = c5.selectbox("🧠 Inteligência da Busca", ["🎯 Inteligente (Focar no Nome do Produto)", "🔍 Ampla (Qualquer parte do texto)"])
        dt_ini = c6.date_input("Data inicial", value=datetime(2025, 1, 1), format="DD/MM/YYYY")
        dt_fim = c7.date_input("Data final", format="DD/MM/YYYY")
        val_ini = c8.number_input("Valor mínimo (R$)", min_value=0.0, step=1.0)
        val_fim = c9.number_input("Valor máximo (R$)", min_value=0.0, step=1.0)
        
        c10, c11, c12 = st.columns([1.5, 3, 1.5])
        uf = c10.selectbox("UF", ["TODAS", "CE", "AC", "AL", "AP", "AM", "BA", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"], index=1)
        relevancia = c11.text_input("Busca Exata (A frase exata precisa estar no texto)")
        ordem = c12.selectbox("Ordenar por", ["DATA RECENTE", "MENOR PREÇO", "MAIOR PREÇO"])
        
        submit = st.form_submit_button("🔎 Consultar Banco")

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
        
        if relevancia: 
            query += f" AND descricao_item LIKE '%{remover_acentos(relevancia).strip()}%'"
            
        if uf != "TODAS": query += f" AND estado = '{uf}'"
        if val_ini > 0: query += f" AND valor_unitario >= {val_ini}"
        if val_fim > 0: query += f" AND valor_unitario <= {val_fim}"
        
        query += f" AND data_assinatura >= '{dt_ini.strftime('%Y-%m-%d')}'"
        query += f" AND data_assinatura <= '{dt_fim.strftime('%Y-%m-%d')}'"
        
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
                            if len(p) <= 3:
                                df = df[df['descricao_item'].str.contains(rf'\b{p}\b', regex=True, na=False)]
                            else:
                                df = df[df['descricao_item'].str.contains(p, na=False)]
                
                if not df.empty and len(df) >= 3:
                    df.insert(0, 'Selecionar', False)
                    df['municipio'] = df['municipio'].fillna('Não Informado')
                    df['data_assinatura'] = pd.to_datetime(df['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                    
                    st.session_state.df_resultados = df
                else:
                    qtd_local = len(df) if not df.empty else 0
                    if qtd_local > 0:
                        st.warning(f"⚠️ Encontramos apenas {qtd_local} resultado(s) no Ceará. O mapa exige 3 cotações.")
                    else:
                        st.warning("⚠️ O Filtro Inteligente não encontrou resultados exatos no Ceará.")
                        
                    with st.spinner("🌐 Acionando o Varejador Nacional... Buscando preços em outros estados do Brasil para completar o mapa!"):
                        time.sleep(1.5) 
                        termo_varejo = remover_acentos(p1) if p1 else "ITEM"
                        # O DISFARCE DO VAREJADOR: Codificação oficial e Cabecalho Chrome
                        termo_varejo_url = urllib.parse.quote(termo_varejo)
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            'Accept': 'application/json, text/plain, */*',
                            'Accept-Language': 'pt-BR,pt;q=0.9'
                        }
                        
                        df_varejador = pd.DataFrame()
                        try:
                            url_api = f"https://pncp.gov.br/api/search/?q={termo_varejo_url}&tipos_documento=item"
                            resposta = requests.get(url_api, headers=headers, timeout=15)
                            if resposta.status_code == 200:
                                dados = resposta.json()
                                itens_api = dados.get('items', [])
                                lista_vars = []
                                for i, it in enumerate(itens_api[:50]):
                                    valor_est = float(it.get('valorUnitarioEstimado', 0))
                                    if valor_est > 0:
                                        lista_vars.append({
                                            'descricao_item': str(it.get('title', termo_varejo)).upper(),
                                            'unid_medida': 'UN',
                                            'valor_unitario': valor_est,
                                            'municipio': 'DADOS NACIONAIS',
                                            'estado': 'BR',
                                            'credor': 'FORNECEDOR VIA VAREJADOR',
                                            'data_assinatura': datetime.now().strftime('%d/%m/%Y'),
                                            'id_item': f"VAR-{int(time.time())}-{i}",
                                            'link_pncp': str(it.get('linkSistemaOrigem', 'https://pncp.gov.br')),
                                            'origem': 'VAREJADOR NACIONAL'
                                        })
                                if lista_vars:
                                    df_varejador = pd.DataFrame(lista_vars)
                        except Exception as e:
                            pass
                            
                        if not df_varejador.empty:
                            if not df.empty:
                                df['municipio'] = df['municipio'].fillna('Não Informado')
                                df['data_assinatura'] = pd.to_datetime(df['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                                df_final = pd.concat([df, df_varejador], ignore_index=True)
                            else:
                                df_final = df_varejador
                                
                            df_final.insert(0, 'Selecionar', False)
                            st.session_state.df_resultados = df_final
                            st.success("✅ O Varejador encontrou resultados a nível Nacional e completou a lista! Selecione os itens abaixo.")
                        else:
                            if not df.empty:
                                df.insert(0, 'Selecionar', False)
                                df['municipio'] = df['municipio'].fillna('Não Informado')
                                df['data_assinatura'] = pd.to_datetime(df['data_assinatura'], errors='coerce').dt.strftime('%d/%m/%Y')
                                st.session_state.df_resultados = df
                                st.error("❌ O Varejador Nacional não encontrou mais itens. Tente usar palavras mais genéricas para completar o mapa.")
                            else:
                                st.session_state.df_resultados = pd.DataFrame()
                                st.error("❌ O Varejador Nacional também não encontrou este item. Tente usar palavras mais genéricas.")
            else:
                st.warning("⚠️ Nenhum item encontrado no Ceará.")
                with st.spinner("🌐 Acionando o Varejador Nacional... Buscando preços em outros estados do Brasil!"):
                    time.sleep(1.5)
                    termo_varejo = remover_acentos(p1) if p1 else "ITEM"
                    # O DISFARCE DO VAREJADOR: Codificação oficial e Cabecalho Chrome
                    termo_varejo_url = urllib.parse.quote(termo_varejo)
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'pt-BR,pt;q=0.9'
                    }
                    
                    df_varejador = pd.DataFrame()
                    try:
                        url_api = f"https://pncp.gov.br/api/search/?q={termo_varejo_url}&tipos_documento=item"
                        resposta = requests.get(url_api, headers=headers, timeout=15)
                        if resposta.status_code == 200:
                            dados = resposta.json()
                            itens_api = dados.get('items', [])
                            lista_vars = []
                            for i, it in enumerate(itens_api[:50]):
                                valor_est = float(it.get('valorUnitarioEstimado', 0))
                                if valor_est > 0:
                                    lista_vars.append({
                                        'descricao_item': str(it.get('title', termo_varejo)).upper(),
                                        'unid_medida': 'UN',
                                        'valor_unitario': valor_est,
                                        'municipio': 'DADOS NACIONAIS',
                                        'estado': 'BR',
                                        'credor': 'FORNECEDOR VIA VAREJADOR',
                                        'data_assinatura': datetime.now().strftime('%d/%m/%Y'),
                                        'id_item': f"VAR-{int(time.time())}-{i}",
                                        'link_pncp': str(it.get('linkSistemaOrigem', 'https://pncp.gov.br')),
                                        'origem': 'VAREJADOR NACIONAL'
                                    })
                        if lista_vars:
                            df_varejador = pd.DataFrame(lista_vars)
                    except Exception as e:
                        pass
                        
                    if not df_varejador.empty:
                        df_varejador.insert(0, 'Selecionar', False)
                        st.session_state.df_resultados = df_varejador
                        st.success("✅ O Varejador encontrou resultados a nível Nacional! Selecione os itens abaixo.")
                    else:
                        st.session_state.df_resultados = pd.DataFrame()
                        st.error("❌ O Varejador Nacional também não encontrou este item. Tente usar palavras mais genéricas.")

        except Exception as e:
            st.error(f"Erro no banco: {e}")
        conn.close()

    if not st.session_state.df_resultados.empty:
        st.success(f"✅ Encontramos {len(st.session_state.df_resultados)} itens únicos.")
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
        
        c_add1, c_add2 = st.columns([2, 1])
        valor_padrao_grupo = item_para_cotar if item_para_cotar else (remover_acentos(p1).upper() if 'p1' in locals() and p1 else "ITEM DA COTAÇÃO")
        nome_grupo = c_add1.text_input("📝 Nome do Produto para agrupar as médias no Mapa de Preços:", value=valor_padrao_grupo)
        
        if c_add2.button("➕ ADICIONAR SELECIONADOS AO CARRINHO", type="primary", use_container_width=True):
            selecionados = df_editado[df_editado['Selecionar'] == True].copy()
            selecionados = selecionados.drop(columns=['Selecionar'])
            if not selecionados.empty:
                selecionados['produto_mapa'] = remover_acentos(nome_grupo).strip()
                st.session_state.carrinho = pd.concat([st.session_state.carrinho, selecionados]).drop_duplicates(subset=['id_item'])
                st.rerun()
            else:
                st.warning("Selecione pelo menos um item.")

    st.divider()
    st.subheader("3. Adicionar Cotação da Internet (Manual)")
    with st.form("form_internet"):
        c_int1, c_int2 = st.columns([3, 1])
        desc_int = c_int1.text_input("Descrição (Escreva IGUAL ao do Governo para agrupar a média)")
        unid_int = c_int2.text_input("Unidade (Ex: UN, CX, KG)")
        c_int3, c_int4 = st.columns([2, 1])
        forn_int = c_int3.text_input("Nome da Loja e CNPJ")
        val_int = c_int4.number_input("Valor Final (R$)", min_value=0.0, step=0.1)
        link_int = st.text_input("Link da Internet (Copie e cole a URL do produto)")
        add_int = st.form_submit_button("➕ Adicionar Cotação Web ao Carrinho")

        if add_int:
            if desc_int and forn_int and val_int > 0:
                novo_item = pd.DataFrame([{
                    'descricao_item': remover_acentos(desc_int),
                    'produto_mapa': remover_acentos(desc_int).strip(), 
                    'unid_medida': remover_acentos(unid_int),
                    'valor_unitario': float(val_int),
                    'municipio': 'LOJA VIRTUAL',
                    'estado': '-',
                    'credor': forn_int.upper(),
                    'data_assinatura': datetime.now().strftime('%d/%m/%Y'), 
                    'id_item': f"INT-{int(time.time())}",
                    'link_pncp': link_int,
                    'origem': 'INTERNET'
                }])
                st.session_state.carrinho = pd.concat([st.session_state.carrinho, novo_item], ignore_index=True)
                st.success("✅ Cotação da internet adicionada! Ela aparecerá no carrinho e no Relatório de Links.")
            else:
                st.error("Preencha a descrição, o fornecedor e o valor para adicionar a cotação.")
