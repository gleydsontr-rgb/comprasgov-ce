import sys
import subprocess
import time

# ==========================================
# 🛡️ INSTALADOR (O BÁSICO QUE FUNCIONA)
# ==========================================
try:
    from bs4 import BeautifulSoup
    import pandas as pd
    import requests
    from google import genai
    from google.genai import types
    import urllib3
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "beautifulsoup4", "pandas", "requests", "google-genai", "streamlit", "urllib3"])
    from bs4 import BeautifulSoup
    import pandas as pd
    import requests
    from google import genai
    from google.genai import types
    import urllib3

import streamlit as st

urllib3.disable_warnings()
st.set_page_config(page_title="ComprasGov PRO | Sniper V15", page_icon="🎯", layout="wide")

CHAVE_API_GOOGLE = "AIzaSyD37nCkJNp_qrSSNHGTRN5Q9keGMm6TixE"

def extrair_preco_com_ia(conteudo_pdf, termo_busca):
    # Trava básica de segurança
    if b'%PDF' not in conteudo_pdf[:10]:
        return "Erro", "O arquivo baixado não é um PDF válido."

    try:
        client = genai.Client(api_key=CHAVE_API_GOOGLE)
        
        # PROMPT DA V10, MAS PEDINDO A LISTA COMPLETA
        prompt = f"""
        Você é um auditor financeiro com visão computacional.
        Analise visualmente as tabelas do documento em anexo e localize o item: '{termo_busca}'.
        
        Liste as variações desse produto encontradas na tabela e seus respectivos VALORES UNITÁRIOS.
        Responda APENAS no formato abaixo (uma linha para cada item):
        - [Nome do Produto Encontrado] : [Valor]
        
        Se não encontrar o item ou o valor nas tabelas, responda a palavra ZERO.
        """
        
        # A MÁGICA DA V10: Mandando o arquivo PDF intacto e puro para a IA
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=[
                types.Part.from_bytes(data=conteudo_pdf, mime_type='application/pdf'),
                prompt
            ]
        )
        
        resultado = response.text.strip()
        
        # Se a IA disser ZERO, a gente avisa na tabela
        if "ZERO" in resultado.upper() and len(resultado) < 10:
            return "---", "A IA analisou visualmente e não encontrou."
            
        return "Extraído com Sucesso", resultado
    except Exception as e:
        return "Erro", f"Falha na API: {str(e)}"

def invadir_link(url_alvo, termo_busca, log_box):
    resultados = []
    log_box.info(f"🕵️ Conectando ao Portal de Licitações...")
    
    try:
        res = requests.get(url_alvo, verify=False, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        links_arquivos = []
        for a_tag in soup.find_all('a', href=True):
            if '.pdf' in a_tag['href'].lower():
                link_completo = a_tag['href'] if a_tag['href'].startswith('http') else "https://www.juazeirodonorte.ce.gov.br/" + a_tag['href'].lstrip('/')
                nome_arquivo = a_tag.get_text(strip=True) or "Contrato/Edital"
                links_arquivos.append((nome_arquivo, link_completo))
        
        if not links_arquivos:
            log_box.warning("Nenhum arquivo PDF encontrado na página.")
            return []
            
        log_box.info(f"🚀 {len(links_arquivos)} PDFs detectados. Acionando Visão Nativa (V10)...")
        
        barra = st.progress(0)
        for i, (nome_arq, link_pdf) in enumerate(links_arquivos):
            barra.progress((i + 1) / len(links_arquivos), text=f"IA visualizando tabelas de: {nome_arq[:30]}...")
            try:
                pdf_res = requests.get(link_pdf, verify=False, timeout=20)
                status, detalhe = extrair_preco_com_ia(pdf_res.content, termo_busca)
                
                resultados.append({
                    "Documento": nome_arq,
                    "Resultado": status,
                    "Visão Nativa da IA": detalhe,
                    "Link Original": link_pdf
                })
            except Exception as e:
                pass
            time.sleep(1) 
            
        barra.empty()
    except Exception as e:
        log_box.error(f"Erro ao acessar o portal: {e}")
        
    return resultados

# ==========================================
# INTERFACE DO USUÁRIO
# ==========================================
st.title("🎯 ComprasGov PRO | Sniper V15 (O Retorno da V10)")
st.markdown("Voltamos ao método original de sucesso: O Gemini recebe o arquivo PDF intacto e usa sua Visão Computacional Nativa para varrer as tabelas.")

c1, c2 = st.columns([1, 2])
with c1:
    item_busca = st.text_input("🟢 O que deseja buscar?", "arroz")
with c2:
    link_prefeitura = st.text_input("🔗 Link da Licitação/Contrato", "https://juazeirodonorte.ce.gov.br/contratos.php?id=5134")

if st.button("🚀 Iniciar Scanner Visual Nativo", type="primary", use_container_width=True):
    if not item_busca or not link_prefeitura:
        st.error("Preencha os campos obrigatórios.")
    else:
        status_log = st.empty()
        dados = invadir_link(link_prefeitura, item_busca, status_log)
        
        if dados:
            status_log.success("Leitura visual concluída!")
            st.dataframe(
                pd.DataFrame(dados), 
                use_container_width=True, 
                column_config={"Link Original": st.column_config.LinkColumn("🔗 Abrir PDF")}
            )
        else:
            status_log.warning("Busca terminada sem resultados.")
