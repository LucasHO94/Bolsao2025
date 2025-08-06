# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v4.8 - Restaura a Geração de PDF)
-------------------------------------------------
Aplicação Streamlit que gera cartas personalizadas de concessão de bolsa
(e calculadora de negociação).

PRÉ-REQUISITOS:
- streamlit
- weasyprint
- gspread
- oauth2client
- Pillow

EXECUÇÃO:
    python -m streamlit run bolsao.py
"""
import io
from datetime import date, timedelta, datetime
from pathlib import Path
import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import weasyprint # Adicionado para gerar o PDF

# --------------------------------------------------
# DADOS DE REFERÊNCIA
# --------------------------------------------------
BOLSA_MAP = {
    0: .30, 1: .30, 2: .30, 3: .35,
    4: .40, 5: .40, 6: .44, 7: .45, 8: .46, 9: .47,
    10: .48, 11: .49, 12: .50, 13: .51, 14: .52,
    15: .53, 16: .54, 17: .55, 18: .56, 19: .57,
    20: .60, 21: .65, 22: .70, 23: .80, 24: 1.00,
}

TUITION = {
    "1ª e 2ª Série EM Militar": {"anuidade": 33036.00, "parcela13": 2541.23},
    "1ª e 2ª Série EM Vestibular": {"anuidade": 33036.00, "parcela13": 2541.23},
    "1º ao 5º Ano": {"anuidade": 24013.00, "parcela13": 1847.15},
    "3ª Série (PV/PM)": {"anuidade": 33164.00, "parcela13": 2551.08},
    "3ª Série EM Medicina": {"anuidade": 33164.00, "parcela13": 2551.08},
    "6º ao 8º Ano": {"anuidade": 28247.00, "parcela13": 2172.85},
    "9º Ano EF II Militar": {"anuidade": 30762.00, "parcela13": 2366.31},
    "9º Ano EF II Vestibular": {"anuidade": 30762.00, "parcela13": 2366.31},
    "AFA/EN/EFOMM": {"anuidade": 13335.00, "parcela13": 1025.77},
    "CN/EPCAr": {"anuidade": 7985.00, "parcela13": 614.23},
    "ESA": {"anuidade": 6437.00, "parcela13": 495.15},
    "EsPCEx": {"anuidade": 13335.00, "parcela13": 1025.77},
    "IME/ITA": {"anuidade": 13335.00, "parcela13": 1025.77},
    "Medicina (Pré)": {"anuidade": 13335.00, "parcela13": 1025.77},
    "Pré-Vestibular": {"anuidade": 13335.00, "parcela13": 1025.77},
}

# LISTA DE UNIDADES COM NOMES COMPLETOS
UNIDADES_COMPLETAS = [
    "COLEGIO E CURSO MATRIZ EDUCACAO CAMPO GRANDE",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO TAQUARA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO BANGU",
    "COLEGIO E CURSO MATRIZ EDUCACAO NOVA IGUACU",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO DUQUE DE CAXIAS",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO SÃO JOÃO DE MERITI",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO ROCHA MIRANDA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO MADUREIRA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO RETIRO DOS ARTISTAS",
    "COLEGIO E CURSO MATRIZ EDUCACAO TIJUCA",
]
# Mapeamento para nomes limpos na interface
UNIDADES_MAP = {name.replace("COLEGIO E CURSO MATRIZ EDUCACAO", "").replace("COLEGIO E CURSO MATRIZ EDUCAÇÃO", "").strip(): name for name in UNIDADES_COMPLETAS}
UNIDADES_LIMPAS = sorted(list(UNIDADES_MAP.keys()))

# PARÂMETRO EDITÁVEL
DESCONTO_MINIMO_PADRAO = 0.60

# --------------------------------------------------
# UTILITÁRIOS
# --------------------------------------------------
def calcula_bolsa(acertos: int) -> float:
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")

# Função para gerar o PDF a partir de um template HTML
def gera_pdf_html(ctx: dict) -> bytes:
    base_dir = Path(__file__).parent
    html_path = base_dir / "carta.html"
    if not html_path.exists():
        st.error(f"O arquivo de template HTML '{html_path}' não foi encontrado.")
        return b""
    with open(html_path, encoding="utf-8") as f:
        html_template = f.read()
    html_renderizado = html_template
    for k, v in ctx.items():
        html_renderizado = html_renderizado.replace(f"{{{{{k}}}}}", str(v))
    html_obj = weasyprint.HTML(string=html_renderizado, base_url=str(base_dir))
    return html_obj.write_pdf()

@st.cache_resource
def get_gspread_client():
    """Autentica e retorna o cliente gspread."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file("credenciais.json", scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Erro de autenticação com o Google Sheets: {e}")
        return None

# Correção do erro de cache: adicionando _ para o parâmetro client
@st.cache_data(ttl=600)  # Cache por 10 minutos
def get_all_hubspot_data(_client):
    """Obtém todos os dados da aba 'Hubspot'."""
    try:
        sheet = _client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
        aba_hubspot = sheet.worksheet("Hubspot")
        df = pd.DataFrame(aba_hubspot.get_all_records())
        return df
    except Exception as e:
        st.error(f"❌ Falha ao carregar todos os dados do Hubspot: {e}")
        return pd.DataFrame()

def get_hubspot_data(client, unidade_completa):
    """Obtém dados da aba 'Hubspot' e filtra pela unidade completa."""
    df = get_all_hubspot_data(client)
    return df[df['Unidade'] == unidade_completa]

def get_limites_data(client):
    """Obtém dados da aba 'Limites' e retorna como dicionário."""
    try:
        sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
        aba_limites = sheet.worksheet("Limites")
        df_limites = pd.DataFrame(aba_limites.get_all_records())
        limites_dict = {}
        for _, row in df_limites.iterrows():
            chave = (row['UNIDADE'], row['GRADE'])
            if row['VALOR LIMITE']:
                valor = float(row['VALOR LIMITE'].replace('R$', '').replace('.', '').replace(',', '.').strip())
                limites_dict[chave] = valor
        return limites_dict
    except Exception as e:
        st.error(f"❌ Falha ao carregar dados de limites: {e}")
        return {}

def calcula_valor_minimo(unidade, serie_modalidade, limites_dict):
    """Calcula o valor mínimo negociável com base na planilha 'Limites' ou em um desconto padrão."""
    chave = (unidade, serie_modalidade)
    if chave in limites_dict:
        return limites_dict[chave]
    else:
        # Se não encontrar na planilha, aplica o desconto padrão de 60%
        valor_integral = TUITION.get(serie_modalidade, {}).get("parcela13", 0)
        return valor_integral * (1 - DESCONTO_MINIMO_PADRAO)

def find_column_index(headers, target_name):
    """Encontra o índice de uma coluna ignorando espaços e case."""
    for i, header in enumerate(headers):
        if header.strip().lower() == target_name.strip().lower():
            return i + 1
    return None

# --------------------------------------------------
# INTERFACE STREAMLIT
# --------------------------------------------------
st.set_page_config(page_title="Gerador de Cartas • Bolsão", layout="centered")
st.title("🎓 Gestor do Bolsão")

# Inicializa o cliente do Google Sheets
client = get_gspread_client()

aba_carta, aba_negociacao, aba_ativacao = st.tabs(["Gerar Carta", "Negociação", "Ativação do Bolsão"])

with aba_carta:
    st.subheader("Gerar Carta")
    
    modo_preenchimento = st.radio(
        "Selecione o modo de preenchimento:",
        ["Preencher manualmente", "Carregar dados de um bolsista"],
        horizontal=True,
        key="modo_preenchimento"
    )

    nome_aluno_pre = ""
    turma_aluno_pre = "1ª série do Ensino Médio Regular"
    unidade_aluno_pre = "BANGU"
    
    # Lógica de pré-preenchimento
    if modo_preenchimento == "Carregar dados de um bolsista":
        if client:
            df_hubspot_all = get_all_hubspot_data(client)
            if not df_hubspot_all.empty:
                unidade_selecionada = st.selectbox(
                    "Selecione a Unidade do bolsista:",
                    UNIDADES_LIMPAS,
                    key="unidade_selecionada_carta"
                )
                
                df_filtrado = df_hubspot_all[df_hubspot_all['Unidade'] == UNIDADES_MAP[unidade_selecionada]]
                nomes_candidatos = ["Selecione um candidato"] + sorted(df_filtrado['Nome do candidato'].tolist())
                
                selecao_candidato = st.selectbox(
                    "Selecione o candidato da lista:",
                    nomes_candidatos,
                    key="selecao_candidato"
                )
                
                if selecao_candidato != "Selecione um candidato":
                    candidato_selecionado = df_filtrado[df_filtrado['Nome do candidato'] == selecao_candidato].iloc[0]
                    nome_aluno_pre = candidato_selecionado.get('Nome do candidato', '')
                    turma_aluno_pre = candidato_selecionado.get('Turma de Interesse - Geral', '1ª série do Ensino Médio Regular')
                    unidade_aluno_pre = unidade_selecionada
                    st.info(f"Dados de {nome_aluno_pre} carregados.")
            else:
                st.warning("Nenhum bolsista encontrado para carregar. Por favor, preencha manualmente.")
    
    st.write("---")
    
    # Campos de entrada da carta (agora não duplicados)
    c1, c2 = st.columns(2)
    with c1:
        unidade_limpa_index = UNIDADES_LIMPAS.index(unidade_aluno_pre) if unidade_aluno_pre in UNIDADES_LIMPAS else 0
        unidade_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, index=unidade_limpa_index, key="c_unid")
        unidade_completa = UNIDADES_MAP[unidade_limpa]
        turma = st.text_input("Turma de interesse", turma_aluno_pre, key="c_turma")
    with c2:
        ac_mat = st.number_input("Acertos - Matemática", 0, 12, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, 12, 0, key="c_port")
    
    aluno = st.text_input("Nome completo do candidato", nome_aluno_pre, key="c_nome")

    if UNIDADES_LIMPAS:
        total = ac_mat + ac_port
        pct = calcula_bolsa(total)
        st.markdown(f"### ➔ Bolsa obtida: **{pct*100:.0f}%** ({total} acertos)")

        serie = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="c_serie")
        val_ano = TUITION[serie]["anuidade"] * (1 - pct)
        val_parc = TUITION[serie]["parcela13"] * (1 - pct)

        if st.button("Gerar Carta PDF", key="c_gerar"):
            if not aluno:
                st.error("Por favor, preencha o nome do candidato.")
            elif client is None:
                st.error("Não foi possível gerar a carta, a conexão com o Google Sheets falhou.")
            else:
                hoje = date.today()
                nome_bolsao = "-"
                try:
                    sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=380208567")
                    aba_bolsao = sheet.worksheet("Bolsão")
                    dados_bolsao = aba_bolsao.get_all_records()
                    for linha in dados_bolsao:
                        data_str = linha.get("Data")
                        bolsao_nome = linha.get("Bolsão")
                        if data_str and bolsao_nome:
                            data_bolsao = datetime.strptime(data_str, "%d/%m/%Y").date()
                            if data_bolsao >= hoje:
                                nome_bolsao = bolsao_nome
                                break
                except Exception as e:
                    st.warning(f"Não foi possível obter nome do bolsão: {e}")
                
                unidades_html = "".join(f"<span class='unidade-item'>{unidade_item}</span>" for unidade_item in UNIDADES_LIMPAS)

                ctx = {
                    "ano": hoje.year,
                    "unidade": f"Colégio Matriz – {unidade_limpa}",
                    "aluno": aluno.strip().title(),
                    "bolsa_pct": f"{pct * 100:.0f}",
                    "acertos_mat": ac_mat,
                    "acertos_port": ac_port,
                    "turma": turma,
                    "n_parcelas": 12,
                    "data_limite": (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
                    "anuidade_vista": format_currency(val_ano * 0.93),
                    "primeira_cota": format_currency(val_parc),
                    "valor_parcela": format_currency(val_parc),
                    "unidades_html": unidades_html,
                }
                
                try:
                    pdf_bytes = gera_pdf_html(ctx)
                    st.success("✅ Carta em PDF gerada com sucesso!")
                    
                    st.download_button(
                        "📄 Baixar Carta",
                        data=pdf_bytes,
                        file_name=f"Carta_Bolsa_{aluno.replace(' ', '_')}.pdf",
                        mime="application/pdf"
                    )
                except Exception as e:
                    st.error(f"❌ Falha ao gerar o PDF da carta: {e}")

                if client:
                    try:
                        sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=380208567")
                        aba_resultados = sheet.worksheet("Resultados_Bolsao")
                        
                        nova_linha = [
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            aluno.strip().title(), unidade_completa, turma,
                            ac_mat, ac_port, total,
                            f"{pct*100:.0f}%", serie,
                            ctx["anuidade_vista"],
                            ctx["primeira_cota"],
                            ctx["valor_parcela"],
                            st.session_state.get("email", "-"),
                            nome_bolsao
                        ]
                        aba_resultados.append_row(nova_linha, value_input_option="USER_ENTERED")
                        st.info("📊 Resposta registrada na planilha.")
                    except Exception as e:
                        st.error(f"❌ Falha ao salvar na planilha: {e}")
    else:
        st.warning("A lista de unidades não pôde ser carregada. Por favor, verifique a lista 'UNIDADES_COMPLETAS' no código.")

with aba_negociacao:
    if client and UNIDADES_LIMPAS:
        st.subheader("Simulador de Negociação")
        
        cn1, cn2 = st.columns(2)
        with cn1:
            unidade_neg_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, key="n_unid")
            unidade_neg_completa = UNIDADES_MAP[unidade_neg_limpa]
            serie_n = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="n_serie")
            parcelas_n = st.radio("Parcelas", [12, 13], horizontal=True, key="n_parc")
        
        # Carrega os limites apenas uma vez
        limites = get_limites_data(client)
        valor_minimo = calcula_valor_minimo(unidade_neg_completa, serie_n, limites)
        
        st.markdown(f"### ➡️ Valor Mínimo Negociável: **{format_currency(valor_minimo)}**")
        st.write("---")

        st.markdown("### Calcular Bolsa ou Parcela?")
        modo_simulacao = st.radio(
            "Selecione o modo de simulação:",
            ["Calcular Valor da Parcela", "Calcular Bolsa Necessária"],
            horizontal=True,
            key="modo_sim"
        )
        
        # Valor integral da parcela, baseado no número de parcelas
        valor_integral_parc = TUITION[serie_n]["parcela13"] if parcelas_n == 13 else TUITION[serie_n]["anuidade"] / 12

        if modo_simulacao == "Calcular Valor da Parcela":
            bolsa_simulada = st.slider("Porcentagem de Bolsa", min_value=0, max_value=100, value=30, step=1, key="bolsa_sim")
            
            valor_resultante = valor_integral_parc * (1 - bolsa_simulada / 100)
            
            st.markdown("---")
            st.metric("Valor da Parcela Resultante", format_currency(valor_resultante))
            if valor_resultante < valor_minimo:
                 st.error("❌ Atenção: O valor resultante está abaixo do valor mínimo negociável!")

        elif modo_simulacao == "Calcular Bolsa Necessária":
            valor_neg = st.number_input("Valor negociado por parcela (R$)", min_value=0.0, value=1500.0, step=10.0, key="valor_neg")
            
            pct_req = max(0.0, 1 - valor_neg / valor_integral_parc) if valor_integral_parc > 0 else 0.0
            
            st.markdown("---")
            bolsa_lanc = int(round(pct_req * 100))
            st.metric("Bolsa Necessária", f"{pct_req*100:.2f}%")
            st.write(f"Sugestão de bolsa a lançar: **{bolsa_lanc}%**")
            
            if valor_neg < valor_minimo:
                 st.error("❌ Atenção: O valor negociado está abaixo do valor mínimo negociável!")

    else:
        st.warning("Não foi possível conectar ao Google Sheets para a negociação.")

with aba_ativacao:
    st.subheader("Ativação de Bolsão")
    
    if client and UNIDADES_LIMPAS:
        unidade_ativacao_limpa = st.selectbox("Selecione a Unidade para Ativação", UNIDADES_LIMPAS, key="a_unid")
        unidade_ativacao_completa = UNIDADES_MAP[unidade_ativacao_limpa]
        
        if st.button("Carregar Lista de Bolsistas", key="a_carregar"):
            df_hubspot = get_hubspot_data(client, unidade_ativacao_completa)
            
            if not df_hubspot.empty:
                st.session_state['df_ativacao'] = df_hubspot
            else:
                st.warning("Nenhum bolsista encontrado para esta unidade.")

        if 'df_ativacao' in st.session_state and not st.session_state['df_ativacao'].empty:
            df_display = st.session_state['df_ativacao']
            
            st.write(f"Lista de bolsistas para a unidade: **{unidade_ativacao_limpa}**")
            
            try:
                sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
                aba_hubspot = sheet.worksheet("Hubspot")
                headers = aba_hubspot.row_values(1)

                nome_candidato_col = find_column_index(headers, 'Nome do candidato')
                contato_realizado_col = find_column_index(headers, 'Contato realizado')
                status_contato_col = find_column_index(headers, 'Status do Contato')
                contato_id_col = find_column_index(headers, 'Contato ID')

            except Exception as e:
                st.error(f"❌ Erro ao ler cabeçalhos da planilha: {e}")
                nome_candidato_col, contato_realizado_col, status_contato_col, contato_id_col = None, None, None, None

            if not all([nome_candidato_col, contato_id_col]):
                 st.warning("⚠️ Algumas colunas essenciais não foram encontradas na sua planilha 'Hubspot'. Por favor, verifique se as colunas 'Nome do candidato', e 'Contato ID' existem. Colunas 'Contato realizado' e 'Status do Contato' também são recomendadas para o funcionamento completo.")
            
            for index, row in df_display.iterrows():
                contato_realizado_valor = str(row.get('Contato realizado', 'Não')).strip().lower() == "sim"
                current_status = str(row.get('Status do Contato', '-')).strip()
                
                status_emoji = "⚪"
                if "confirmado" in current_status.lower():
                    status_emoji = "✅"
                elif "não atende" in current_status.lower():
                    status_emoji = "📞"
                elif "não comparecerá" in current_status.lower():
                    status_emoji = "❌"
                elif contato_realizado_valor:
                    status_emoji = "✅"

                title_expander = f"{status_emoji} **{row.get('Nome do candidato', 'Nome não disponível')}** | Status: **{current_status}** | Contato: {row.get('Celular Tratado', 'Não disponível')}"

                with st.expander(title_expander):
                    col1, col2 = st.columns(2)
                    col1.markdown(f"**Responsável:** {row.get('Nome', 'Não disponível')}")
                    col1.markdown(f"**E-mail:** {row.get('E-mail', 'Não disponível')}")
                    col1.markdown(f"**Celular:** {row.get('Celular Tratado', 'Não disponível')}")
                    col2.markdown(f"**Turma:** {row.get('Turma de Interesse - Geral', 'Não disponível')}")
                    col2.markdown(f"**Fonte Original:** {row.get('Fonte original', 'Não disponível')}")

                    novo_nome = st.text_input("Nome do Candidato", value=row.get('Nome do candidato', ''), key=f"nome_input_{index}")

                    status_options = ["-", "Não atende", "Confirmado", "Não comparecerá"]
                    
                    contato_realizado = st.checkbox("Contato Realizado", key=f"contato_check_{index}", value=contato_realizado_valor)
                    
                    if current_status not in status_options:
                        current_status = '-'
                    
                    status_contato = st.selectbox("Status do Contato", status_options, index=status_options.index(current_status), key=f"status_select_{index}")
                    
                    if st.button("Salvar Status", key=f"salvar_button_{index}"):
                        try:
                            # Tenta encontrar a linha pelo ID do contato
                            cell = aba_hubspot.find(str(row.get('Contato ID', '')), in_column=contato_id_col)
                            if cell:
                                row_idx = cell.row
                                if nome_candidato_col:
                                    aba_hubspot.update_cell(row_idx, nome_candidato_col, novo_nome)
                                if contato_realizado_col:
                                    aba_hubspot.update_cell(row_idx, contato_realizado_col, "Sim" if contato_realizado else "Não") 
                                if status_contato_col:
                                    aba_hubspot.update_cell(row_idx, status_contato_col, status_contato) 
                                
                                st.success(f"Status e nome de {novo_nome} atualizados com sucesso!")
                                st.experimental_rerun()
                            else:
                                st.error("Não foi possível encontrar a linha do candidato na planilha.")
                        except Exception as e:
                            st.error(f"❌ Falha ao atualizar a planilha: {e}")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a ativação do bolsão.")

# --------------------------------------------------
# RODAPÉ / METADADOS
# --------------------------------------------------
st.caption("Desenvolvido para Matriz Educação • Suporte: TI Interno")
