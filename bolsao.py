# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v5.1 - Versão Unificada com Observações)
-------------------------------------------------
Aplicação Streamlit que gera cartas, gerencia negociações e ativações de bolsão,
utilizando WeasyPrint para PDF e Pandas para manipulação de dados.
"""
import io
from datetime import date, timedelta, datetime
from pathlib import Path
import streamlit as st
import pandas as pd
import weasyprint
from google.oauth2.service_account import Credentials
import gspread

# --------------------------------------------------
# DADOS DE REFERÊNCIA E CONFIGURAÇÕES
# --------------------------------------------------
BOLSA_MAP = {
    0: .30, 1: .30, 2: .30, 3: .35,
    4: .40, 5: .40, 6: .44, 7: .45, 8: .46, 9: .47,
    10: .48, 11: .49, 12: .50, 13: .51, 14: .52,
    15: .53, 16: .54, 17: .55, 18: .56, 19: .57,
    20: .60, 21: .65, 22: .70, 23: .80, 24: 1.00,
}

TUITION = {
    "1ª e 2ª Série EM Militar": {"anuidade": 36339.60, "parcela13": 2795.35},
    "1ª e 2ª Série EM Vestibular": {"anuidade": 36339.60, "parcela13": 2795.35},
    "1º ao 5º Ano": {"anuidade": 26414.30, "parcela13": 2031.87},
    "3ª Série (PV/PM)": {"anuidade": 36480.40, "parcela13": 2806.19},
    "3ª Série EM Medicina": {"anuidade": 36480.40, "parcela13": 2806.19},
    "6º ao 8º Ano": {"anuidade": 31071.70, "parcela13": 2390.14},
    "9º Ano EF II Militar": {"anuidade": 33838.20, "parcela13": 2602.94},
    "9º Ano EF II Vestibular": {"anuidade": 33838.20, "parcela13": 2602.94},
    "AFA/EN/EFOMM": {"anuidade": 14668.50, "parcela13": 1128.35},
    "CN/EPCAr": {"anuidade": 8783.50, "parcela13": 675.65},
    "ESA": {"anuidade": 7080.70, "parcela13": 544.67},
    "EsPCEx": {"anuidade": 14668.50, "parcela13": 1128.35},
    "IME/ITA": {"anuidade": 14668.50, "parcela13": 1128.35},
    "Medicina (Pré)": {"anuidade": 14668.50, "parcela13": 1128.35},
    "Pré-Vestibular": {"anuidade": 14668.50, "parcela13": 1128.35},
}

# Sistema de Unidades aprimorado
UNIDADES_COMPLETAS = [
    "COLEGIO E CURSO MATRIZ EDUCACAO CAMPO GRANDE", "COLEGIO E CURSO MATRIZ EDUCAÇÃO TAQUARA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO BANGU", "COLEGIO E CURSO MATRIZ EDUCACAO NOVA IGUACU",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO DUQUE DE CAXIAS", "COLEGIO E CURSO MATRIZ EDUCAÇÃO SÃO JOÃO DE MERITI",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO ROCHA MIRANDA", "COLEGIO E CURSO MATRIZ EDUCAÇÃO MADUREIRA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO RETIRO DOS ARTISTAS", "COLEGIO E CURSO MATRIZ EDUCACAO TIJUCA",
]
UNIDADES_MAP = {name.replace("COLEGIO E CURSO MATRIZ EDUCACAO", "").replace("COLEGIO E CURSO MATRIZ EDUCAÇÃO", "").strip(): name for name in UNIDADES_COMPLETAS}
UNIDADES_LIMPAS = sorted(list(UNIDADES_MAP.keys()))

# Limites de desconto adaptados por unidade (usando o nome limpo como chave)
DESCONTOS_MAXIMOS_POR_UNIDADE = {
    "RETIRO DOS ARTISTAS": 0.50,
    "CAMPO GRANDE": 0.6320,
    "ROCHA MIRANDA": 0.6606,
    "TAQUARA": 0.6755,
    "NOVA IGUACU": 0.6700,
    "DUQUE DE CAXIAS": 0.6823,
    "BANGU": 0.6806,
    "MADUREIRA": 0.7032,
    "TIJUCA": 0.6800,
    "SÃO JOÃO DE MERITI": 0.7197,
}

# --------------------------------------------------
# FUNÇÕES DE LÓGICA E UTILITÁRIOS
# --------------------------------------------------
def calcula_bolsa(acertos: int) -> float:
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")

def gera_pdf_html(ctx: dict) -> bytes:
    base_dir = Path(__file__).parent
    html_path = base_dir / "carta.html"
    with open(html_path, encoding="utf-8") as f:
        html_template = f.read()
    html_renderizado = html_template
    for k, v in ctx.items():
        html_renderizado = html_renderizado.replace(f"{{{{{k}}}}}", str(v))
    html_obj = weasyprint.HTML(string=html_renderizado, base_url=str(base_dir))
    return html_obj.write_pdf()

@st.cache_resource
def get_gspread_client():
    """Conecta ao Google Sheets usando os segredos do Streamlit e faz cache da conexão."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Erro de autenticação com o Google Sheets: {e}")
        return None

@st.cache_data(ttl=600)
def get_all_hubspot_data(_client):
    """Obtém todos os dados da aba 'Hubspot'."""
    try:
        sheet = _client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
        aba_hubspot = sheet.worksheet("Hubspot")
        df = pd.DataFrame(aba_hubspot.get_all_records())
        return df
    except Exception as e:
        st.error(f"❌ Falha ao carregar dados do Hubspot: {e}")
        return pd.DataFrame()

def calcula_valor_minimo(unidade, serie_modalidade):
    """
    Calcula o valor mínimo negociável usando APENAS o dicionário local.
    """
    try:
        # Pega o desconto máximo da unidade, se não encontrar, retorna 0
        desconto_maximo = DESCONTOS_MAXIMOS_POR_UNIDADE.get(unidade, 0)
        
        # Pega o valor da anuidade para a série/modalidade
        valor_anuidade_integral = TUITION.get(serie_modalidade, {}).get("anuidade", 0)

        # O valor mínimo negociável é a anuidade descontada do desconto máximo,
        # dividido pelo número de parcelas (12)
        if valor_anuidade_integral > 0 and desconto_maximo > 0:
            valor_minimo_anual = valor_anuidade_integral * (1 - desconto_maximo)
            return valor_minimo_anual / 12
        else:
            return 0
    except Exception as e:
        st.error(f"❌ Erro ao calcular valor mínimo: {e}")
        return 0


def find_column_index(headers, target_name):
    """Encontra o índice de uma coluna ignorando espaços e case."""
    for i, header in enumerate(headers):
        if header.strip().lower() == target_name.strip().lower():
            return i + 1
    return None

# --------------------------------------------------
# INTERFACE STREAMLIT
# --------------------------------------------------
st.set_page_config(page_title="Gestor do Bolsão", layout="centered")
st.title("🎓 Gestor do Bolsão")

client = get_gspread_client()

aba_carta, aba_negociacao, aba_ativacao = st.tabs(["Gerar Carta", "Negociação", "Ativação do Bolsão"])

# --- ABA GERAR CARTA ---
with aba_carta:
    st.subheader("Gerar Carta")
    
    modo_preenchimento = st.radio(
        "Selecione o modo de preenchimento:",
        ["Preencher manualmente", "Carregar dados de um candidato"],
        horizontal=True, key="modo_preenchimento"
    )

    # Valores padrão ou pré-preenchidos
    nome_aluno_pre = ""
    turma_aluno_pre = "1ª série do Ensino Médio Regular"
    unidade_aluno_pre = "BANGU"
    
    if modo_preenchimento == "Carregar dados de um candidato":
        if client:
            df_hubspot_all = get_all_hubspot_data(client)
            if not df_hubspot_all.empty:
                unidade_selecionada = st.selectbox(
                    "Selecione a Unidade do candidato:", UNIDADES_LIMPAS, key="unidade_selecionada_carta"
                )
                
                df_filtrado = df_hubspot_all[df_hubspot_all['Unidade'] == UNIDADES_MAP[unidade_selecionada]]
                nomes_candidatos = ["Selecione um candidato"] + sorted(df_filtrado['Nome do candidato'].tolist())
                
                selecao_candidato = st.selectbox(
                    "Selecione o candidato da lista:", nomes_candidatos, key="selecao_candidato"
                )
                
                if selecao_candidato != "Selecione um candidato":
                    candidato_selecionado = df_filtrado[df_filtrado['Nome do candidato'] == selecao_candidato].iloc[0]
                    nome_aluno_pre = candidato_selecionado.get('Nome do candidato', '')
                    turma_aluno_pre = candidato_selecionado.get('Turma de Interesse - Geral', '1ª série do Ensino Médio Regular')
                    unidade_aluno_pre = unidade_selecionada
                    st.info(f"Dados de {nome_aluno_pre} carregados.")
            else:
                st.warning("Nenhum candidato encontrado para carregar.")
    
    st.write("---")
    
    c1, c2 = st.columns(2)
    with c1:
        unidade_limpa_index = UNIDADES_LIMPAS.index(unidade_aluno_pre) if unidade_aluno_pre in UNIDADES_LIMPAS else 0
        unidade_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, index=unidade_limpa_index, key="c_unid")
        turma = st.text_input("Turma de interesse", turma_aluno_pre, key="c_turma")
    with c2:
        ac_mat = st.number_input("Acertos - Matemática", 0, 12, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, 12, 0, key="c_port")
    
    aluno = st.text_input("Nome completo do candidato", nome_aluno_pre, key="c_nome")

    total = ac_mat + ac_port
    pct = calcula_bolsa(total)
    st.markdown(f"### ➔ Bolsa obtida: *{pct*100:.0f}%* ({total} acertos)")

    serie = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="c_serie")
    val_ano = TUITION[serie]["anuidade"] * (1 - pct)
    val_parc = TUITION[serie]["parcela13"] * (1 - pct)

    if st.button("Gerar Carta PDF", key="c_gerar"):
        if not aluno:
            st.error("Por favor, preencha o nome do candidato.")
        elif client is None:
            st.error("Não foi possível gerar a carta pois a conexão com a planilha falhou.")
        else:
            hoje = date.today()
            nome_bolsao = "-"
            try:
                sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=380208567")
                aba_bolsao = sheet.worksheet("Bolsão")
                dados_bolsao = aba_bolsao.get_all_records()
                for linha in dados_bolsao:
                    data_str, bolsao_nome_temp = linha.get("Data"), linha.get("Bolsão")
                    if data_str and bolsao_nome_temp:
                        if datetime.strptime(data_str, "%d/%m/%Y").date() >= hoje:
                            nome_bolsao = bolsao_nome_temp
                            break
            except Exception as e:
                st.warning(f"Não foi possível obter nome do bolsão: {e}")
            
            unidades_html = "".join(f"<span class='unidade-item'>{u}</span>" for u in UNIDADES_LIMPAS)
            ctx = {
                "ano": hoje.year, "unidade": f"Colégio Matriz – {unidade_limpa}",
                "aluno": aluno.strip().title(), "bolsa_pct": f"{pct * 100:.0f}",
                "acertos_mat": ac_mat, "acertos_port": ac_port, "turma": turma,
                "n_parcelas": 12, "data_limite": (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
                "anuidade_vista": format_currency(val_ano * 0.95),
                "primeira_cota": format_currency(val_parc), "valor_parcela": format_currency(val_parc),
                "unidades_html": unidades_html,
            }
            
            pdf_bytes = gera_pdf_html(ctx)
            st.success("✅ Carta em PDF gerada com sucesso!")

            try:
                sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
                aba_resultados = sheet.worksheet("Resultados_Bolsao")
                unidade_completa = UNIDADES_MAP[unidade_limpa]
                nova_linha = [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"), aluno.strip().title(), unidade_completa, turma,
                    ac_mat, ac_port, total, f"{pct*100:.0f}%", serie,
                    ctx["anuidade_vista"], ctx["primeira_cota"], ctx["valor_parcela"],
                    st.session_state.get("email", "-"), nome_bolsao
                ]
                aba_resultados.append_row(nova_linha, value_input_option="USER_ENTERED")
                st.info("📊 Resposta registrada na planilha.")
            except Exception as e:
                st.error(f"❌ Falha ao salvar na planilha: {e}")

            st.download_button(
                "📄 Baixar Carta", data=pdf_bytes,
                file_name=f"Carta_Bolsa_{aluno.replace(' ', '_')}.pdf", mime="application/pdf"
            )

# --- ABA NEGOCIAÇÃO ---
with aba_negociacao:
    st.subheader("Simulador de Negociação")
    if client:
        cn1, cn2 = st.columns(2)
        with cn1:
            unidade_neg_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, key="n_unid")
            serie_n = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="n_serie")
        with cn2:
            # A alteração foi feita aqui: [13, 12] e index=0 para ser a opção padrão
            parcelas_n = st.radio("Parcelas", [13, 12], horizontal=True, index=0, key="n_parc")

        # Chama a função que agora usa apenas o dicionário interno
        valor_minimo = calcula_valor_minimo(unidade_neg_limpa, serie_n)
        
        st.markdown(f"### ➡️ Valor Mínimo Negociável: *{format_currency(valor_minimo)}*")
        st.write("---")

        modo_simulacao = st.radio(
            "Calcular por:", ["Bolsa (%)", "Valor da Parcela (R$)"],
            horizontal=True, key="modo_sim"
        )
        
        valor_integral_parc = TUITION[serie_n]["parcela13"] if parcelas_n == 13 else TUITION[serie_n]["anuidade"] / 12

        if modo_simulacao == "Bolsa (%)":
            bolsa_simulada = st.slider("Porcentagem de Bolsa", 0, 100, 30, 1, key="bolsa_sim")
            valor_resultante = valor_integral_parc * (1 - bolsa_simulada / 100)
            st.metric("Valor da Parcela Resultante", format_currency(valor_resultante))
            if valor_resultante < valor_minimo:
                st.error("❌ Atenção: O valor resultante está abaixo do mínimo negociável!")
        else: # "Valor da Parcela (R$)"
            valor_neg = st.number_input("Valor desejado da parcela (R$)", 0.0, value=1500.0, step=10.0, key="valor_neg")
            pct_req = max(0.0, 1 - valor_neg / valor_integral_parc) if valor_integral_parc > 0 else 0.0
            bolsa_lanc = int(round(pct_req * 100))
            st.metric("Bolsa Necessária", f"{pct_req*100:.2f}%")
            st.write(f"Sugestão de bolsa a lançar: *{bolsa_lanc}%*")
            if valor_neg < valor_minimo:
                st.error("❌ Atenção: O valor negociado está abaixo do mínimo negociável!")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a negociação.")

# --- ABA ATIVAÇÃO DO BOLSÃO ---
with aba_ativacao:
    st.subheader("Ativação de Bolsão")
    if client:
        unidade_ativacao_limpa = st.selectbox("Selecione a Unidade para Ativação", UNIDADES_LIMPAS, key="a_unid")
        
        if st.button("Carregar Lista de Candidatos", key="a_carregar"):
            unidade_ativacao_completa = UNIDADES_MAP[unidade_ativacao_limpa]
            df_hubspot = get_all_hubspot_data(client)
            df_filtrado = df_hubspot[df_hubspot['Unidade'] == unidade_ativacao_completa]
            st.session_state['df_ativacao'] = df_filtrado
            st.session_state['unidade_ativa'] = unidade_ativacao_limpa

        if 'df_ativacao' in st.session_state and not st.session_state['df_ativacao'].empty:
            st.write(f"Lista de candidatos para a unidade: *{st.session_state['unidade_ativa']}*")
            df_display = st.session_state['df_ativacao']
            
            try:
                sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
                aba_hubspot = sheet.worksheet("Hubspot")
                headers = aba_hubspot.row_values(1)
                
                cols = {
                    'nome': find_column_index(headers, 'Nome do candidato'),
                    'contato_realizado': find_column_index(headers, 'Contato realizado'),
                    'status': find_column_index(headers, 'Status do Contato'),
                    'id': find_column_index(headers, 'Contato ID'),
                    'observacoes': find_column_index(headers, 'Observações') # Adicionada a coluna de observações
                }

                if not all([cols['nome'], cols['id']]):
                    st.warning("⚠️ Colunas 'Nome do candidato' e 'Contato ID' são essenciais e não foram encontradas.")
                else:
                    for index, row in df_display.iterrows():
                        status_atual = str(row.get('Status do Contato', '-')).strip()
                        contato_realizado_bool = str(row.get('Contato realizado', 'Não')).strip().lower() == "sim"
                        observacoes_atuais = str(row.get('Observações', '')).strip() # Obtém o valor atual das observações
                        
                        emoji = "⚪"
                        if "confirmado" in status_atual.lower(): emoji = "✅"
                        elif "não atende" in status_atual.lower(): emoji = "📞"
                        elif "não comparecerá" in status_atual.lower(): emoji = "❌"
                        elif contato_realizado_bool: emoji = "✅"

                        expander_title = f"{emoji} *{row.get('Nome do candidato', 'N/A')}* | Status: *{status_atual}* | Cel: {row.get('Celular Tratado', 'N/A')}"
                        with st.expander(expander_title):
                            st.markdown(f"""
                            - **Responsável:** {row.get('Nome', 'N/A')}
                            - **E-mail:** {row.get('E-mail', 'N/A')}
                            - **Turma:** {row.get('Turma de Interesse - Geral', 'N/A')}
                            - **Fonte:** {row.get('Fonte original', 'N/A')}
                            """)
                            
                            novo_nome = st.text_input("Editar Nome", value=row.get('Nome do candidato', ''), key=f"nome_{index}")
                            
                            status_options = ["-", "Não atende", "Confirmado", "Não comparecerá","Bolsão Reagendado","Duplicado"]
                            status_index = status_options.index(status_atual) if status_atual in status_options else 0
                            
                            contato_realizado = st.checkbox("Contato Realizado", value=contato_realizado_bool, key=f"check_{index}")
                            status_contato = st.selectbox("Status do Contato", status_options, index=status_index, key=f"status_{index}")
                            novas_observacoes = st.text_area("Observações", value=observacoes_atuais, key=f"obs_{index}")
                            
                            if st.button("Salvar Status", key=f"save_{index}"):
                                try:
                                    cell = aba_hubspot.find(str(row.get('Contato ID', '')), in_column=cols['id'])
                                    if cell:
                                        aba_hubspot.update_cell(cell.row, cols['nome'], novo_nome)
                                        if cols['contato_realizado']:
                                            aba_hubspot.update_cell(cell.row, cols['contato_realizado'], "Sim" if contato_realizado else "Não")
                                        if cols['status']:
                                            aba_hubspot.update_cell(cell.row, cols['status'], status_contato)
                                        # ADIÇÃO: Salva o campo de observações
                                        if cols['observacoes']:
                                            aba_hubspot.update_cell(cell.row, cols['observacoes'], novas_observacoes)
                                        st.success(f"Status e observações de {novo_nome} atualizados!")
                                        st.rerun()
                                    else:
                                        st.error("Candidato não encontrado na planilha para atualização.")
                                except Exception as e:
                                    st.error(f"❌ Falha ao atualizar planilha: {e}")
            except Exception as e:
                st.error(f"❌ Erro ao processar a aba de ativação: {e}")
        else:
            st.info("Nenhum candidato encontrado para a unidade selecionada.")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a ativação.")

