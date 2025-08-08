# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v5.1 - Versão Unificada e Otimizada)
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

DESCONTO_MINIMO_PADRAO = 0.60

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
                valor = float(str(row['VALOR LIMITE']).replace('R$', '').replace('.', '').replace(',', '.').strip())
                limites_dict[chave] = valor
        return limites_dict
    except Exception as e:
        st.error(f"❌ Falha ao carregar dados de limites: {e}")
        return {}

def find_column_index(headers, target_name):
    """Encontra o índice de uma coluna ignorando espaços e case."""
    for i, header in enumerate(headers):
        if header.strip().lower() == target_name.strip().lower():
            return i + 1
    return None
    
def calcula_valor_minimo(unidade, serie_modalidade, limites_dict):
    """Calcula o valor mínimo negociável com base na planilha 'Limites' ou em um desconto padrão."""
    chave = (unidade, serie_modalidade)
    if chave in limites_dict:
        return limites_dict[chave]
    else:
        # Se não houver valor na planilha, usa o desconto padrão
        valor_integral = TUITION.get(serie_modalidade, {}).get("parcela13", 0)
        return valor_integral * (1 - DESCONTO_MINIMO_PADRAO)

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
                "anuidade_vista": format_currency(val_ano * 0.93),
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
            parcelas_n = st.radio("Parcelas", [12, 13], horizontal=True, key="n_parc")

        unidade_neg_completa = UNIDADES_MAP[unidade_neg_limpa]
        limites = get_limites_data(client)
        valor_minimo = calcula_valor_minimo(unidade_neg_completa, serie_n, limites)
        
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
        
        # Otimização: Carrega os dados uma única vez e armazena em cache de sessão
        if st.button("Carregar Lista de Candidatos", key="a_carregar") or 'df_ativacao' not in st.session_state:
            unidade_ativacao_completa = UNIDADES_MAP[unidade_ativacao_limpa]
            df_hubspot = get_all_hubspot_data(client)
            df_filtrado = df_hubspot[df_hubspot['Unidade'] == unidade_ativacao_completa].copy()
            # Adiciona o índice da linha original para usar na atualização
            df_filtrado['__row_index__'] = df_filtrado.index + 2  # +2 pois o get_all_records começa da linha 2
            st.session_state['df_ativacao'] = df_filtrado.to_dict('records')
            st.session_state['unidade_ativa'] = unidade_ativacao_limpa

        if 'df_ativacao' in st.session_state and st.session_state['df_ativacao']:
            st.write(f"Lista de candidatos para a unidade: *{st.session_state['unidade_ativa']}*")
            
            # Use um formulário para agrupar todas as atualizações
            with st.form("form_atualizacao_bolsao"):
                updates_pendentes = []
                
                try:
                    for index, row_dict in enumerate(st.session_state['df_ativacao']):
                        status_atual = str(row_dict.get('Status do Contato', '-')).strip()
                        contato_realizado_bool = str(row_dict.get('Contato realizado', 'Não')).strip().lower() == "sim"
                        
                        emoji = "⚪"
                        if "confirmado" in status_atual.lower(): emoji = "✅"
                        elif "não atende" in status_atual.lower(): emoji = "📞"
                        elif "não comparecerá" in status_atual.lower(): emoji = "❌"
                        elif contato_realizado_bool: emoji = "✅"

                        expander_title = f"{emoji} *{row_dict.get('Nome do candidato', 'N/A')}* | Status: *{status_atual}* | Cel: {row_dict.get('Celular Tratado', 'N/A')}"
                        
                        with st.expander(expander_title):
                            st.markdown(f"""
                            - **Responsável:** {row_dict.get('Nome', 'N/A')}
                            - **E-mail:** {row_dict.get('E-mail', 'N/A')}
                            - **Turma:** {row_dict.get('Turma de Interesse - Geral', 'N/A')}
                            - **Fonte:** {row_dict.get('Fonte original', 'N/A')}
                            """)
                            
                            novo_nome = st.text_input("Editar Nome", value=row_dict.get('Nome do candidato', ''), key=f"nome_{index}")
                            status_options = ["-", "Não atende", "Confirmado", "Não comparecerá","Bolsão Reagendado","Duplicado"]
                            status_index = status_options.index(status_atual) if status_atual in status_options else 0
                            
                            contato_realizado = st.checkbox("Contato Realizado", value=contato_realizado_bool, key=f"check_{index}")
                            status_contato = st.selectbox("Status do Contato", status_options, index=status_index, key=f"status_{index}")
                            
                            # Adicione uma flag para detectar se algo foi alterado
                            if novo_nome != row_dict.get('Nome do candidato', '') or contato_realizado != contato_realizado_bool or status_contato != status_atual:
                                updates_pendentes.append({
                                    'index_na_planilha': row_dict['__row_index__'],
                                    'novo_nome': novo_nome,
                                    'contato_realizado': "Sim" if contato_realizado else "Não",
                                    'status_contato': status_contato
                                })
                                st.info("Alteração pendente. Clique em 'Salvar Todas as Alterações' no final da lista.")

                    # Botão para salvar todas as alterações de uma vez
                    submitted = st.form_submit_button("Salvar Todas as Alterações")

                    if submitted and updates_pendentes:
                        try:
                            sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=422747648")
                            aba_hubspot = sheet.worksheet("Hubspot")
                            headers = aba_hubspot.row_values(1)
                            
                            cols = {
                                'nome': find_column_index(headers, 'Nome do candidato'),
                                'contato_realizado': find_column_index(headers, 'Contato realizado'),
                                'status': find_column_index(headers, 'Status do Contato')
                            }

                            if not all(cols.values()):
                                st.error("⚠️ Uma ou mais colunas essenciais ('Nome do candidato', 'Contato realizado', 'Status do Contato') não foram encontradas. Verifique a planilha.")
                            else:
                                cells_to_update = []
                                for update in updates_pendentes:
                                    row_num = update['index_na_planilha']
                                    cells_to_update.append(gspread.Cell(row_num, cols['nome'], update['novo_nome']))
                                    cells_to_update.append(gspread.Cell(row_num, cols['contato_realizado'], update['contato_realizado']))
                                    cells_to_update.append(gspread.Cell(row_num, cols['status'], update['status_contato']))

                                aba_hubspot.batch_update(cells_to_update)
                                st.success("✅ Todos os status foram atualizados com sucesso! A página será recarregada.")
                                st.rerun()

                        except Exception as e:
                            st.error(f"❌ Falha ao atualizar planilha: {e}")
                    elif submitted and not updates_pendentes:
                        st.info("Nenhuma alteração a ser salva.")

                except Exception as e:
                    st.error(f"❌ Erro ao processar a aba de ativação: {e}")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a ativação.")

st.caption("Desenvolvido para Matriz Educação • Suporte: TI Interno")

