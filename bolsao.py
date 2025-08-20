# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v7.4 - Versão com Filtro por Filial no Formulário)
-------------------------------------------------
Aplicação Streamlit que gera cartas, gerencia negociações e ativações de bolsão,
utilizando WeasyPrint para PDF e Pandas para manipulação de dados.

# Histórico de alterações
# v7.4 - 20/08/2025:
# - Adicionado filtro obrigatório por unidade na aba "Formulário básico" antes
#   de carregar a lista de candidatos, resolvendo o problema de não carregar
#   nenhum aluno e melhorando a usabilidade.
# v7.3 - 20/08/2025:
# - Corrigido erro "list index out of range" na aba "Formulário básico".
# v7.2 - 20/08/2025:
# - Corrigido erro de "Session State API" na aba "Gerar Carta".
# - Melhorada a mensagem de erro para colunas ausentes na aba "Ativação".
# - Implementada gravação de dados robusta baseada no cabeçalho da planilha.
# v7.1 - 20/08/2025:
# - Corrigidos os nomes das colunas na aba "Formulário básico".
# v7.0 - 20/08/2025:
# - Adicionada a aba "Formulário básico" e otimizações de performance.
"""
import io
import uuid
from datetime import date, timedelta, datetime
from functools import lru_cache
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
import weasyprint
from google.oauth2.service_account import Credentials

# --------------------------------------------------
# UTILITÁRIOS DE ACESSO AO GOOGLE SHEETS (OTIMIZADOS)
# --------------------------------------------------
SPREAD_URL = "https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=0"

@st.cache_resource
def get_gspread_client():
    """Conecta ao Google Sheets usando os segredos do Streamlit e faz cache da conexão."""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Erro de autenticação com o Google Sheets: {e}")
        return None

@st.cache_resource
def get_workbook(_client):
    """Abre a planilha e faz cache do objeto."""
    if not _client:
        return None
    return _client.open_by_url(SPREAD_URL)

@lru_cache(maxsize=32)
def get_ws(title: str):
    """Obtém uma aba (worksheet) pelo título e faz cache."""
    client = get_gspread_client()
    wb = get_workbook(client)
    if wb:
        try:
            return wb.worksheet(title)
        except gspread.WorksheetNotFound:
            st.error(f"Aba da planilha com o nome '{title}' não foi encontrada.")
            return None
    return None

@lru_cache(maxsize=32)
def header_map(ws_title: str):
    """Cria um mapa de 'nome_da_coluna': indice para uma dada aba."""
    ws = get_ws(ws_title)
    if ws:
        headers = ws.row_values(1)
        return {h.strip(): i + 1 for i, h in enumerate(headers) if h and h.strip()}
    return {}

def get_values(ws, a1_range: str):
    """Leitura enxuta por range; muito mais barato que get_all_records()."""
    return ws.get(a1_range, value_render_option="UNFORMATTED_VALUE")

def find_row_by_id(ws, id_col_idx: int, target_id: str):
    """Evita ws.find repetido. Carrega a coluna de IDs 1x e busca em memória."""
    try:
        col_values = ws.col_values(id_col_idx)[1:]  # ignora header
        for i, value in enumerate(col_values, start=2):
            if str(value) == str(target_id):
                return i
    except Exception:
        return None
    return None

def batch_update_cells(ws, updates):
    """
    Executa múltiplas atualizações de células em uma única requisição.
    updates: lista de dicts [{"range": "A2", "values": [[...]]}, ...]
    """
    body = {"valueInputOption": "USER_ENTERED", "data": updates}
    ws.spreadsheet.values_batch_update(body)

def new_uuid():
    """Gera um ID único e curto."""
    return uuid.uuid4().hex[:12]

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

UNIDADES_COMPLETAS = [
    "COLEGIO E CURSO MATRIZ EDUCACAO CAMPO GRANDE", "COLEGIO E CURSO MATRIZ EDUCAÇÃO TAQUARA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO BANGU", "COLEGIO E CURSO MATRIZ EDUCACAO NOVA IGUACU",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO DUQUE DE CAXIAS", "COLEGIO E CURSO MATRIZ EDUCAÇÃO SÃO JOÃO DE MERITI",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO ROCHA MIRANDA", "COLEGIO E CURSO MATRIZ EDUCAÇÃO MADUREIRA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO RETIRO DOS ARTISTAS", "COLEGIO E CURSO MATRIZ EDUCACAO TIJUCA",
]
UNIDADES_MAP = {name.replace("COLEGIO E CURSO MATRIZ EDUCACAO", "").replace("COLEGIO E CURSO MATRIZ EDUCAÇÃO", "").strip(): name for name in UNIDADES_COMPLETAS}
UNIDADES_LIMPAS = sorted(list(UNIDADES_MAP.keys()))

DESCONTOS_MAXIMOS_POR_UNIDADE = {
    "RETIRO DOS ARTISTAS": 0.50, "CAMPO GRANDE": 0.6320, "ROCHA MIRANDA": 0.6606,
    "TAQUARA": 0.6755, "NOVA IGUACU": 0.6700, "DUQUE DE CAXIAS": 0.6823,
    "BANGU": 0.6806, "MADUREIRA": 0.7032, "TIJUCA": 0.6800, "SÃO JOÃO DE MERITI": 0.7197,
}

# --------------------------------------------------
# FUNÇÕES DE LÓGICA E UTILITÁRIOS
# --------------------------------------------------
def precos_2026(serie_modalidade: str) -> dict:
    base = TUITION.get(serie_modalidade, {})
    if not base:
        return {"primeira_cota": 0.0, "parcela_mensal": 0.0, "anuidade": 0.0}
    
    parcela_2026_do_dict = float(base.get("parcela13", 0.0))
    if parcela_2026_do_dict <= 0:
        return {"primeira_cota": 0.0, "parcela_mensal": 0.0, "anuidade": 0.0}

    parcela_2025 = round(parcela_2026_do_dict / 1.10, 2)
    primeira_cota = parcela_2025
    parcela_mensal = round(parcela_2025 * 1.093, 2)
    anuidade = round(primeira_cota + 12 * parcela_mensal, 2)

    return {"primeira_cota": primeira_cota, "parcela_mensal": parcela_mensal, "anuidade": anuidade}

def calcula_bolsa(acertos: int) -> float:
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    try:
        v_float = float(v)
        return f"R$ {v_float:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
    except (ValueError, TypeError):
        return str(v)

def gera_pdf_html(ctx: dict) -> bytes:
    base_dir = Path(__file__).parent
    html_path = base_dir / "carta.html"
    try:
        with open(html_path, encoding="utf-8") as f:
            html_template = f.read()
        html_renderizado = html_template
        for k, v in ctx.items():
            html_renderizado = html_renderizado.replace(f"{{{{{k}}}}}", str(v))
        html_obj = weasyprint.HTML(string=html_renderizado, base_url=str(base_dir))
        return html_obj.write_pdf()
    except FileNotFoundError:
        st.error(f"Arquivo 'carta.html' não encontrado no diretório. Crie o template HTML.")
        return b""
    except Exception as e:
        st.error(f"Erro ao gerar PDF: {e}")
        return b""

@st.cache_data(ttl=600)
def get_hubspot_data_for_activation():
    """Obtém dados otimizados da aba 'Hubspot' para a ativação."""
    try:
        ws_hub = get_ws("Hubspot")
        if not ws_hub:
            return pd.DataFrame()

        hmap_h = header_map("Hubspot")
        cols_needed = ["Unidade", "Nome do candidato", "Contato ID", "Status do Contato", 
                       "Contato realizado", "Observações", "Celular Tratado", "Nome", 
                       "E-mail", "Turma de Interesse - Geral", "Fonte original"]
        
        missing_cols = [c for c in cols_needed if c not in hmap_h]
        if missing_cols:
            st.error(f"As seguintes colunas necessárias não foram encontradas na aba 'Hubspot': {', '.join(missing_cols)}")
            return pd.DataFrame()

        data = ws_hub.get_all_records(head=1)
        df = pd.DataFrame(data)
        
        return df[cols_needed]

    except Exception as e:
        st.error(f"❌ Falha ao carregar dados do Hubspot: {e}")
        return pd.DataFrame()

def calcula_valor_minimo(unidade, serie_modalidade):
    try:
        desconto_maximo = DESCONTOS_MAXIMOS_POR_UNIDADE.get(unidade, 0)
        precos = precos_2026(serie_modalidade)
        valor_anuidade_integral = precos.get("anuidade", 0.0)

        if valor_anuidade_integral > 0 and desconto_maximo > 0:
            valor_minimo_anual = valor_anuidade_integral * (1 - desconto_maximo)
            return valor_minimo_anual / 12
        else:
            return 0.0
    except Exception as e:
        st.error(f"❌ Erro ao calcular valor mínimo: {e}")
        return 0.0

# --------------------------------------------------
# INTERFACE STREAMLIT
# --------------------------------------------------
st.set_page_config(page_title="Gestor do Bolsão", layout="centered")
st.title("🎓 Gestor do Bolsão")

client = get_gspread_client()

aba_carta, aba_negociacao, aba_ativacao, aba_formulario = st.tabs([
    "Gerar Carta", "Negociação", "Ativação do Bolsão", "Formulário básico"
])

# --- ABA GERAR CARTA ---
with aba_carta:
    st.subheader("Gerar Carta")
    
    modo_preenchimento = st.radio(
        "Selecione o modo de preenchimento:",
        ["Preencher manualmente", "Carregar dados de um candidato"],
        horizontal=True, key="modo_preenchimento"
    )

    nome_aluno_pre = ""
    turma_aluno_pre = "1ª e 2ª Série EM Vestibular"
    unidade_aluno_pre = "BANGU"
    
    if modo_preenchimento == "Carregar dados de um candidato":
        if client:
            df_hubspot_all = get_hubspot_data_for_activation()
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
                    
                    turma_aluno_pre = candidato_selecionado.get('Turma de Interesse - Geral', '1ª e 2ª Série EM Vestibular')
                    # Seta o session state para os dois campos sincronizados
                    st.session_state["c_turma"] = turma_aluno_pre
                    st.session_state["c_serie"] = turma_aluno_pre
                    unidade_aluno_pre = unidade_selecionada
                    st.info(f"Dados de {nome_aluno_pre} carregados.")
            else:
                st.warning("Nenhum candidato encontrado. Verifique se há erros de coluna na aba 'Ativação'.")
    
    st.write("---")
    
    opcoes_series = list(TUITION.keys())
    def _normaliza_turma(valor):
        return valor if valor in opcoes_series else opcoes_series[0]
        
    def sync_from_turma():
        st.session_state.c_serie = st.session_state.c_turma
    
    def sync_from_serie():
        st.session_state.c_turma = st.session_state.c_serie
    
    # Inicializa o estado da sessão se ainda não existir
    if "c_turma" not in st.session_state:
        st.session_state.c_turma = _normaliza_turma(turma_aluno_pre)
    if "c_serie" not in st.session_state:
        st.session_state.c_serie = st.session_state.c_turma

    c1, c2 = st.columns(2)
    with c1:
        unidade_limpa_index = UNIDADES_LIMPAS.index(unidade_aluno_pre) if unidade_aluno_pre in UNIDADES_LIMPAS else 0
        unidade_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, index=unidade_limpa_index, key="c_unid")
    
        # CORREÇÃO: Removido o parâmetro 'index' para evitar conflito com o session state
        turma = st.selectbox(
            "Turma de interesse", opcoes_series,
            key="c_turma", on_change=sync_from_turma
        )
    with c2:
        ac_mat = st.number_input("Acertos - Matemática", 0, 12, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, 12, 0, key="c_port")
    
    aluno = st.text_input("Nome completo do candidato", nome_aluno_pre, key="c_nome")

    total = ac_mat + ac_port
    pct = calcula_bolsa(total)
    st.markdown(f"### ➔ Bolsa obtida: *{pct*100:.0f}%* ({total} acertos)")

    # CORREÇÃO: Removido o parâmetro 'index' para evitar conflito com o session state
    serie = st.selectbox(
        "Série / Modalidade", opcoes_series,
        key="c_serie", on_change=sync_from_serie
    )

    precos = precos_2026(st.session_state.c_serie)
    val_ano = precos["anuidade"] * (1 - pct)
    val_parcela_mensal = precos["parcela_mensal"] * (1 - pct)
    val_primeira_cota = precos["primeira_cota"] * (1 - pct)

    if st.button("Gerar Carta PDF", key="c_gerar"):
        if not aluno:
            st.error("Por favor, preencha o nome do candidato.")
        elif client is None:
            st.error("Não foi possível gerar a carta pois a conexão com a planilha falhou.")
        else:
            ws_res = get_ws("Resultados_Bolsao")
            hmap_res = header_map("Resultados_Bolsao")

            if not ws_res or not hmap_res:
                st.error("Não foi possível acessar a planilha 'Resultados_Bolsao'. Verifique o nome e as permissões.")
            elif "REGISTRO_ID" not in hmap_res:
                st.error("A planilha 'Resultados_Bolsao' precisa de uma coluna chamada 'REGISTRO_ID'. Por favor, adicione-a e tente novamente.")
            else:
                hoje = date.today()
                nome_bolsao = "-"
                try:
                    ws_bolsao = get_ws("Bolsão")
                    if ws_bolsao:
                        dados_bolsao = ws_bolsao.get_all_records()
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
                    "acertos_mat": ac_mat, "acertos_port": ac_port, "turma": st.session_state.c_turma,
                    "n_parcelas": 12, "data_limite": (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
                    "anuidade_vista": format_currency(val_ano * 0.95),
                    "primeira_cota": format_currency(val_primeira_cota),
                    "valor_parcela": format_currency(val_parcela_mensal),
                    "unidades_html": unidades_html,
                }
                
                pdf_bytes = gera_pdf_html(ctx)
                if pdf_bytes:
                    st.success("✅ Carta em PDF gerada com sucesso!")

                    try:
                        REGISTRO_ID = new_uuid()
                        row_data_map = {
                            "Data/Hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                            "Nome do Aluno": aluno.strip().title(),
                            "Unidade": UNIDADES_MAP[unidade_limpa],
                            "Turma de Interesse": st.session_state.c_turma,
                            "Acertos Matemática": ac_mat,
                            "Acertos Português": ac_port,
                            "Total de Acertos": total,
                            "% Bolsa": f"{pct*100:.0f}%",
                            "Série / Modalidade": st.session_state.c_serie,
                            "Valor Anuidade à Vista": ctx["anuidade_vista"],
                            "Valor da 1ª Cota": ctx["primeira_cota"],
                            "Valor da Mensalidade com Bolsa": ctx["valor_parcela"],
                            "Usuário": st.session_state.get("user", "-"),
                            "Bolsão": nome_bolsao,
                            "REGISTRO_ID": REGISTRO_ID
                        }
                        
                        header_list = sorted(hmap_res, key=hmap_res.get)
                        nova_linha = [row_data_map.get(col_name, "") for col_name in header_list]
                        
                        ws_res.append_row(nova_linha, value_input_option="USER_ENTERED")
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
            parcelas_n = st.radio("Parcelas", [13, 12], horizontal=True, index=0, key="n_parc")

        valor_minimo = calcula_valor_minimo(unidade_neg_limpa, serie_n)
        
        st.markdown(f"### ➡️ Valor Mínimo Negociável: *{format_currency(valor_minimo)}*")
        st.write("---")

        modo_simulacao = st.radio(
            "Calcular por:", ["Bolsa (%)", "Valor da Parcela (R$)"],
            horizontal=True, key="modo_sim"
        )
        
        precos_n = precos_2026(serie_n)
        valor_integral_parc = precos_n["parcela_mensal"]

        if modo_simulacao == "Bolsa (%)":
            bolsa_simulada = st.slider("Porcentagem de Bolsa", 0, 100, 30, 1, key="bolsa_sim")
            valor_resultante = valor_integral_parc * (1 - bolsa_simulada / 100)
            st.metric("Valor da Parcela Resultante", format_currency(valor_resultante))
            if valor_resultante < valor_minimo:
                st.error("❌ Atenção: O valor resultante está abaixo do mínimo negociável!")
        else:
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
            df_hubspot = get_hubspot_data_for_activation()
            if not df_hubspot.empty:
                df_filtrado = df_hubspot[df_hubspot['Unidade'] == unidade_ativacao_completa].copy()
                st.session_state['df_ativacao'] = df_filtrado
                st.session_state['unidade_ativa'] = unidade_ativacao_limpa
            else:
                st.session_state['df_ativacao'] = pd.DataFrame()


        if 'df_ativacao' in st.session_state and not st.session_state['df_ativacao'].empty:
            st.write(f"Lista de candidatos para a unidade: *{st.session_state['unidade_ativa']}*")
            df_display = st.session_state['df_ativacao']
            
            try:
                ws_hub = get_ws("Hubspot")
                hmap = header_map("Hubspot")
                
                required_cols = ['Nome do candidato', 'Contato ID']
                if not all(c in hmap for c in required_cols):
                    st.warning(f"⚠️ Colunas essenciais ({', '.join(required_cols)}) não foram encontradas.")
                else:
                    id_col_idx = hmap['Contato ID']
                    for index, row in df_display.iterrows():
                        row_id = str(row.get('Contato ID', ''))
                        status_atual = str(row.get('Status do Contato', '-')).strip()
                        contato_realizado_bool = str(row.get('Contato realizado', 'Não')).strip().lower() == "sim"
                        observacoes_atuais = str(row.get('Observações', '')).strip()
                        
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
                            
                            novo_nome = st.text_input("Editar Nome", value=row.get('Nome do candidato', ''), key=f"nome_{row_id}")
                            
                            status_options = ["-", "Não atende", "Confirmado", "Não comparecerá", "Bolsão Reagendado", "Duplicado"]
                            status_index = status_options.index(status_atual) if status_atual in status_options else 0
                            
                            contato_realizado = st.checkbox("Contato Realizado", value=contato_realizado_bool, key=f"check_{row_id}")
                            status_contato = st.selectbox("Status do Contato", status_options, index=status_index, key=f"status_{row_id}")
                            novas_observacoes = st.text_area("Observações", value=observacoes_atuais, key=f"obs_{row_id}")
                            
                            if st.button("Salvar Status", key=f"save_{row_id}"):
                                try:
                                    rownum = find_row_by_id(ws_hub, id_col_idx, row_id)
                                    if rownum:
                                        updates = []
                                        if 'Nome do candidato' in hmap:
                                            updates.append({"range": gspread.utils.rowcol_to_a1(rownum, hmap['Nome do candidato']), "values": [[novo_nome]]})
                                        if 'Contato realizado' in hmap:
                                            updates.append({"range": gspread.utils.rowcol_to_a1(rownum, hmap['Contato realizado']), "values": [["Sim" if contato_realizado else "Não"]]})
                                        if 'Status do Contato' in hmap:
                                            updates.append({"range": gspread.utils.rowcol_to_a1(rownum, hmap['Status do Contato']), "values": [[status_contato]]})
                                        if 'Observações' in hmap:
                                            updates.append({"range": gspread.utils.rowcol_to_a1(rownum, hmap['Observações']), "values": [[novas_observacoes]]})
                                        
                                        if updates:
                                            batch_update_cells(ws_hub, updates)
                                            st.success(f"Status e observações de {novo_nome} atualizados!")
                                            st.rerun()
                                    else:
                                        st.error(f"Candidato com ID {row_id} não encontrado na planilha para atualização.")
                                except Exception as e:
                                    st.error(f"❌ Falha ao atualizar planilha: {e}")
            except Exception as e:
                st.error(f"❌ Erro ao processar a aba de ativação: {e}")
        else:
            st.info("Clique em 'Carregar Lista de Candidatos' para começar.")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a ativação.")

# --- ABA FORMULÁRIO BÁSICO ---
with aba_formulario:
    st.subheader("Formulário Básico de Matrícula")

    if not client:
        st.warning("Conexão com o Google Sheets não disponível.")
    else:
        try:
            ws_res = get_ws("Resultados_Bolsao")
            if ws_res:
                hmap = header_map("Resultados_Bolsao")

                cols_list = ["REGISTRO_ID", "Nome do Aluno", "Unidade", "% Bolsa", "Valor da Mensalidade com Bolsa",
                             "Responsável Financeiro", "CPF Responsável", "Escola de Origem",
                             "Valor Negociado", "Aluno Matriculou?", "Optou por PIA?",
                             "Valor Limite (PIA)", "Observações (Form)", "Data/Hora"]
                
                missing = [c for c in cols_list if c not in hmap]
                if missing:
                    st.error(f"Faltam colunas em 'Resultados_Bolsao': {', '.join(missing)}")
                else:
                    # --- NOVO: Filtro por Unidade ---
                    unidade_selecionada_filtro = st.selectbox(
                        "Primeiro, filtre por uma unidade",
                        ["Selecione..."] + UNIDADES_LIMPAS,
                        key="filtro_unidade_form"
                    )

                    options = {"Selecione um candidato...": None}

                    # Só carrega os candidatos DEPOIS de selecionar uma unidade
                    if unidade_selecionada_filtro != "Selecione...":
                        
                        cols_for_dropdown = ["REGISTRO_ID", "Nome do Aluno", "Unidade"]
                        abs_indices = [hmap[c] for c in cols_for_dropdown]
                        min_col_idx, max_col_idx = min(abs_indices), max(abs_indices)

                        min_col_letter = gspread.utils.rowcol_to_a1(1, min_col_idx)[0]
                        max_col_letter = gspread.utils.rowcol_to_a1(1, max_col_idx)[0]
                        range_str = f"{min_col_letter}2:{max_col_letter}5000"
                        
                        all_data = ws_res.get(range_str)
                        
                        id_idx_rel = hmap["REGISTRO_ID"] - min_col_idx
                        aluno_idx_rel = hmap["Nome do Aluno"] - min_col_idx
                        unidade_idx_rel = hmap["Unidade"] - min_col_idx

                        unidade_completa_filtro = UNIDADES_MAP[unidade_selecionada_filtro]

                        for row in all_data:
                            max_req_idx = max(id_idx_rel, aluno_idx_rel, unidade_idx_rel)
                            if len(row) <= max_req_idx or not row[id_idx_rel]:
                                continue
                            
                            # Aplica o filtro de unidade
                            if row[unidade_idx_rel] == unidade_completa_filtro:
                                reg_id = row[id_idx_rel]
                                aluno = row[aluno_idx_rel]
                                
                                # Para o label, usamos o nome e o ID para garantir unicidade
                                label = f"{aluno} ({reg_id})"
                                options[label] = reg_id

                    selecao = st.selectbox("Selecione o Registro do Bolsão", options.keys())

                    if options.get(selecao):
                        reg_id_selecionado = options[selecao]
                        rownum = find_row_by_id(ws_res, hmap["REGISTRO_ID"], reg_id_selecionado)
                        
                        if rownum:
                            row_data = ws_res.row_values(rownum)
                            
                            def get_col_val(name):
                                idx = hmap.get(name)
                                return row_data[idx - 1] if idx and len(row_data) >= idx else ""

                            st.info(f"**Aluno:** {get_col_val('Nome do Aluno')} | **Bolsa:** {get_col_val('% Bolsa')} | **Parcela:** {get_col_val('Valor da Mensalidade com Bolsa')}")
                            st.write("---")

                            resp_fin = st.text_input("Responsável Financeiro", get_col_val("Responsável Financeiro"))
                            cpf_resp = st.text_input("CPF Responsável", get_col_val("CPF Responsável"))
                            escola_origem = st.text_input("Escola de Origem", get_col_val("Escola de Origem"))
                            valor_negociado = st.text_input("Valor Negociado", get_col_val("Valor Negociado"))
                            
                            matriculou_options = ["", "Sim", "Não"]
                            matriculou_idx = matriculou_options.index(get_col_val("Aluno Matriculou?")) if get_col_val("Aluno Matriculou?") in matriculou_options else 0
                            aluno_matriculou = st.selectbox("Aluno Matriculou?", matriculou_options, index=matriculou_idx)

                            optou_pia = st.checkbox("Optou por PIA?", value=(get_col_val("Optou por PIA?") == "Sim"))
                            valor_limite_pia = st.text_input("Valor Limite (PIA)", get_col_val("Valor Limite (PIA)"), disabled=not optou_pia)
                            
                            obs_form = st.text_area("Observações (Form)", get_col_val("Observações (Form)"))

                            if st.button("Salvar Formulário"):
                                updates_dict = {
                                    "Responsável Financeiro": resp_fin,
                                    "CPF Responsável": cpf_resp,
                                    "Escola de Origem": escola_origem,
                                    "Valor Negociado": valor_negociado,
                                    "Aluno Matriculou?": aluno_matriculou,
                                    "Optou por PIA?": "Sim" if optou_pia else "Não",
                                    "Valor Limite (PIA)": valor_limite_pia if optou_pia else "",
                                    "Observações (Form)": obs_form,
                                }
                                
                                updates_to_batch = []
                                for col_name, value in updates_dict.items():
                                    col_idx = hmap.get(col_name)
                                    if col_idx:
                                        a1_notation = gspread.utils.rowcol_to_a1(rownum, col_idx)
                                        updates_to_batch.append({"range": a1_notation, "values": [[value]]})
                                
                                if updates_to_batch:
                                    batch_update_cells(ws_res, updates_to_batch)
                                    st.success("Dados do formulário salvos com sucesso!")
                                    st.rerun()

        except Exception as e:
            st.error(f"Ocorreu um erro ao carregar o formulário: {e}")
