# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v8.5 - Form + 13x-only + Hubspot rename)
-----------------------------------------------------------------
- Somente plano 13x (1ª + 12) ou à vista: removida qualquer lógica de 12x no simulador.
- Renomeado get_hubspot_data_for_activation() -> get_hubspot_data() e corrigidas mensagens.
- Formulário básico com st.form + validação/round server-side + rerender estável.
- Aba Valores com busca por Série e download CSV do que está na tela.
- Limpeza de imports.
"""
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
# ACESSO AO GOOGLE SHEETS (CACHEADO)
# --------------------------------------------------
SPREAD_URL = "https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=0"

@st.cache_resource
def get_gspread_client():
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Erro de autenticação com o Google Sheets: {e}")
        return None

@st.cache_resource
def get_workbook(_client):
    if not _client:
        return None
    return _client.open_by_url(SPREAD_URL)

@lru_cache(maxsize=32)
def get_ws(title: str):
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
    ws = get_ws(ws_title)
    if ws:
        headers = ws.row_values(1)
        return {h.strip(): i + 1 for i, h in enumerate(headers) if h and h.strip()}
    return {}

def get_values(ws, a1_range: str):
    return ws.get(a1_range, value_render_option="UNFORMATTED_VALUE")

def find_row_by_id(ws, id_col_idx: int, target_id: str):
    try:
        col_values = ws.col_values(id_col_idx)[1:]  # ignora header
        for i, value in enumerate(col_values, start=2):
            if str(value) == str(target_id):
                return i
    except Exception:
        return None
    return None

def batch_update_cells(ws, updates):
    """Força prefixo do título da aba nos ranges A1."""
    if not updates:
        return
    fixed = []
    sheet_title_safe = ws.title.replace("'", "''")
    for u in updates:
        rng = u.get("range", "")
        if not rng:
            continue
        if "!" not in rng:
            rng = f"'{sheet_title_safe}'!{rng}"
        fixed.append({"range": rng, "values": u.get("values", [[]])})
    body = {"valueInputOption": "USER_ENTERED", "data": fixed}
    ws.spreadsheet.values_batch_update(body)

def ensure_size(ws, min_rows=2000, min_cols=40):
    try:
        if ws and (ws.row_count < min_rows or ws.col_count < min_cols):
            ws.resize(rows=max(ws.row_count, min_rows), cols=max(ws.col_count, min_cols))
    except Exception:
        pass

def new_uuid():
    return uuid.uuid4().hex[:12]

# --------------------------------------------------
# DADOS E REGRAS
# --------------------------------------------------
BOLSA_MAP = {
    0:.30,1:.30,2:.30,3:.35,4:.40,5:.40,6:.44,7:.45,8:.46,9:.47,
    10:.48,11:.49,12:.50,13:.51,14:.52,15:.53,16:.54,17:.55,18:.56,19:.57,
    20:.60,21:.65,22:.70,23:.80,24:1.00,
}

TUITION = {
    "1ª e 2ª Série EM Militar":{"anuidade":36339.60,"parcela13":2795.35},
    "1ª e 2ª Série EM Vestibular":{"anuidade":36339.60,"parcela13":2795.35},
    "1º ao 5º Ano":{"anuidade":26414.30,"parcela13":2031.87},
    "3ª Série (PV/PM)":{"anuidade":36480.40,"parcela13":2806.19},
    "3ª Série EM Medicina":{"anuidade":36480.40,"parcela13":2806.19},
    "6º ao 8º Ano":{"anuidade":31071.70,"parcela13":2390.14},
    "9º Ano EF II Militar":{"anuidade":33838.20,"parcela13":2602.94},
    "9º Ano EF II Vestibular":{"anuidade":33838.20,"parcela13":2602.94},
    "AFA/EN/EFOMM":{"anuidade":14668.50,"parcela13":1128.35},
    "CN/EPCAr":{"anuidade":8783.50,"parcela13":675.65},
    "ESA":{"anuidade":7080.70,"parcela13":544.67},
    "EsPCEx":{"anuidade":14668.50,"parcela13":1128.35},
    "IME/ITA":{"anuidade":14668.50,"parcela13":1128.35},
    "Medicina (Pré)":{"anuidade":14668.50,"parcela13":1128.35},
    "Pré-Vestibular":{"anuidade":14668.50,"parcela13":1128.35},
}

TURMA_DE_INTERESSE_MAP = {
    "1ª série IME ITA Jr":"1ª e 2ª Série EM Militar",
    "1ª série do EM - Militar":"1ª e 2ª Série EM Militar",
    "1ª série do EM - Pré-Vestibular":"1ª e 2ª Série EM Vestibular",
    "1º ano do EF1":"1º ao 5º Ano",
    "2ª série IME ITA Jr":"1ª e 2ª Série EM Militar",
    "2ª série do EM - Militar":"1ª e 2ª Série EM Militar",
    "2ª série do EM - Pré-Vestibular":"1ª e 2ª Série EM Vestibular",
    "2º ano do EF1":"1º ao 5º Ano",
    "3ª série do EM - AFA EN EFOMM":"3ª Série (PV/PM)",
    "3ª série do EM - ESA":"3ª Série (PV/PM)",
    "3ª série do EM - EsPCEx":"3ª Série (PV/PM)",
    "3ª série do EM - IME ITA":"3ª Série (PV/PM)",
    "3ª série do EM - Medicina":"3ª Série EM Medicina",
    "3ª série do EM - Pré-Vestibular":"3ª Série (PV/PM)",
    "3º ano do EF1":"1º ao 5º Ano",
    "4º ano do EF1":"1º ao 5º Ano",
    "5º ano do EF1":"1º ao 5º Ano",
    "6º ano do EF2":"6º ao 8º Ano",
    "7º ano do EF2":"6º ao 8º Ano",
    "8º ano do EF2":"6º ao 8º Ano",
    "9º ano do EF2 - Militar":"9º Ano EF II Militar",
    "9º ano do EF2 - Vestibular":"9º Ano EF II Vestibular",
    "Pré-Militar AFA EN EFOMM":"AFA/EN/EFOMM",
    "Pré-Militar CN EPCAr":"CN/EPCAr",
    "Pré-Militar ESA":"ESA",
    "Pré-Militar EsPCEx":"EsPCEx",
    "Pré-Militar IME ITA":"IME/ITA",
    "Pré-Vestibular":"Pré-Vestibular",
    "Pré-Vestibular - Medicina":"Medicina (Pré)",
}
SERIE_TO_TURMA_MAP = {v:k for k,v in reversed(list(TURMA_DE_INTERESSE_MAP.items()))}

UNIDADES_COMPLETAS = [
    "COLEGIO E CURSO MATRIZ EDUCACAO CAMPO GRANDE","COLEGIO E CURSO MATRIZ EDUCAÇÃO TAQUARA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO BANGU","COLEGIO E CURSO MATRIZ EDUCACAO NOVA IGUACU",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO DUQUE DE CAXIAS","COLEGIO E CURSO MATRIZ EDUCAÇÃO SÃO JOÃO DE MERITI",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO ROCHA MIRANDA","COLEGIO E CURSO MATRIZ EDUCAÇÃO MADUREIRA",
    "COLEGIO E CURSO MATRIZ EDUCAÇÃO RETIRO DOS ARTISTAS","COLEGIO E CURSO MATRIZ EDUCACAO TIJUCA",
]
UNIDADES_MAP = {name.replace("COLEGIO E CURSO MATRIZ EDUCACAO","").replace("COLEGIO E CURSO MATRIZ EDUCAÇÃO","").strip(): name for name in UNIDADES_COMPLETAS}
UNIDADES_LIMPAS = sorted(list(UNIDADES_MAP.keys()))

DESCONTOS_MAXIMOS_POR_UNIDADE = {
    "RETIRO DOS ARTISTAS":0.50,"CAMPO GRANDE":0.6320,"ROCHA MIRANDA":0.6606,
    "TAQUARA":0.6755,"NOVA IGUACU":0.6700,"DUQUE DE CAXIAS":0.6823,
    "BANGU":0.6806,"MADUREIRA":0.7032,"TIJUCA":0.6800,"SÃO JOÃO DE MERITI":0.7197,
}

# --------------------------------------------------
# REGRAS DE CÁLCULO (13x)
# --------------------------------------------------
def precos_2026(serie_modalidade: str) -> dict:
    base = TUITION.get(serie_modalidade, {})
    if not base:
        return {"primeira_cota":0.0,"parcela_mensal":0.0,"anuidade":0.0}
    parcela_2026_do_dict = float(base.get("parcela13", 0.0))
    if parcela_2026_do_dict <= 0:
        return {"primeira_cota":0.0,"parcela_mensal":0.0,"anuidade":0.0}
    parcela_2025 = round(parcela_2026_do_dict / 1.10, 2)
    primeira_cota = parcela_2025
    parcela_mensal = round(parcela_2025 * 1.093, 2)  # 12 iguais
    anuidade = round(primeira_cota + 12 * parcela_mensal, 2)
    return {"primeira_cota":primeira_cota,"parcela_mensal":parcela_mensal,"anuidade":anuidade}

def calcula_bolsa(acertos: int) -> float:
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    try:
        v_float = float(v)
        return f"R$ {v_float:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
    except (ValueError, TypeError):
        return str(v)

def parse_brl_to_float(x) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if not x:
        return 0.0
    s = str(x).strip().replace("R$","").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

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
        st.error("Arquivo 'carta.html' não encontrado no diretório. Crie o template HTML.")
        return b""
    except Exception as e:
        st.error(f"Erro ao gerar PDF: {e}")
        return b""

# ---- Hubspot (renomeado)
@st.cache_data(ttl=600)
def get_hubspot_data():
    """Lê a aba 'Hubspot' da planilha."""
    try:
        ws_hub = get_ws("Hubspot")
        if not ws_hub:
            return pd.DataFrame()
        hmap_h = header_map("Hubspot")
        cols_needed = ["Unidade","Nome do candidato","Contato ID","Status do Contato",
                       "Contato Realizado","Observações","Celular Tratado","Nome",
                       "E-mail","Turma de Interesse - Geral","Fonte original"]
        missing = [c for c in cols_needed if c not in hmap_h]
        if missing:
            st.error(f"Colunas ausentes na aba 'Hubspot': {', '.join(missing)}")
            return pd.DataFrame()
        data = ws_hub.get_all_records(head=1)
        df = pd.DataFrame(data)
        if "Contato Realizado" in df.columns:
            df.rename(columns={"Contato Realizado": "Contato realizado"}, inplace=True)
        return df
    except Exception as e:
        st.error(f"❌ Falha ao carregar dados do Hubspot: {e}")
        return pd.DataFrame()

# --------------------------------------------------
# UI
# --------------------------------------------------
st.set_page_config(page_title="Gestor do Bolsão", layout="centered")
st.title("🎓 Gestor do Bolsão")

client = get_gspread_client()

aba_carta, aba_negociacao, aba_formulario, aba_valores = st.tabs([
    "Gerar Carta", "Negociação", "Formulário básico", "Valores"
])

# --- GERAR CARTA ---
with aba_carta:
    st.subheader("Gerar Carta")

    modo_preenchimento = st.radio(
        "Selecione o modo de preenchimento:",
        ["Preencher manualmente", "Carregar dados de um candidato"],
        horizontal=True, key="modo_preenchimento"
    )

    nome_aluno_pre = ""
    serie_modalidade_pre = "1ª e 2ª Série EM Vestibular"
    unidade_aluno_pre = "BANGU"
    opcoes_turma_interesse = list(TURMA_DE_INTERESSE_MAP.keys())

    if modo_preenchimento == "Carregar dados de um candidato":
        if client:
            df_hubspot_all = get_hubspot_data()
            if not df_hubspot_all.empty:
                unidade_selecionada = st.selectbox(
                    "Selecione a Unidade do candidato:", UNIDADES_LIMPAS, key="unidade_selecionada_carta"
                )
                df_filtrado = df_hubspot_all[df_hubspot_all['Unidade'] == UNIDADES_MAP[unidade_selecionada]]
                nomes_candidatos = ["Selecione um candidato"] + sorted(df_filtrado['Nome do candidato'].tolist())
                selecao_candidato = st.selectbox("Selecione o candidato da lista:", nomes_candidatos, key="selecao_candidato")
                if selecao_candidato != "Selecione um candidato":
                    candidato = df_filtrado[df_filtrado['Nome do candidato'] == selecao_candidato].iloc[0]
                    nome_aluno_pre = candidato.get('Nome do candidato', '')
                    serie_modalidade_pre = candidato.get('Turma de Interesse - Geral', '1ª e 2ª Série EM Vestibular')
                    unidade_aluno_pre = unidade_selecionada
                    turma_interesse_carregada = SERIE_TO_TURMA_MAP.get(serie_modalidade_pre, opcoes_turma_interesse[0])
                    st.session_state.c_turma = turma_interesse_carregada
                    st.session_state.c_serie = serie_modalidade_pre
                    st.info(f"Dados de {nome_aluno_pre} carregados.")
            else:
                st.warning("Nenhum candidato encontrado. Verifique as colunas na aba 'Hubspot' da planilha.")

    st.write("---")

    def update_serie_from_turma():
        st.session_state.c_serie = TURMA_DE_INTERESSE_MAP.get(st.session_state.c_turma)

    if "c_turma" not in st.session_state:
        default_turma = SERIE_TO_TURMA_MAP.get(serie_modalidade_pre, opcoes_turma_interesse[0])
        st.session_state.c_turma = default_turma
        st.session_state.c_serie = serie_modalidade_pre

    c1, c2 = st.columns(2)
    with c1:
        unidade_limpa_index = UNIDADES_LIMPAS.index(unidade_aluno_pre) if unidade_aluno_pre in UNIDADES_LIMPAS else 0
        unidade_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, index=unidade_limpa_index, key="c_unid")

        st.selectbox("Turma de interesse", opcoes_turma_interesse, key="c_turma", on_change=update_serie_from_turma)
    with c2:
        ac_mat = st.number_input("Acertos - Matemática", 0, 12, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, 12, 0, key="c_port")

    aluno = st.text_input("Nome completo do candidato", nome_aluno_pre, key="c_nome")

    total = ac_mat + ac_port
    pct = calcula_bolsa(total)
    st.markdown(f"### ➔ Bolsa obtida: *{pct*100:.0f}%* ({total} acertos)")

    st.text_input("Série / Modalidade (para cálculo)", key="c_serie", disabled=True)

    precos = precos_2026(st.session_state.c_serie)
    val_ano = precos["anuidade"] * (1 - pct)                 # total 13x com bolsa
    val_parcela_mensal = precos["parcela_mensal"] * (1 - pct)  # 12 iguais
    val_primeira_cota = precos["primeira_cota"] * (1 - pct)    # 1ª cota
    val_vista = val_ano * 0.95  # mantém 5% (ajuste se a política mudar)

    if st.button("Gerar Carta PDF", key="c_gerar"):
        if not aluno:
            st.error("Por favor, preencha o nome do candidato.")
        elif client is None:
            st.error("Não foi possível gerar a carta pois a conexão com a planilha falhou.")
        else:
            ws_res = get_ws("Resultados_Bolsao")
            ensure_size(ws_res, 2000, 40)
            hmap_res = header_map("Resultados_Bolsao")
            if not ws_res or not hmap_res:
                st.error("Não foi possível acessar a planilha 'Resultados_Bolsao'. Verifique o nome e as permissões.")
            elif "REGISTRO_ID" not in hmap_res:
                st.error("A planilha 'Resultados_Bolsao' precisa de uma coluna chamada 'REGISTRO_ID'.")
            else:
                hoje = date.today()
                nome_bolsao = "-"
                try:
                    ws_bolsao = get_ws("Bolsão")
                    if ws_bolsao:
                        for linha in ws_bolsao.get_all_records():
                            data_str, bolsao_nome_temp = linha.get("Data"), linha.get("Bolsão")
                            if data_str and bolsao_nome_temp:
                                if datetime.strptime(data_str, "%d/%m/%Y").date() >= hoje:
                                    nome_bolsao = bolsao_nome_temp
                                    break
                except Exception as e:
                    st.warning(f"Não foi possível obter nome do bolsão: {e}")

                ctx = {
                    "ano": hoje.year,
                    "unidade": f"Colégio Matriz – {unidade_limpa}",
                    "aluno": aluno.strip().title(),
                    "bolsa_pct": f"{pct * 100:.0f}",
                    "acertos_mat": ac_mat,
                    "acertos_port": ac_port,
                    "turma": st.session_state.c_turma,
                    "n_parcelas": 12,  # 12 demais (plano 13x)
                    "data_limite": (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
                    "anuidade_vista": format_currency(val_vista),
                    "primeira_cota": format_currency(val_primeira_cota),
                    "valor_parcela": format_currency(val_parcela_mensal),
                    "unidades_html": "".join(f"<span class='unidade-item'>{u}</span>" for u in UNIDADES_LIMPAS),
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
                        file_name=f"Carta_Bolsa_{aluno.replace(' ', '_')}.pdf",
                        mime="application/pdf"
                    )

# --- NEGOCIAÇÃO (13x only) ---
with aba_negociacao:
    st.subheader("Simulador de Negociação (13x ou à vista)")
    st.caption("Plano padrão: 1ª cota + 12 parcelas iguais (13x).")

    if client:
        c1, c2 = st.columns(2)
        with c1:
            unidade_neg_limpa = st.selectbox("Unidade", UNIDADES_LIMPAS, key="n_unid")
            serie_n = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="n_serie")
        with c2:
            st.write(" ")  # espaçador visual

        valor_minimo = calcula_valor_minimo(unidade_neg_limpa, serie_n)
        st.markdown(f"### ➡️ Valor Mínimo Negociável (parcela 13x): *{format_currency(valor_minimo)}*")
        st.write("---")

        modo_simulacao = st.radio("Calcular por:", ["Bolsa (%)", "Valor da Parcela (R$)"], horizontal=True, key="modo_sim")

        precos_n = precos_2026(serie_n)
        valor_integral_parc = precos_n["parcela_mensal"]
        anuidade_integral = precos_n["anuidade"]

        if modo_simulacao == "Bolsa (%)":
            bolsa_simulada = st.slider("Porcentagem de Bolsa", 0, 100, 30, 1, key="bolsa_sim")
            valor_resultante = valor_integral_parc * (1 - bolsa_simulada / 100)
            st.metric("Parcela 13x resultante", format_currency(round(valor_resultante,2)))
            st.metric("À vista (5%)", format_currency(round(anuidade_integral*(1 - bolsa_simulada/100)*0.95,2)))
            if valor_resultante < valor_minimo:
                st.error("❌ Atenção: O valor resultante está abaixo do mínimo negociável!")
        else:
            valor_neg = st.number_input("Valor desejado da parcela (R$)", 0.0, value=1500.0, step=10.0, key="valor_neg")
            pct_req = max(0.0, 1 - valor_neg / valor_integral_parc) if valor_integral_parc > 0 else 0.0
            st.metric("Bolsa Necessária", f"{pct_req*100:.2f}%")
            st.metric("À vista (5%)", format_currency(round(anuidade_integral*(1 - pct_req)*0.95,2)))
            if valor_neg < valor_minimo:
                st.error("❌ Atenção: O valor negociado está abaixo do mínimo negociável!")
    else:
        st.warning("Não foi possível conectar ao Google Sheets para a negociação.")

# --- FORMULÁRIO BÁSICO ---
with aba_formulario:
    st.subheader("Formulário Básico de Matrícula")

    if not client:
        st.warning("Conexão com o Google Sheets não disponível.")
    else:
        try:
            ws_res = get_ws("Resultados_Bolsao")
            ensure_size(ws_res, 2000, 40)
            if ws_res:
                hmap = header_map("Resultados_Bolsao")
                COL_MENOR = "Menor valor negociável"
                COL_MENOR_FALLBACK = "Valor Limite (PIA)"
                menor_colname = COL_MENOR if COL_MENOR in hmap else (COL_MENOR_FALLBACK if COL_MENOR_FALLBACK in hmap else None)

                cols_list = [
                    "REGISTRO_ID","Nome do Aluno","Unidade","% Bolsa",
                    "Valor da Mensalidade com Bolsa","Bolsão",
                    "Escola de Origem","Valor Negociado",
                    "Aluno Matriculou?","Observações (Form)","Data/Hora"
                ]
                if menor_colname:
                    cols_list.append(menor_colname)

                missing = [c for c in cols_list if c not in hmap]
                if missing:
                    st.error(f"Faltam colunas em 'Resultados_Bolsao': {', '.join(missing)}")
                else:
                    unidade_sel = st.selectbox("Primeiro, filtre por uma unidade", ["Selecione..."] + UNIDADES_LIMPAS, key="filtro_unidade_form")
                    bolsao_options = ["Selecione..."]
                    options = {"Selecione um candidato...": None}

                    if unidade_sel != "Selecione...":
                        @st.cache_data(ttl=60)
                        def get_form_data(_unit_full: str):
                            # pode-se otimizar para ler apenas colunas mínimas no futuro
                            return ws_res.get_all_records()

                        all_data = get_form_data(UNIDADES_MAP[unidade_sel])
                        unit_full = UNIDADES_MAP[unidade_sel]
                        unit_filtered = [row for row in all_data if row.get("Unidade") == unit_full]

                        unique_bolsoes = sorted(list(set(row.get("Bolsão", "") for row in unit_filtered if row.get("Bolsão"))))
                        bolsao_options.extend(unique_bolsoes)

                        bolsao_sel = st.selectbox("Agora, selecione o bolsão", bolsao_options, key="filtro_bolsao_form")
                        if bolsao_sel != "Selecione...":
                            for row in unit_filtered:
                                if row.get("Bolsão") == bolsao_sel:
                                    reg_id = row.get("REGISTRO_ID")
                                    aluno = row.get("Nome do Aluno")
                                    if reg_id and aluno:
                                        options[f"{aluno} ({reg_id})"] = reg_id

                    selecao = st.selectbox("Selecione o Registro do Bolsão", options.keys())

                    if options.get(selecao):
                        reg_id_sel = options[selecao]
                        rownum = find_row_by_id(ws_res, hmap["REGISTRO_ID"], reg_id_sel)
                        if rownum:
                            row_data = ws_res.row_values(rownum)
                            def get_col_val(name):
                                idx = hmap.get(name)
                                return row_data[idx - 1] if idx and len(row_data) >= idx else ""

                            st.info(
                                f"**Aluno:** {get_col_val('Nome do Aluno')} | "
                                f"**Bolsa:** {get_col_val('% Bolsa')} | "
                                f"**Parcela (13x):** {get_col_val('Valor da Mensalidade com Bolsa')}"
                            )
                            st.write("---")

                            with st.form(f"form_reg_{reg_id_sel}"):
                                escola_origem = st.text_input("Escola de Origem", get_col_val("Escola de Origem"))

                                valor_neg_ini = parse_brl_to_float(get_col_val("Valor Negociado"))
                                valor_neg_num = st.number_input("Valor negociado (R$)", min_value=0.0, step=10.0, value=valor_neg_ini, format="%.2f", key="valor_neg_num")

                                matriculou_options = ["", "Sim", "Não"]
                                atual = get_col_val("Aluno Matriculou?")
                                matriculou_idx = matriculou_options.index(atual) if atual in matriculou_options else 0
                                aluno_matriculou = st.selectbox("Aluno Matriculou?", matriculou_options, index=matriculou_idx)

                                menor_val_ini = parse_brl_to_float(get_col_val(menor_colname) if menor_colname else "")
                                menor_val_num = st.number_input("Menor valor negociável (R$)", min_value=0.0, step=10.0, value=menor_val_ini, format="%.2f", key="menor_val_num")

                                obs_form = st.text_area("Observações (Form)", get_col_val("Observações (Form)"))

                                salvar = st.form_submit_button("Salvar Formulário")

                            if salvar:
                                valor_neg_num = round(float(valor_neg_num or 0), 2)
                                menor_val_num = round(float(menor_val_num or 0), 2)

                                if menor_colname and valor_neg_num and menor_val_num and valor_neg_num < menor_val_num:
                                    st.warning("⚠️ O **Valor negociado** está abaixo do **Menor valor negociável**.")

                                updates_dict = {
                                    "Escola de Origem": escola_origem,
                                    "Valor Negociado": format_currency(valor_neg_num),
                                    "Aluno Matriculou?": aluno_matriculou,
                                    "Observações (Form)": obs_form,
                                }
                                if menor_colname:
                                    updates_dict[menor_colname] = format_currency(menor_val_num)

                                updates_to_batch = []
                                for col_name, value in updates_dict.items():
                                    col_idx = hmap.get(col_name)
                                    if col_idx:
                                        a1 = gspread.utils.rowcol_to_a1(rownum, col_idx)
                                        updates_to_batch.append({"range": a1, "values": [[value]]})

                                if updates_to_batch:
                                    batch_update_cells(ws_res, updates_to_batch)
                                    st.success("Dados do formulário salvos com sucesso!")
                                    st.rerun()
        except Exception as e:
            st.error(f"Ocorreu um erro ao carregar o formulário: {e}")

# --- VALORES ---
with aba_valores:
    st.subheader("Valores 2026 (Tabela)")

    linhas = [
        ("EFI","1º Ano",24013.00,10.00,2031.85,12,2031.85,26414.00,24565.02),
        ("EFI","2º Ano",24013.00,10.00,2031.85,12,2031.85,26414.00,24565.02),
        ("EFI","3º Ano",24013.00,10.00,2031.85,12,2031.85,26414.00,24565.02),
        ("EFI","4º Ano",24013.00,10.00,2031.85,12,2031.85,26414.00,24565.02),
        ("EFI","5º Ano",24013.00,10.00,2031.85,12,2031.85,26414.00,24565.02),

        ("EFII","6º Ano",28247.00,10.00,2390.15,12,2390.15,31072.00,28896.96),
        ("EFII","7º Ano",28247.00,10.00,2390.15,12,2390.15,31072.00,28896.96),
        ("EFII","8º Ano",28247.00,10.00,2390.15,12,2390.15,31072.00,28896.96),
        ("EFII","9º Ano - Militar",30762.00,10.00,2602.92,12,2602.92,33838.00,31469.34),
        ("EFII","9º Ano - Vestibular",30762.00,10.00,2602.92,12,2602.92,33838.00,31469.34),

        ("EM","1ª Série - Militar",33036.00,10.00,2795.38,12,2795.38,36340.00,33796.20),
        ("EM","1ª Série - Vestibular",33036.00,10.00,2795.38,12,2795.38,36340.00,33796.20),
        ("EM","2ª Série - Militar",33036.00,10.00,2795.38,12,2795.38,36340.00,33796.20),
        ("EM","2ª Série - Vestibular",33036.00,10.00,2795.38,12,2795.38,36340.00,33796.20),
        ("EM","3ª série - Medicina",33164.00,10.00,2806.15,12,2806.15,36480.00,33926.40),
        ("EM","3ª Série - Militar",33164.00,10.00,2806.15,12,2806.15,36480.00,33926.40),
        ("EM","3ª Série - Vestibular",33164.00,10.00,2806.15,12,2806.15,36480.00,33926.40),

        ("PM","AFA/EN/EFOMM",13335.00,10.00,1128.38,12,1128.38,14669.00,13642.17),
        ("PM","CN/EPCAr",7985.00,10.00,675.69,12,675.69,8784.00,8169.12),
        ("PM","ESA",6437.00,10.00,544.69,12,544.69,7081.00,6585.33),
        ("PM","EsPCEx",13335.00,10.00,1128.38,12,1128.38,14669.00,13642.17),
        ("PM","IME/ITA",13335.00,10.00,1128.38,12,1128.38,14669.00,13642.17),

        ("PV","Medicina",13335.00,10.00,1128.38,12,1128.38,14669.00,13642.17),
        ("PV","Pré-Vestibular",13335.00,10.00,1128.38,12,1128.38,14669.00,13642.17),
    ]

    df = pd.DataFrame(linhas, columns=[
        "Curso","Série","Anuidade 25","% Reajuste 2026","1ª Cota",
        "Quantidade demais parcelas","Mensalidade Tabela","Anuidade Tabela",
        "Condição à vista 7% até 30/09/2025"
    ])

    # Filtro por curso
    cursos = ["Todos"] + sorted(df["Curso"].unique().tolist())
    curso_sel = st.selectbox("Filtrar por curso", cursos, index=0, key="valores_filtro_curso")
    df_filtrado = df if curso_sel == "Todos" else df[df["Curso"] == curso_sel].reset_index(drop=True)

    # Busca textual por Série
    busca = st.text_input("Pesquisar por série (ex.: 3ª, Militar, Medicina)...", key="valores_busca")
    if busca:
        df_filtrado = df_filtrado[df_filtrado["Série"].str.contains(busca, case=False, na=False)]

    # Seletor de colunas (padrão sem colunas “explicativas”)
    all_columns = df.columns.tolist()
    cols_to_exclude = ["Anuidade 25", "% Reajuste 2026", "Quantidade demais parcelas"]
    default_cols = [col for col in all_columns if col not in cols_to_exclude]
    selected_columns = st.multiselect("Selecione as colunas para exibir", options=all_columns, default=default_cols, key="col_selector")
    df_display = df_filtrado[selected_columns]

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Anuidade 25": st.column_config.NumberColumn(format="R$ %.2f"),
            "% Reajuste 2026": st.column_config.NumberColumn(format="%.2f%%"),
            "1ª Cota": st.column_config.NumberColumn(format="R$ %.2f"),
            "Quantidade demais parcelas": st.column_config.NumberColumn(format="%d"),
            "Mensalidade Tabela": st.column_config.NumberColumn(format="R$ %.2f"),
            "Anuidade Tabela": st.column_config.NumberColumn(format="R$ %.2f"),
            "Condição à vista 7% até 30/09/2025": st.column_config.NumberColumn(format="R$ %.2f"),
        },
    )

    # Download do que está na tela
    csv_bytes = df_display.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Baixar tabela atual (CSV)",
        data=csv_bytes,
        file_name="valores_2026_filtrado.csv",
        mime="text/csv",
        key="baixar_valores_2026_filtrado"
    )
