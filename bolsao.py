# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v10.0 - Sincronizado com a versão Desktop)
-------------------------------------------------
Aplicação Streamlit que gera cartas, gerencia negociações e ativações de bolsão,
utilizando WeasyPrint para PDF e Pandas para manipulação de dados.
Lógica de data, preços e material didático atualizada para corresponder ao app desktop.
"""
import io
import re
import uuid
from datetime import date, timedelta, datetime
from functools import lru_cache
from pathlib import Path

import gspread
import pandas as pd
import streamlit as st
import weasyprint
from google.oauth2.service_account import Credentials
import requests
import pytz

# --------------------------------------------------
# UTILITÁRIOS DE ACESSO AO GOOGLE SHEETS
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
    """Executa múltiplas atualizações de células em uma única requisição."""
    if not updates:
        return
    fixed = []
    sheet_title_safe = ws.title.replace("'", "''")  # escapa apóstrofos
    for u in updates:
        rng = u.get("range", "")
        if not rng:
            continue
        if "!" not in rng:  # range sem nome de aba
            rng = f"'{sheet_title_safe}'!{rng}"
        fixed.append({"range": rng, "values": u.get("values", [[]])})
    body = {"valueInputOption": "USER_ENTERED", "data": fixed}
    ws.spreadsheet.values_batch_update(body)

def ensure_size(ws, min_rows=2000, min_cols=40):
    """Garante tamanho mínimo para evitar 'exceeds grid limits'."""
    try:
        if ws and (ws.row_count < min_rows or ws.col_count < min_cols):
            ws.resize(rows=max(ws.row_count, min_rows), cols=max(ws.col_count, min_cols))
    except Exception:
        pass

def new_uuid():
    """Gera um ID único e curto."""
    return uuid.uuid4().hex[:12]

@st.cache_data(ttl=300)
def load_resultados_snapshot(columns_needed: tuple[str, ...]):
    """Carrega um snapshot cacheado da aba 'Resultados_Bolsao'."""
    ws = get_ws("Resultados_Bolsao")
    if not ws:
        return {"rows": [], "id_to_rownum": {}}
    hmap = header_map("Resultados_Bolsao")
    missing = [c for c in columns_needed if c not in hmap]
    if missing:
        raise RuntimeError(f"Faltam colunas em 'Resultados_Bolsao': {', '.join(missing)}")
    
    data = ws.get_all_records(expected_headers=list(columns_needed))
    df = pd.DataFrame(data)
    rows = df.to_dict('records')
    id_to_rownum = {str(row["REGISTRO_ID"]): i + 2 for i, row in enumerate(rows) if row.get("REGISTRO_ID")}
    return {"rows": rows, "id_to_rownum": id_to_rownum}

# --------------------------------------------------
# DADOS DE REFERÊNCIA E CONFIGURAÇÕES (ATUALIZADOS)
# --------------------------------------------------
BOLSA_MAP = {
    0: .30, 1: .30, 2: .30, 3: .35, 4: .40, 5: .40, 6: .44, 7: .45, 8: .46, 9: .47,
    10: .48, 11: .49, 12: .50, 13: .51, 14: .52, 15: .53, 16: .54, 17: .55, 18: .56, 19: .57,
    20: .60, 21: .65, 22: .70, 23: .80, 24: 1.00,
}

TUITION = {
    "1ª e 2ª Série EM Militar": {"anuidade": 36670.00, "parcela13": 2820.77},
    "1ª e 2ª Série EM Vestibular": {"anuidade": 36670.00, "parcela13": 2820.77},
    "1º ao 5º Ano": {"anuidade": 26654.00, "parcela13": 2050.31},
    "3ª Série (PV/PM)": {"anuidade": 36812.00, "parcela13": 2831.69},
    "3ª Série EM Medicina": {"anuidade": 36812.00, "parcela13": 2831.69},
    "6º ao 8º Ano": {"anuidade": 31354.00, "parcela13": 2411.85},
    "9º Ano EF II Militar": {"anuidade": 34146.00, "parcela13": 2626.62},
    "9º Ano EF II Vestibular": {"anuidade": 34146.00, "parcela13": 2626.62},
    "AFA/EN/EFOMM": {"anuidade": 14802.00, "parcela13": 1138.62},
    "CN/EPCAr": {"anuidade": 8863.00, "parcela13": 681.77},
    "ESA": {"anuidade": 7145.00, "parcela13": 549.62},
    "EsPCEx": {"anuidade": 14802.00, "parcela13": 1138.62},
    "IME/ITA": {"anuidade": 14802.00, "parcela13": 1138.62},
    "Medicina (Pré)": {"anuidade": 14802.00, "parcela13": 1138.62},
    "Pré-Vestibular": {"anuidade": 14802.00, "parcela13": 1138.62},
}

TURMA_DE_INTERESSE_MAP = {
    "1ª série IME ITA Jr": "1ª e 2ª Série EM Militar", "1ª série do EM - Militar": "1ª e 2ª Série EM Militar",
    "1ª série do EM - Pré-Vestibular": "1ª e 2ª Série EM Vestibular", "1º ano do EF1": "1º ao 5º Ano",
    "2ª série IME ITA Jr": "1ª e 2ª Série EM Militar", "2ª série do EM - Militar": "1ª e 2ª Série EM Militar",
    "2ª série do EM - Pré-Vestibular": "1ª e 2ª Série EM Vestibular", "2º ano do EF1": "1º ao 5º Ano",
    "3ª série do EM - AFA EN EFOMM": "3ª Série (PV/PM)", "3ª série do EM - ESA": "3ª Série (PV/PM)",
    "3ª série do EM - EsPCEx": "3ª Série (PV/PM)", "3ª série do EM - IME ITA": "3ª Série (PV/PM)",
    "3ª série do EM - Medicina": "3ª Série EM Medicina", "3ª série do EM - Pré-Vestibular": "3ª Série (PV/PM)",
    "3º ano do EF1": "1º ao 5º Ano", "4º ano do EF1": "1º ao 5º Ano", "5º ano do EF1": "1º ao 5º Ano",
    "6º ano do EF2": "6º ao 8º Ano", "7º ano do EF2": "6º ao 8º Ano", "8º ano do EF2": "6º ao 8º Ano",
    "9º ano do EF2 - Militar": "9º Ano EF II Militar", "9º ano do EF2 - Vestibular": "9º Ano EF II Vestibular",
    "Pré-Militar AFA EN EFOMM": "AFA/EN/EFOMM", "Pré-Militar CN EPCAr": "CN/EPCAr", "Pré-Militar ESA": "ESA",
    "Pré-Militar EsPCEx": "EsPCEx", "Pré-Militar IME ITA": "IME/ITA", "Pré-Vestibular": "Pré-Vestibular",
    "Pré-Vestibular - Medicina": "Medicina (Pré)",
}
SERIE_TO_TURMA_MAP = {v: k for k, v in reversed(list(TURMA_DE_INTERESSE_MAP.items()))}

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
# FUNÇÕES DE LÓGICA E UTILITÁRIOS (ATUALIZADAS)
# --------------------------------------------------
def get_current_brasilia_date() -> date:
    """Obtém a data atual de Brasília a partir de uma API online com fallback."""
    try:
        response = requests.get("http://worldtimeapi.org/api/timezone/America/Sao_Paulo", timeout=3)
        response.raise_for_status()
        data = response.json()
        current_datetime = datetime.fromisoformat(data['datetime'])
        return current_datetime.date()
    except Exception:
        utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
        br_tz = pytz.timezone("America/Sao_Paulo")
        return utc_now.astimezone(br_tz).date()

@lru_cache(maxsize=1)
def get_bolsao_name_for_date(target_date=None):
    """Verifica a data e retorna o nome do bolsão ou 'Bolsão Avulso'."""
    if target_date is None:
        target_date = get_current_brasilia_date()
    try:
        ws_bolsao = get_ws("Bolsão")
        dates_cells = ws_bolsao.get('A2:A', value_render_option='FORMATTED_STRING')
        names_cells = ws_bolsao.get('C2:C')
        dates_col = [cell[0] for cell in dates_cells if cell]
        names_col = [cell[0] for cell in names_cells if cell]
        for i, date_str in enumerate(dates_col):
            if i < len(names_col) and names_col[i]:
                try:
                    bolsao_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                    if bolsao_date == target_date:
                        return names_col[i]
                except ValueError: continue
        return "Bolsão Avulso"
    except Exception: return "Bolsão Avulso"

def precos_2026(serie_modalidade: str) -> dict:
    base = TUITION.get(serie_modalidade, {})
    if not base: return {"primeira_cota": 0.0, "parcela_mensal": 0.0, "anuidade": 0.0}
    parcela_2026_do_dict = float(base.get("parcela13", 0.0))
    if parcela_2026_do_dict <= 0: return {"primeira_cota": 0.0, "parcela_mensal": 0.0, "anuidade": 0.0}
    parcela_2025 = round(parcela_2026_do_dict / 1.10, 2)
    primeira_cota = parcela_2025
    parcela_mensal = round(parcela_2025 * 1.093, 2)
    anuidade = round(primeira_cota + 12 * parcela_mensal, 2)
    return {"primeira_cota": primeira_cota, "parcela_mensal": parcela_mensal, "anuidade": anuidade}

def calcula_bolsa(acertos: int, serie_modalidade: str | None = None) -> float:
    """Inclui a regra especial para o EF1 (1º ao 5º Ano)."""
    if serie_modalidade == "1º ao 5º Ano":
        a = max(0, min(acertos, 10))
        if a == 0: return 0.0
        if 1 <= a <= 3: return 0.30
        if 4 <= a <= 5: return 0.50
        if 6 <= a <= 8: return 0.60
        return 0.65 # 9-10 acertos
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    try:
        return f"R$ {float(v):,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
    except (ValueError, TypeError): return str(v)

def parse_brl_to_float(x) -> float:
    if isinstance(x, (int, float)): return float(x)
    if not x: return 0.0
    s = str(x).strip().replace("R$", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except Exception: return 0.0

def format_phone_mask(raw: str) -> str:
    if raw is None: return ""
    digits = re.sub(r"\D", "", str(raw))
    if len(digits) >= 11: return f"({digits[:2]}) {digits[2:7]}-{digits[7:11]}"
    elif len(digits) == 10: return f"({digits[:2]}) {digits[2:6]}-{digits[6:10]}"
    return digits

def enforce_phone_mask(key: str):
    st.session_state[key] = format_phone_mask(st.session_state.get(key, ""))

def gerar_html_material_didatico(unidade: str) -> str:
    """Gera o HTML para as tabelas de material didático com base na unidade."""
    precos_gerais = {"Medicina": ("R$ 4.009,95", "11x de R$ 364,54"), "Pré-Vestibular": ("R$ 4.009,95", "11x de R$ 364,54")}
    precos_militares = {"AFA/EN/EFOMM": ("R$ 2.333,73", "11x de R$ 212,16"), "EPCAR": ("R$ 2.501,36", "11x de R$ 227,40"), "ESA": ("R$ 1.111,98", "11x de R$ 101,09"), "EsPCEx": ("R$ 2.668,97", "11x de R$ 242,63"), "IME/ITA": ("R$ 2.333,73", "11x de R$ 212,16")}
    precos_didatico_padrao = {"1ª ao 5ª ano": ("R$ 2.552,80", "11x de R$ 232,07"), "6ª ao 8ª ano": ("R$ 2.765,77", "11x de R$ 251,43"), "9ª ano Vestibular": ("R$ 2.872,69", "11x de R$ 261,15"), "1ª e 2ª série Vestibular": ("R$ 3.399,67", "11x de R$ 309,06"), "3ª série": ("R$ 4.009,95", "11x de R$ 364,54")}
    precos_sao_joao = {"1ª ao 5ª ano": ("R$ 1.933,56", "11x de R$ 175,78"), "6ª ao 8ª ano": ("R$ 2.020,92", "11x de R$ 183,72"), "9ª ano Vestibular": ("R$ 2.019,84", "11x de R$ 183,62"), "1ª e 2ª série Vestibular": ("R$ 2.474,20", "11x de R$ 224,93"), "3ª série": ("R$ 2.932,21", "11x de R$ 266,56")}
    precos_retiro = {"1ª ao 5ª ano": ("R$ 2.552,80", "11x de R$ 232,07")}
    
    dados_didatico = {}
    if unidade == "SÃO JOÃO DE MERITI":
        titulo_didatico = "Material Didático (exclusivo São João de Meriti)"
        dados_didatico = precos_sao_joao
    elif unidade == "RETIRO DOS ARTISTAS":
        titulo_didatico = "Material Didático"
        dados_didatico = precos_didatico_padrao.copy()
        dados_didatico.update(precos_retiro)
    else:
        titulo_didatico = "Material Didático"
        dados_didatico = precos_didatico_padrao

    tabela_didatico_html = f'<table class="pag2"><tr><th colspan="3">{titulo_didatico}</th></tr>'
    for curso, valores in dados_didatico.items(): tabela_didatico_html += f'<tr><td>{curso}</td><td>{valores[0]}</td><td>{valores[1]}</td></tr>'
    tabela_didatico_html += '</table><br>'

    tabela_geral_html = '<table class="pag2"><tr><th colspan="3">Material Didático (geral)</th></tr>'
    for curso, valores in precos_gerais.items(): tabela_geral_html += f'<tr><td>{curso}</td><td>{valores[0]}</td><td>{valores[1]}</td></tr>'
    tabela_geral_html += '</table><br>'
    
    tabela_militares_html = '<table class="pag2"><tr><th colspan="3">Material Militares</th></tr>'
    for curso, valores in precos_militares.items(): tabela_militares_html += f'<tr><td>{curso}</td><td>{valores[0]}</td><td>{valores[1]}</td></tr>'
    tabela_militares_html += '</table><br>'

    return tabela_didatico_html + tabela_geral_html + tabela_militares_html

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
        st.error("Arquivo 'carta.html' ou 'style.css' não encontrado.")
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
        cols_needed = ["Unidade", "Nome do Candidato", "Contato ID", "Status do Contato",
                       "Contato Realizado", "Observações", "Celular Tratado", "Nome",
                       "E-mail", "Turma de Interesse - Geral", "Fonte original"]
        missing_cols = [c for c in cols_needed if c not in hmap_h]
        if missing_cols:
            st.error(f"As seguintes colunas necessárias não foram encontradas na aba 'Hubspot': {', '.join(missing_cols)}")
            return pd.DataFrame()

        data = ws_hub.get_all_records(head=1)
        df = pd.DataFrame(data)
        if "Contato Realizado" in df.columns:
            df.rename(columns={"Contato Realizado": "Contato realizado"}, inplace=True)
        return df

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
# INTERFACE STREAMLIT (CÓDIGO ORIGINAL PRESERVADO)
# --------------------------------------------------
st.set_page_config(page_title="Gestor do Bolsão", layout="centered")
st.title("🎓 Gestor do Bolsão")

client = get_gspread_client()

aba_carta, aba_negociacao, aba_formulario, aba_valores = st.tabs([
    "Gerar Carta", "Negociação", "Formulário básico", "Valores"
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
    serie_modalidade_pre = "1ª e 2ª Série EM Vestibular"
    unidade_aluno_pre = "BANGU"
    opcoes_turma_interesse = list(TURMA_DE_INTERESSE_MAP.keys())

    if modo_preenchimento == "Carregar dados de um candidato":
        if client:
            df_hubspot_all = get_hubspot_data_for_activation()
            if not df_hubspot_all.empty:
                unidade_selecionada = st.selectbox(
                    "Selecione a Unidade do candidato:", UNIDADES_LIMPAS, key="unidade_selecionada_carta"
                )
                df_filtrado = df_hubspot_all[df_hubspot_all['Unidade'] == UNIDADES_MAP[unidade_selecionada]]
                nomes_candidatos = ["Selecione um candidato"] + sorted(df_filtrado['Nome do Candidato'].tolist())
                selecao_candidato = st.selectbox(
                    "Selecione o candidato da lista:", nomes_candidatos, key="selecao_candidato"
                )
                if selecao_candidato != "Selecione um candidato":
                    candidato_selecionado = df_filtrado[df_filtrado['Nome do Candidato'] == selecao_candidato].iloc[0]
                    nome_aluno_pre = candidato_selecionado.get('Nome do Candidato', '')
                    serie_modalidade_pre = candidato_selecionado.get('Turma de Interesse - Geral', '1ª e 2ª Série EM Vestibular')
                    unidade_aluno_pre = unidade_selecionada
                    turma_interesse_carregada = SERIE_TO_TURMA_MAP.get(serie_modalidade_pre, opcoes_turma_interesse[0])
                    st.session_state.c_turma = turma_interesse_carregada
                    st.session_state.c_serie = serie_modalidade_pre
                    st.info(f"Dados de {nome_aluno_pre} carregados.")
            else:
                st.warning("Nenhum candidato encontrado. Verifique se há erros de coluna na aba 'Ativação'.")

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
        turma = st.selectbox(
            "Turma de interesse",
            opcoes_turma_interesse,
            key="c_turma",
            on_change=update_serie_from_turma
        )
    with c2:
        max_por_materia = 5 if st.session_state.c_serie == "1º ao 5º Ano" else 12
        ac_mat = st.number_input("Acertos - Matemática", 0, max_por_materia, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, max_por_materia, 0, key="c_port")

    aluno = st.text_input("Nome completo do candidato", nome_aluno_pre, key="c_nome")

    total = ac_mat + ac_port
    pct = calcula_bolsa(total, st.session_state.c_serie)
    st.markdown(f"### ➔ Bolsa obtida: *{pct*100:.0f}%* ({total} acertos)")

    st.text_input("Série / Modalidade (para cálculo)", key="c_serie", disabled=True)

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
                st.error("A planilha 'Resultados_Bolsao' precisa de uma coluna chamada 'REGISTRO_ID'.")
            else:
                hoje = get_current_brasilia_date()
                nome_bolsao = get_bolsao_name_for_date(hoje)
                unidades_html = "".join(f"<span class='unidade-item'>{u}</span>" for u in UNIDADES_LIMPAS)
                html_tabelas_material = gerar_html_material_didatico(unidade_limpa)

                ctx = {
                    "ano": hoje.year,
                    "unidade": f"Colégio Matriz – {unidade_limpa}",
                    "aluno": aluno.strip().title(),
                    "bolsa_pct": f"{pct * 100:.0f}",
                    "acertos_mat": ac_mat,
                    "acertos_port": ac_port,
                    "turma": st.session_state.c_turma,
                    "n_parcelas": 12,
                    "data_limite": (hoje + timedelta(days=7)).strftime("%d/%m/%Y"),
                    "anuidade_vista": format_currency(val_ano * 0.95),
                    "primeira_cota": format_currency(val_primeira_cota),
                    "valor_parcela": format_currency(val_parcela_mensal),
                    "unidades_html": unidades_html,
                    "tabelas_material_didatico": html_tabelas_material,
                }

                pdf_bytes = gera_pdf_html(ctx)
                if pdf_bytes:
                    st.success("✅ Carta em PDF gerada com sucesso!")
                    try:
                        REGISTRO_ID = new_uuid()
                        row_data_map = {
                            "Data/Hora": datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M:%S"),
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

# --- ABA FORMULÁRIO BÁSICO ---
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

                COL_MENOR = "Expectativa de mensalidade"
                COL_MENOR_FALLBACK = "Valor Limite (PIA)"
                menor_colname = COL_MENOR if COL_MENOR in hmap else (COL_MENOR_FALLBACK if COL_MENOR_FALLBACK in hmap else None)

                base_cols = [
                    "REGISTRO_ID", "Nome do Aluno", "Unidade", "Bolsão",
                    "% Bolsa", "Valor da Mensalidade com Bolsa",
                    "Escola de Origem", "Valor Negociado",
                    "Responsável Financeiro", "Telefone",
                    "Aluno Matriculou?", "Observações (Form)", "Data/Hora"
                ]
                if menor_colname:
                    base_cols.append(menor_colname)

                if st.button("Recarregar lista (atualizar snapshot)", use_container_width=False):
                    load_resultados_snapshot.clear()

                try:
                    snapshot = load_resultados_snapshot(tuple(base_cols))
                except RuntimeError as e:
                    st.error(str(e))
                    snapshot = {"rows": [], "id_to_rownum": {}}

                if not snapshot["rows"]:
                    st.info("Nenhum registro encontrado em 'Resultados_Bolsao'.")
                else:
                    unidade_selecionada = st.selectbox(
                        "Filtrar por unidade",
                        ["Selecione..."] + UNIDADES_LIMPAS,
                        key="filtro_unidade_form"
                    )

                    if unidade_selecionada != "Selecione...":
                        unidade_completa = UNIDADES_MAP[unidade_selecionada]
                        rows_unit = [r for r in snapshot["rows"] if r.get("Unidade") == unidade_completa]

                        bolsoes = sorted({r.get("Bolsão") for r in rows_unit if r.get("Bolsão")})
                        bolsao_sel = st.selectbox(
                            "Selecione o bolsão",
                            ["Todos"] + bolsoes,
                            key="filtro_bolsao_form"
                        )

                        rows_filtered = rows_unit if bolsao_sel == "Todos" else [r for r in rows_unit if r.get("Bolsão") == bolsao_sel]

                        options = {"Selecione um candidato...": None}
                        for r in rows_filtered:
                            rid = r.get("REGISTRO_ID")
                            aluno = r.get("Nome do Aluno")
                            if rid and aluno:
                                options[f"{aluno} ({rid})"] = rid

                        selecao = st.selectbox("Selecione o Registro do Bolsão", options.keys())

                        if not options.get(selecao):
                            st.info("Selecione um registro para editar.")
                        else:
                            reg_id = options[selecao]
                            rownum = snapshot["id_to_rownum"].get(str(reg_id))
                            if not rownum:
                                st.error("Registro não localizado (ID → linha). Atualize o snapshot e tente novamente.")
                            else:
                                row = next((r for r in rows_filtered if str(r.get("REGISTRO_ID")) == str(reg_id)), None)
                                if not row:
                                    st.error("Linha não encontrada após o filtro. Atualize o snapshot.")
                                else:
                                    def get_val(col):
                                        return row.get(col, "")

                                    st.info(
                                        f"**Aluno:** {get_val('Nome do Aluno')} | "
                                        f"**Bolsa:** {get_val('% Bolsa')} | "
                                        f"**Parcela:** {get_val('Valor da Mensalidade com Bolsa')}"
                                    )
                                    st.write("---")

                                    escola_origem = st.text_input("Escola de Origem", get_val("Escola de Origem"))
                                    responsavel_fin = st.text_input("Responsável Financeiro", get_val("Responsável Financeiro"))

                                    phone_initial = format_phone_mask(get_val("Telefone"))
                                    if st.session_state.get("telefone_form_reg_id") != reg_id:
                                        st.session_state["telefone_form"] = phone_initial
                                        st.session_state["telefone_form_reg_id"] = reg_id
                                    telefone_masked = st.text_input(
                                        "Telefone",
                                        key="telefone_form",
                                        placeholder="(##) #####-####",
                                        on_change=enforce_phone_mask,
                                        args=("telefone_form",),
                                        help="Formato sugerido: (21) 98765-4321"
                                    )

                                    valor_neg_ini = parse_brl_to_float(get_val("Valor Negociado"))
                                    valor_neg_num = st.number_input(
                                        "Valor negociado (R$)", min_value=0.0, step=10.0,
                                        value=valor_neg_ini, format="%.2f", key="valor_neg_num"
                                    )

                                    matriculou_options = ["", "Sim", "Não"]
                                    atual_matric = get_val("Aluno Matriculou?")
                                    try:
                                        matriculou_idx = matriculou_options.index(atual_matric)
                                    except ValueError:
                                        matriculou_idx = 0
                                    aluno_matriculou = st.selectbox("Aluno Matriculou?", matriculou_options, index=matriculou_idx)

                                    menor_val_num = 0.0
                                    if menor_colname:
                                        menor_val_ini = parse_brl_to_float(get_val(menor_colname))
                                        menor_val_num = st.number_input(
                                            "Expectativa de mensalidade (R$)", min_value=0.0, step=10.0,
                                            value=menor_val_ini, format="%.2f", key="menor_val_num"
                                        )

                                    obs_form = st.text_area("Observações (Form)", get_val("Observações (Form)"))

                                    if st.button("Salvar Formulário"):
                                        updates_dict = {
                                            "Escola de Origem": escola_origem,
                                            "Responsável Financeiro": responsavel_fin,
                                            "Telefone": st.session_state.get("telefone_form", ""),
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
                                                a1_notation = gspread.utils.rowcol_to_a1(rownum, col_idx)
                                                updates_to_batch.append({"range": a1_notation, "values": [[value]]})

                                        if updates_to_batch:
                                            batch_update_cells(ws_res, updates_to_batch)
                                            
                                            row.update({
                                                "Escola de Origem": updates_dict.get("Escola de Origem", row.get("Escola de Origem")),
                                                "Responsável Financeiro": updates_dict.get("Responsável Financeiro", row.get("Responsável Financeiro")),
                                                "Telefone": updates_dict.get("Telefone", row.get("Telefone")),
                                                "Valor Negociado": updates_dict.get("Valor Negociado", row.get("Valor Negociado")),
                                                "Aluno Matriculou?": updates_dict.get("Aluno Matriculou?", row.get("Aluno Matriculou?")),
                                                "Observações (Form)": updates_dict.get("Observações (Form)", row.get("Observações (Form)")),
                                            })
                                            if menor_colname and menor_colname in updates_dict:
                                                row[menor_colname] = updates_dict[menor_colname]
                                            
                                            st.success("Dados do formulário salvos com sucesso!")
        except Exception as e:
            st.error(f"Ocorreu um erro ao carregar o formulário: {e}")

# --- ABA VALORES ---
with aba_valores:
    st.subheader("Valores 2026 (Tabela)")

    linhas = [
        ("EFI",  "1º Ano",                2031.85, 2031.85),
        ("EFI",  "2º Ano",                2031.85, 2031.85),
        ("EFI",  "3º Ano",                2031.85, 2031.85),
        ("EFI",  "4º Ano",                2031.85, 2031.85),
        ("EFI",  "5º Ano",                2031.85, 2031.85),

        ("EFII", "6º Ano",                2390.15, 2390.15),
        ("EFII", "7º Ano",                2390.15, 2390.15),
        ("EFII", "8º Ano",                2390.15, 2390.15),
        ("EFII", "9º Ano - Militar",      2602.92, 2602.92),
        ("EFII", "9º Ano - Vestibular",   2602.92, 2602.92),

        ("EM",   "1ª Série - Militar",    2795.38, 2795.38),
        ("EM",   "1ª Série - Vestibular", 2795.38, 2795.38),
        ("EM",   "2ª Série - Militar",    2795.38, 2795.38),
        ("EM",   "2ª Série - Vestibular", 2795.38, 2795.38),
        ("EM",   "3ª série - Medicina",   2806.15, 2806.15),
        ("EM",   "3ª Série - Militar",    2806.15, 2806.15),
        ("EM",   "3ª Série - Vestibular", 2806.15, 2806.15),

        ("PM",   "AFA/EN/EFOMM",          1128.38, 1128.38),
        ("PM",   "CN/EPCAr",              675.69,  675.69),
        ("PM",   "ESA",                   544.69,  544.69),
        ("PM",   "EsPCEx",                1128.38, 1128.38),
        ("PM",   "IME/ITA",               1128.38, 1128.38),

        ("PV",   "Medicina",              1128.38, 1128.38),
        ("PV",   "Pré-Vestibular",        1128.38, 1128.38),
    ]

    df = pd.DataFrame(linhas, columns=["Curso", "Série", "Primeira Cota", "12 parcelas de"])

    cursos = ["Todos"] + sorted(df["Curso"].unique().tolist())
    curso_sel = st.selectbox("Filtrar por curso", cursos, index=0, key="valores_filtro_curso")
    df_filtrado = df if curso_sel == "Todos" else df[df["Curso"] == curso_sel].reset_index(drop=True)

    st.dataframe(
        df_filtrado,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Primeira Cota": st.column_config.NumberColumn(format="R$ %.2f"),
            "12 parcelas de": st.column_config.NumberColumn(format="R$ %.2f"),
        },
    )
