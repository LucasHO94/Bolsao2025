# -*- coding: utf-8 -*-
"""
Gerador_Carta_Bolsa.py (v2.1 - Layout Corrigido)
-------------------------------------------------
Aplicação Streamlit que gera cartas personalizadas de concessão de bolsa
(e calculadora de negociação), utilizando WeasyPrint para criar o PDF
a partir de um template HTML e CSS.

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
import weasyprint
from google.oauth2.service_account import Credentials
import gspread

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

# LISTA DE UNIDADES ATUALIZADA
UNIDADES = [
    "Bangu", "Campo Grande", "Caxias", "Madureira", "Nova Iguaçu", "Retiro dos Artistas", 
    "Rocha Miranda", "São João de Meriti", "Taquara", "Tijuca",
]

# --------------------------------------------------
# UTILITÁRIOS
# --------------------------------------------------
def calcula_bolsa(acertos: int) -> float:
    ac = max(0, min(acertos, 24))
    return BOLSA_MAP.get(ac, 0.30)

def format_currency(v: float) -> str:
    return f"R$ {v:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")

# --------------------------------------------------
# GERAÇÃO DE PDF COM WEASYPRINT
# --------------------------------------------------
def gera_pdf_html(ctx: dict) -> bytes:
    """Gera um PDF a partir de um template HTML, preenchendo com dados de `ctx`."""
    base_dir = Path(__file__).parent
    html_path = base_dir / "carta.html"
    
    with open(html_path, encoding="utf-8") as f:
        html_template = f.read()

    html_renderizado = html_template
    for k, v in ctx.items():
        html_renderizado = html_renderizado.replace(f"{{{{{k}}}}}", str(v))

    html_obj = weasyprint.HTML(string=html_renderizado, base_url=str(base_dir))
    return html_obj.write_pdf()


# --------------------------------------------------
# INTERFACE STREAMLIT
# --------------------------------------------------
st.set_page_config(page_title="Gerador de Cartas • Bolsão", layout="centered")
st.title("🎓 Gerador de Cartas de Bolsa & Calculadora de Negociação")

aba_carta, aba_negociacao = st.tabs(["Gerar Carta", "Negociação"])

with aba_carta:
    # ... (o resto da interface continua igual) ...
    c1, c2 = st.columns(2)
    with c1:
        unidade = st.selectbox("Unidade", UNIDADES, index=UNIDADES.index("Bangu"), key="c_unid")
        turma = st.text_input("Turma de interesse", "1ª série do Ensino Médio Regular", key="c_turma")
    with c2:
        ac_mat = st.number_input("Acertos - Matemática", 0, 12, 0, key="c_mat")
        ac_port = st.number_input("Acertos - Português", 0, 12, 0, key="c_port")
    aluno = st.text_input("Nome completo do candidato", "", key="c_nome")

    total = ac_mat + ac_port
    pct = calcula_bolsa(total)
    st.markdown(f"### ➔ Bolsa obtida: **{pct*100:.0f}%** ({total} acertos)")

    serie = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="c_serie")
    val_ano = TUITION[serie]["anuidade"] * (1 - pct)
    val_parc = TUITION[serie]["parcela13"] * (1 - pct)

    if st.button("Gerar Carta PDF", key="c_gerar"):
        if not aluno:
            st.error("Por favor, preencha o nome do candidato.")
        else:
            hoje = date.today()
            nome_bolsao = "-"
            try:
                # ... (código do Google Sheets continua igual) ...
                scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
                client = gspread.authorize(creds)
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
            
            # *** MUDANÇA AQUI ***
            # Gera o HTML para a lista de unidades
            unidades_html = "".join(f"<span class='unidade-item'>{unidade}</span>" for unidade in UNIDADES)

            ctx = {
                "ano": hoje.year,
                "unidade": f"Colégio Matriz – {unidade}",
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
                "unidades_html": unidades_html, # <-- Nova variável para o HTML
            }
            
            pdf_bytes = gera_pdf_html(ctx)
            st.success("✅ Carta em PDF gerada com sucesso!")

            # ... (código de registro no Google Sheets e download continua igual) ...
            try:
                if 'client' not in locals():
                    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                    creds = Credentials.from_service_account_file("credenciais.json", scopes=scope)
                    client = gspread.authorize(creds)
                    sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1qBV70qrPswnAUDxnHfBgKEU4FYAISpL7iVP0IM9zU2Q/edit#gid=380208567")

                aba_resultados = sheet.worksheet("Resultados_Bolsao")
                
                nova_linha = [
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    aluno.strip().title(), unidade, turma,
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

            st.download_button(
                "📄 Baixar Carta",
                data=pdf_bytes,
                file_name=f"Carta_Bolsa_{aluno.replace(' ', '_')}.pdf",
                mime="application/pdf"
            )

with aba_negociacao:
    # ... (aba de negociação continua igual) ...
    cn1, cn2 = st.columns(2)
    with cn1:
        serie_n = st.selectbox("Série / Modalidade", list(TUITION.keys()), key="n_serie")
        parcelas = st.radio("Parcelas", [12, 13], horizontal=True, key="n_parc")
    with cn2:
        valor_neg = st.number_input("Valor negociado por parcela (R$)", min_value=0.0, value=1500.0, step=50.0, key="n_valor")

    mensal_full = TUITION[serie_n]["parcela13"] if parcelas == 13 else TUITION[serie_n]["anuidade"] / 12
    pct_req = max(0.0, 1 - valor_neg / mensal_full) if mensal_full > 0 else 0.0
    st.metric("Bolsa necessária", f"{pct_req*100:.2f}%")
    pct_lanc = int(round(pct_req * 100 + 0.499))
    st.write(f"Sugestão de bolsa a lançar: **{pct_lanc}%**")
    mens_res = mensal_full * (1 - pct_lanc / 100)
    st.write(f"Parcela resultante: {format_currency(mens_res)} em {parcelas}×")

# --------------------------------------------------
# RODAPÉ / METADADOS
# --------------------------------------------------

st.caption("Desenvolvido para Matriz Educação • Suporte: TI Interno")
