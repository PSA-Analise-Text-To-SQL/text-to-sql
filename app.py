import os
import streamlit as st
from dotenv import load_dotenv
from src.services.llm_service import GeminiLLMService
from src.models.database_parameters import DatabaseParameters
from src.services.database_service import DatabaseService
from src.services.history_service import HistoryService
from src.repository.history_repository import HistoryRepository

load_dotenv()

if "db_service" not in st.session_state:
    st.session_state.db_service = DatabaseService()
if "db_schema" not in st.session_state:
    st.session_state.db_schema = None
if "sql_result" not in st.session_state:
    st.session_state.sql_result = None
if "df" not in st.session_state:
    st.session_state.df = None
if "explanation" not in st.session_state:
    st.session_state.explanation = None
if "analysis" not in st.session_state:
    st.session_state.analysis = None
if "llm_service" not in st.session_state:
    st.session_state.llm_service = None
if "last_question" not in st.session_state:
    st.session_state.last_question = ""
if "history_service" not in st.session_state:
    st.session_state.history_service = None


def _friendly_error_message(exc: Exception, context: str = "geral") -> str:
    msg = str(exc).lower()

    # Conexão
    if any(k in msg for k in ["password authentication failed", "access denied", "invalid credentials", "login failed"]):
        return "Não foi possível conectar: usuário ou senha inválidos."
    if any(k in msg for k in ["could not translate host name", "name or service not known", "unknown host"]):
        return "Não foi possível conectar: host inválido ou inexistente."
    if any(k in msg for k in ["connection refused", "can't connect", "refused"]):
        return "Não foi possível conectar: o servidor recusou a conexão."
    if any(k in msg for k in ["timed out", "timeout"]):
        return "Não foi possível conectar: tempo de resposta excedido."
    if any(k in msg for k in ["unknown database", "database does not exist", "ora-12514"]):
        return "Não foi possível conectar: banco de dados não encontrado."
    if any(k in msg for k in ["permission denied", "not authorized", "insufficient privilege"]):
        return "Você não tem permissão para realizar esta operação."

    # Execução SQL
    if context == "query":
        if any(k in msg for k in ["429", "resource_exhausted", "quota exceeded", "rate limit"]):
            return "⏳ Limite de requisições atingido na API Google. Tente novamente em alguns minutos ou considere um plano pago."
        if any(k in msg for k in ["erro ao gerar sql", "a ia não retornou", "api key", "invalid api"]):
            return "Não foi possível gerar a consulta: verifique a API Key ou reformule a pergunta."
        if any(k in msg for k in ["syntax error", "sql syntax", "ora-00933", "ora-00900"]):
            return "A consulta gerada é inválida para este banco."
        return "Não foi possível executar a consulta no banco."

    return "Ocorreu um erro inesperado. Tente novamente."


# --- CABEÇALHO ---
st.set_page_config(page_title="SQL Genius", layout="wide")

# --- BARRA LATERAL DE CONFIGURAÇÃO ---
with st.sidebar:
    st.title("🔌 Conexão")
    db_type = st.selectbox("Banco", ["postgresql", "mysql", "oracle"])
    host = st.text_input("Host *", value="localhost")
    database = st.text_input("Nome do Banco *")
    user = st.text_input("Usuário *")
    password = st.text_input("Senha", type="password")
    gemini_key = st.text_input("Google API Key *", type="password")

    if st.button("Conectar"):
        try:
            obrigatorios = {
                "Host": host,
                "Nome do Banco": database,
                "Usuário": user,
                "Google API Key": gemini_key,
            }
            faltando = [nome for nome, valor in obrigatorios.items()
                        if not valor]

            if faltando:
                st.warning(
                    f"Preencha os campos obrigatórios: {', '.join(faltando)}.")
                st.stop()

            config = {
                "host": host,
                "database": database,
                "user": user,
                "password": password
            }

            params = DatabaseParameters.make(db_type, **config)

            if st.session_state.db_service.connect(params):
                st.success(f"Conectado ao {params.get_dialect_name()}!")
                st.session_state.db_schema = st.session_state.db_service.get_schema()

        except Exception as e:
            st.error(_friendly_error_message(e, context="connection"))

    st.divider()
    st.subheader("📖 Banco de Histórico (MySQL)")

    if st.button("Conectar ao Histórico"):
        try:
            repo = HistoryRepository(
                host=os.getenv("HIST_HOST") or "localhost",
                user=os.getenv("HIST_USER") or "root",
                password=os.getenv("HIST_PASSWORD") or "",
                database=os.getenv("HIST_DATABASE") or "text_to_sql_history",
                port=int(os.getenv("HIST_PORT", 3306))
            )
            st.session_state.history_service = HistoryService(repo)
            st.success("Histórico conectado!")
        except Exception as e:
            st.error(f"Falha ao conectar ao histórico: {e}")

# --- INTERFACE (PRINCIPAL) ---
st.title("✨ SQL Genius")

# Verificamos se o serviço está conectado através da property que criamos
if not st.session_state.db_service.is_connected:
    st.info("👈 Conecte-se ao banco na barra lateral para começar.")
else:
    tab_query, tab_schema, tab_history = st.tabs(
        ["💬 Perguntar", "📊 Esquema", "📖 Histórico"])

    with tab_query:
        st.subheader("Faça uma pergunta sobre seus dados")
        question = st.text_area(
            "Sua pergunta:", placeholder="Ex: Qual o total de vendas por mês?")

        if st.button("Gerar e Executar"):
            if not gemini_key:
                st.warning("Insira a API Key.")
            elif not question:
                st.warning("Faça uma pergunta.")
            else:
                st.session_state.explanation = None
                st.session_state.analysis = None

                with st.spinner("IA processando..."):
                    try:
                        current_schema = st.session_state.db_service.get_schema()
                        st.session_state.llm_service = GeminiLLMService(
                            gemini_key)
                        sql_result = st.session_state.llm_service.generate_sql_query(
                            question, current_schema, db_type)
                        df = st.session_state.db_service.execute_query(
                            sql_result)

                        st.session_state.sql_result = sql_result
                        st.session_state.df = df
                        st.session_state.last_question = question

                        # Salva no histórico (se conectado)
                        if st.session_state.history_service:
                            st.session_state.history_service.record(
                                database_name=database,
                                question=question,
                                generated_query=sql_result,
                                df_result=df
                            )
                    except Exception as e:
                        st.error(_friendly_error_message(e, context="query"))

        if st.session_state.sql_result:
            st.subheader("Query Gerada")
            st.code(st.session_state.sql_result, language="sql")
            if not st.session_state.llm_service:
                st.session_state.llm_service = GeminiLLMService(gemini_key)

            if st.button("🔍 Explicar Query"):
                with st.spinner("IA traduzindo..."):
                    st.session_state.explanation = st.session_state.llm_service.explain_query(
                        st.session_state.sql_result)
            if st.session_state.explanation:
                st.info(st.session_state.explanation)
            if st.session_state.df is not None:
                st.subheader("Resultado")
                if st.session_state.df.empty:
                    st.warning("Nenhum dado encontrado.")
                else:
                    st.dataframe(st.session_state.df, use_container_width=True)
                    if st.button("📊 Analisar Resultados"):
                        with st.spinner("IA analisando..."):
                            sample = st.session_state.df.head(5).to_csv()
                            st.session_state.analysis = st.session_state.llm_service.explain_results(
                                st.session_state.last_question, sample)
                    if st.session_state.analysis:
                        st.success(st.session_state.analysis)

    with tab_schema:
        st.subheader("Esquema do Banco")
        if st.session_state.db_schema:
            st.code(st.session_state.db_schema)
        else:
            st.warning("Não foi possível obter o esquema do banco.")

    with tab_history:
        st.subheader("Histórico")
        if not st.session_state.history_service:
            st.info("Conecte-se ao banco de histórico na barra lateral.")
        else:
            if st.button("🗑️ Limpar Histórico"):
                st.session_state.history_service.clear()
                st.success("Histórico limpo!")

            entries = st.session_state.history_service.get_all()
            if not entries:
                st.info("Nenhuma consulta registrada ainda.")
            else:
                for entry in entries:
                    with st.expander(f"🗄️ {entry.database_name} | {entry.created_at.strftime('%d/%m/%Y %H:%M')} — {entry.question[:60]}"):
                        st.markdown(f"**Pergunta:** {entry.question}")
                        st.code(entry.generated_query, language="sql")
                        st.markdown("**Prévia do resultado:**")
                        st.text(entry.result_preview)
