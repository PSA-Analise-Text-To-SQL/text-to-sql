import logging
import re
from typing import NoReturn

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

from src.models.database_parameters import DatabaseParameters
import pandas as pd # type: ignore
import re
import logging

# Regista falhas de segurança e erros da IA num ficheiro oculto
logging.basicConfig(
    filename="security_audit.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class DatabaseService:
    def __init__(self) -> None:
        self._engine: Engine | None = None
        self._params: DatabaseParameters | None = None
        self._schema_cache: str | None = None

    def connect(self, params: DatabaseParameters) -> bool:
        try:
            new_engine = create_engine(params.get_uri())
            with new_engine.connect():
                pass
            self._engine = new_engine
            self._params = params
            self._schema_cache = None
            return True
        except Exception as e:
            raise ConnectionError(f"Falha ao conectar no banco: {e}") from e

    def get_schema(self) -> str:
        if not self._engine:
            return "Nenhum banco conectado."

        if self._schema_cache:
            return self._schema_cache

        inspector = inspect(self._engine)
        schema_text = ""
        for table_name in inspector.get_table_names():
            schema_text += f"\nTabela: {table_name}\n"
            for col in inspector.get_columns(table_name):
                schema_text += f" - {col['name']} ({col['type']})\n"

        self._schema_cache = schema_text
        return schema_text

    def _sanitize_query(self, sql: str) -> str:
        """
        Garante que apenas comandos de leitura (SELECT) sejam executados
        e bloqueia operações destrutivas.
        """
        sql_clean = sql.strip().upper()

        # 1. Validação Primária: Bloqueia tudo o que não for SELECT
        if not sql_clean.startswith("SELECT"):
            logging.warning(f"Ataque bloqueado (Não é SELECT). Comando recebido: {sql}")
            raise ValueError(
                "Operação negada: Apenas comandos de leitura (SELECT) são permitidos."
            )

        # 2. Validação Secundária: Bloqueia palavras-chave destrutivas (Regex)
        forbidden_keywords = [
            "DROP",
            "DELETE",
            "UPDATE",
            "INSERT",
            "ALTER",
            "TRUNCATE",
            "REPLACE",
            "GRANT",
            "REVOKE",
            "MERGE",
        ]

        for keyword in forbidden_keywords:
            if re.search(rf"\b{keyword}\b", sql_clean):
                logging.warning(
                    f"Ataque bloqueado (Palavra restrita '{keyword}')."
                    f"Comando recebido: {sql}"
                )
                raise ValueError(
                    "Operação de segurança ativada: "
                    f"O comando restrito '{keyword}' não é permitido."
                )
        return sql

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._engine:
            raise RuntimeError("Conecte-se a um banco primeiro.")

        safe_sql = self._sanitize_query(sql)
        try:
            with self._engine.connect() as conn:
                return pd.read_sql(text(safe_sql), conn)
        except Exception as e:
            self._handle_query_error(e, sql)

    def _handle_query_error(self, e: Exception, sql: str) -> NoReturn:
        if isinstance(e, ValueError):
            raise e

        if isinstance(e, ProgrammingError):
            logging.error(f"Alucinação da IA: {sql} | {e}")
            raise RuntimeError(
                "A inteligência artificial gerou uma consulta inválida "
                "ou referenciou colunas inexistentes."
            )

        if isinstance(e, OperationalError):
            logging.error(f"Erro de ligação: {e}")
            raise RuntimeError(
                "A ligação com o banco de dados falhou durante a consulta."
            )

        logging.error(f"Erro inesperado: {e}")
        raise RuntimeError("Ocorreu um erro inesperado ao processar os dados.") from e

    @property
    def is_connected(self) -> bool:
        return self._engine is not None
