import logging
import re
from typing import NoReturn
 
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError
 
from src.models.database_parameters import DatabaseParameters

# Regista falhas de segurança e erros da IA num ficheiro oculto
logging.basicConfig(
    filename="security_audit.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class DatabaseService:
    def __init__(self):
        self._engine: Engine | None = None
        self._params: DatabaseParameters | None = None
        self._schema_cache: str | None = None

    def connect(self, params: DatabaseParameters):
        try:
            new_engine = create_engine(params.get_uri())
            with new_engine.connect():
                pass
            
            self._engine = new_engine
            self._params = params
            self._schema_cache = None  
            return True
        except Exception as e:
            raise ConnectionError(f"Falha ao conectar no banco: {e}")

    def get_schema(self) -> str:
        if not self._engine:
            return "Nenhum banco conectado."

        if self._schema_cache:
            return self._schema_cache

        inspector = inspect(self._engine)
        dialect = self._engine.dialect.name
        tables = self._get_user_tables(inspector, dialect)
        
        lines = []
        for table_name in tables:
            kwargs = {"schema": "public"} if dialect == "postgresql" else {}
            cols = inspector.get_columns(table_name, **kwargs)
            col_defs = ", ".join(
                f"{c['name']}:{self._simplify_type(c['type'])}" for c in cols
            )
            lines.append(f"{table_name}({col_defs})")
 
        self._schema_cache = "\n".join(lines)
        return self._schema_cache

    def _get_user_tables(self, inspector, dialect: str) -> list[str]:
        SYSTEM_PREFIXES: dict[str, list[str]] = {
            "postgresql": [],
            "mysql": [],
            "oracle": [
                "AQ$", "DR$", "DEF$", "LOGMNR", "MVIEW$", "OL$",
                "REPCAT$", "SCHEDULER$", "STREAMS$", "WRH$", "WRI$",
                "WRM$", "XDB$", "APEX$", "DBMS_", "SYS_",
            ],
        }
 
        if dialect == "postgresql":
            return inspector.get_table_names(schema="public")
 
        all_tables = inspector.get_table_names()
        prefixes = SYSTEM_PREFIXES.get(dialect, [])
 
        if not prefixes:
            return all_tables
 
        return [
            t for t in all_tables
            if not any(t.upper().startswith(p) for p in prefixes)
        ]
        
    def _sanitize_query(self, sql: str) -> str:
        sql_clean = sql.strip().upper()

        if not sql_clean.startswith("SELECT"):
            logging.warning(f"Ataque bloqueado (Não é SELECT). Comando recebido: {sql}")
            raise ValueError("Operação negada: Apenas comandos de leitura (SELECT) são permitidos.")

        forbidden_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", 
            "TRUNCATE", "REPLACE", "GRANT", "REVOKE", "MERGE"
        ]
        
        for keyword in forbidden_keywords:
            if re.search(rf'\b{keyword}\b', sql_clean):
                logging.warning(f"Ataque bloqueado (Palavra restrita '{keyword}'). Comando recebido: {sql}")
                raise ValueError(f"Operação de segurança ativada: O comando restrito '{keyword}' não é permitido.")

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

    def _handle_query_error(self, e: Exception, sql: str):
        if isinstance(e, ValueError):
            raise e

        if isinstance(e, ProgrammingError):
            logging.error(f"Alucinação da IA: {sql} | {e}")
            raise RuntimeError(
                "A inteligência artificial gerou uma consulta inválida ou referenciou colunas inexistentes."
            )

        if isinstance(e, OperationalError):
            logging.error(f"Erro de ligação: {e}")
            raise RuntimeError(
                "A ligação com o banco de dados falhou durante a consulta."
            )

        logging.error(f"Erro inesperado: {e}")
        raise RuntimeError("Ocorreu um erro inesperado ao processar os dados.")

    @staticmethod
    def _simplify_type(col_type) -> str:
        type_str = str(col_type).upper()
 
        SIMPLIFICATIONS = [
            (["VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT",
              "CLOB", "NCLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
              "STRING", "BPCHAR"], "TEXT"),
            (["BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT",
              "MEDIUMINT", "NUMBER", "SERIAL", "BYTEINT"], "INT"),
            (["NUMERIC", "DECIMAL", "FLOAT", "DOUBLE",
              "REAL", "BINARY_FLOAT", "BINARY_DOUBLE"], "NUM"),
            (["TIMESTAMP", "DATETIME"], "DATETIME"),
            (["DATE"], "DATE"),
            (["TIME"], "TIME"),
            (["BOOLEAN", "BOOL", "BIT"], "BOOL"),
            (["BLOB", "BINARY", "VARBINARY", "RAW", "LONG RAW", "BYTEA"], "BYTES"),
            (["JSON", "JSONB"], "JSON"),
            (["UUID"], "UUID"),
        ]
 
        for prefixes, label in SIMPLIFICATIONS:
            if any(type_str.startswith(p) for p in prefixes):
                return label
        
        return type_str.split("(")[0]

    @property
    def is_connected(self) -> bool:
        return self._engine is not None