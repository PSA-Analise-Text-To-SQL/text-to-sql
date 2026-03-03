from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from src.models.database_parameters import DatabaseParameters
import pandas as pd
import re

class DatabaseService:
    def __init__(self):
        self._engine: Engine | None = None
        self._params: DatabaseParameters | None = None
        self._schema_cache: str | None = None

    def connect(self, params: DatabaseParameters):
        try:
            new_engine = create_engine(params.get_uri())
            with new_engine.connect() as conn:
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
        # Remove espaços em branco do início e fim e converte para maiúsculo
        sql_clean = sql.strip().upper()

        # 1. Validação Primária: A query OBRIGATORIAMENTE deve começar com SELECT
        if not sql_clean.startswith("SELECT"):
            raise ValueError("Operação negada: Apenas comandos de leitura (SELECT) são permitidos.")

        # 2. Validação Secundária: Bloqueio de palavras-chave destrutivas
        # Usamos regex \b para garantir que estamos buscando a palavra exata 
        forbidden_keywords = [
            "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", 
            "TRUNCATE", "REPLACE", "GRANT", "REVOKE", "MERGE"
        ]
        
        for keyword in forbidden_keywords:
            if re.search(rf'\b{keyword}\b', sql_clean):
                raise ValueError(f"Operação de segurança ativada: O comando restrito '{keyword}' não é permitido.")

        return sql

    def execute_query(self, sql: str) -> pd.DataFrame:
        if not self._engine:
            raise RuntimeError("Conecte-se a um banco primeiro.")

        # Aplicação do tratamento de exceções focado na execução segura
        try:
            safe_sql = self._sanitize_query(sql)

            with self._engine.connect() as conn:
                return pd.read_sql(text(safe_sql), conn)
                
        except ValueError as ve:
            raise ve
        except Exception as e:
            raise RuntimeError(f"Erro na execução da consulta SQL: {e}")

    @property
    def is_connected(self) -> bool:
        return self._engine is not None