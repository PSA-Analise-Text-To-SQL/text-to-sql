from abc import ABC, abstractmethod

from pydantic import BaseModel, Field
from typing import Any


class DatabaseParameters(BaseModel, ABC):
    host: str = Field(..., description="Endereço do servidor")
    database: str = Field(..., description="Nome do banco")
    user: str = Field(..., min_length=1, description="Usuário do banco")
    password: str = Field(default="", description="Senha do banco")
    port: int = Field(..., gt=0, description="Porta de conexão")

    @staticmethod
    def make(dialect: str, **kwargs: Any) -> "DatabaseParameters":
        if dialect.lower() == "postgresql":
            return PostgresParameters(**kwargs)
        elif dialect.lower() == "mysql":
            return MySQLParameters(**kwargs)
        elif dialect.lower() == "oracle":
            return OracleParameters(**kwargs)
        else:
            raise ValueError(f"Banco {dialect} desconhecido.")

    @abstractmethod
    def get_dialect_name(self) -> str:
        pass

    @abstractmethod
    def get_uri(self) -> str:
        pass

class PostgresParameters(DatabaseParameters):
    port: int = 5432
    
    def get_dialect_name(self) -> str:
        return "PostgreSQL"
    
    def get_uri(self) -> str:
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class MySQLParameters(DatabaseParameters):
    port: int = 3306
    
    def get_dialect_name(self) -> str:
        return "MySQL"
    
    def get_uri(self) -> str:
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

class OracleParameters(DatabaseParameters):
    port: int = 1521
    
    def get_dialect_name(self) -> str:
        return "Oracle"
    
    def get_uri(self) -> str:
        return f"oracle+oracledb://{self.user}:{self.password}@{self.host}:{self.port}/?service_name={self.database}"
