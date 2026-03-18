from pydantic import BaseModel, Field
from datetime import datetime


class HistoryEntry(BaseModel):
    id: int | None = Field(default=None, description="ID gerado pelo banco")
    database_name: str = Field(..., description="Nome do banco consultado")
    question: str = Field(..., description="Pergunta feita pelo usuário")
    generated_query: str = Field(..., description="Query SQL gerada pela IA")
    result_preview: str = Field(..., description="Prévia do resultado em texto")
    created_at: datetime = Field(default_factory=datetime.now)