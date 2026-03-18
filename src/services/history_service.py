import pandas as pd
from src.models.history_entry import HistoryEntry
from src.repository.history_repository import HistoryRepository


class HistoryService:
    def __init__(self, repository: HistoryRepository):
        self._repository = repository

    def record(
        self,
        database_name: str,
        question: str,
        generated_query: str,
        df_result: pd.DataFrame
    ) -> HistoryEntry:
        preview = df_result.head(5).to_string(index=False) if not df_result.empty else "Nenhum resultado."
        entry = HistoryEntry(
            database_name=database_name,
            question=question,
            generated_query=generated_query,
            result_preview=preview
        )
        return self._repository.save(entry)

    def get_all(self) -> list[HistoryEntry]:
        return self._repository.find_all()

    def clear(self):
        self._repository.clear()