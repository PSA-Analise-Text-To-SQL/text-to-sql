from abc import ABC, abstractmethod

class LLMService(ABC):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = self._create_client()

    @abstractmethod
    def _create_client(self):
        pass

    @abstractmethod
    def _call_model(self, prompt: str) -> str:
        pass

    def generate_sql_query(self, question: str, schema: str, db_type: str) -> str:
        try:
            bd_type = db_type.upper() if db_type else "GENERIC SQL"
            prompt = self._build_prompt(question, schema, bd_type)
            response_text = self._call_model(prompt)

            if not response_text:
                raise Exception("Erro: A IA não retornou texto.")
            return self._clean_response(response_text)

        except Exception as e:
            raise Exception(f"Erro ao gerar SQL: {e}")

    def _build_prompt(self, question: str, schema: str, db_type: str) -> str:
        return f"""
        Você é um especialista em SQL. Converta a pergunta do usuário em uma consulta SQL válida baseada no schema fornecido e no tipo de banco de dados informado.

        SCHEMA DO BANCO DE DADOS:
        {schema}

        TIPO DE BANCO DE DADOS
        {db_type}
        
        PERGUNTA DO USUÁRIO:
        "{question}"
        
        REGRAS ESTRITAS:
        1. Retorne APENAS o código SQL puro.
        2. NÃO use markdown (sem ```sql ou ```). Apenas o texto da query.
        3. Não invente colunas.
        4. Não responda com outras operações além de SELECT.
        5. Caso seja identificado um pedido não condizente com SELECT, retorne uma mensagem de erro.
        6. Caso haja nomes de colunas iguais em 2 tabelas de uma query, utilize apelidos 'AS' para evitar confusão.
        """

    def _clean_response(self, text: str) -> str:
        return text.replace("```sql", "").replace("```", "").strip()
    

from google import genai


class GeminiLLMService(LLMService):

    def _create_client(self):
        return genai.Client(api_key=self.api_key)

    def _call_model(self, prompt: str) -> str:
        response = self.client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt
        )

        return response.text if response.text else None