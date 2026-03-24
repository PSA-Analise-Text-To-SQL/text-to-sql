import re
import time
from abc import ABC, abstractmethod

from google import genai
from google.genai import types

class LLMService(ABC):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = self._create_client()

    @abstractmethod
    def _create_client(self) -> genai.Client:
        pass

    @abstractmethod
    def _call_model(self, prompt: str) -> str:
        pass

    def generate_sql_query(self, question: str, schema: str, db_type: str) -> str:
        bd_type = db_type.upper() if db_type else "GENERIC SQL"
        prompt = self._build_prompt(question, schema, bd_type)
        response_text = self._call_model(prompt)
        if not response_text:
            raise RuntimeError("A IA retornou uma resposta vazia.")
        return self._clean_response(response_text)

    def _build_prompt(self, question: str, schema: str, db_type: str) -> str:
        return f"""
        Você é um especialista em SQL.
        Converta a pergunta do usuário em uma consulta SQL válida
        baseada no schema fornecido e no tipo de banco de dados informado.

        SCHEMA DO BANCO DE DADOS:
        {schema}

        TIPO DE BANCO DE DADOS
        {db_type}

        PERGUNTA DO USUÁRIO:
        "{question}"

        REGRAS ESTRITAS:
        1. Retorne APENAS o código SQL puro.
        2. NÃO use markdown (sem ```sql ou ```). Apenas o texto da query.
        3. Não invente colunas; caso seja informada uma coluna inesistente,
        retorne uma mensagem de aviso.
        4. Não responda com outras operações além de SELECT.
        5. Caso seja identificado um pedido não condizente com SELECT,
        retorne uma mensagem de erro.
        6. Caso haja nomes de colunas iguais em 2 tabelas de uma query,
        utilize apelidos 'AS' para evitar confusão.
        """

    def _clean_response(self, text: str) -> str:
        return text.replace("```sql", "").replace("```", "").strip()

class GeminiLLMService(LLMService):
    # Modelos ativos no free tier em março 2026, do mais capaz ao mais leve
    _MODEL_FALLBACKS = [
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
    ]

    # Erros que justificam tentar o próximo modelo da lista
    _SKIP_MODEL_ERRORS = [
        "not found", "404", "deprecated", "invalid model",
        "model is not supported",
    ]

    # Erros de cota que permitem retry com backoff no MESMO modelo
    _RETRY_ERRORS = [
        "429", "resource_exhausted", "quota exceeded", "rate limit",
    ]

    def _create_client(self) -> genai.Client:
        return genai.Client(api_key=self.api_key)

    @staticmethod
    def _extract_retry_delay(error_str: str) -> float:
        """
        Extrai o retryDelay sugerido pela API do Google na mensagem de erro.
        Ex: 'Please retry in 15.86s' → 16.0
        Retorna 30.0 como fallback seguro se não encontrar.
        """
        match = re.search(r"retry in\s+([\d.]+)s", error_str, re.IGNORECASE)
        if match:
            return float(match.group(1)) + 1.0  # +1s de margem
        return 30.0

    def _call_model(self, prompt: str) -> str:
        last_error = None

        for model_name in self._MODEL_FALLBACKS:
            for attempt in range(2):
                try:
                    response = self.client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=types.GenerateContentConfig(
                            # max_output_tokens=1024,
                            temperature=0.0,
                        ),
                    )
                    if response.text:
                        return response.text

                except Exception as e:
                    error_str = str(e)
                    error_lower = error_str.lower()

                    # Modelo não existe mais → pula direto para o próximo
                    if any(k in error_lower for k in self._SKIP_MODEL_ERRORS):
                        last_error = e
                        break

                    # Erro de cota momentânea (RPM) → espera e tenta de novo
                    if any(k in error_lower for k in self._RETRY_ERRORS):
                        last_error = e
                        if attempt == 0:
                            wait = self._extract_retry_delay(error_str)
                            time.sleep(wait)
                            continue  # tenta de novo o mesmo modelo
                        else:
                            break
                    raise

        raise RuntimeError(
            "Nenhum modelo Gemini disponível com esta API Key. "
            f"Último erro: {last_error}"
        )

    def explain_query(self, query: str) -> str:
        try:
            prompt = (
                "Explique de forma didática e enxuta o que a seguinte query "
                "SQL faz, passo a passo:\n"
                f"{query}"
            )
            response_text = self._call_model(prompt)

            if not response_text:
                raise Exception("Erro: A IA não retornou texto.")
            return response_text.strip()
        except Exception as e:
            raise Exception(f"Erro ao explicar SQL: {e}") from e

    def explain_results(self, question: str, results: str) -> str:
        try:
            prompt = f"""
            Pergunta original: {question}
            Dados retornados: {results}

            Explique o que esses dados significam em relação à pergunta do usuário.
            Seja breve e didático, destacando insights importantes.
            Evite repetir a pergunta ou os dados, foque na interpretação.
            """
            response_text = self._call_model(prompt)

            if not response_text:
                raise Exception("Erro: A IA não retornou texto.")
            return response_text.strip()
        except Exception as e:
            raise Exception(f"Erro ao explicar resultados: {e}") from e
