from google import genai


def generate_sql_query(question, schema, api_key, current_dialect):
    try:
        client = genai.Client(api_key=api_key)

        bd_type = "MySQL"

        prompt = f"""
        Você é um especialista em SQL. Converta a pergunta do usuário em uma consulta SQL válida baseada no schema fornecido e no tipo de banco de dados informado.

        SCHEMA DO BANCO DE DADOS:
        {schema}

        TIPO DE BANCO DE DADOS
        {bd_type}
        
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

        response = client.models.generate_content(
            model="gemini-3-flash-preview",
            contents=prompt
        )

        if response.text:
            clean_sql = response.text.replace(
                "```sql", "").replace("```", "").strip()
            return clean_sql
        else:
            raise Exception("Erro: A IA não retornou texto.")
    except Exception as e:
        raise Exception(f"Erro ao gerar SQL: {e}")
