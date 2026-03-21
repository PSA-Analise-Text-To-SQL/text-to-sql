import logging

logger = logging.getLogger(__name__)

ERROR_RULES = [
    (
        ["password authentication failed", "access denied",
         "invalid credentials", "login failed"],
        "Não foi possível conectar: usuário ou senha inválidos.",
    ),
    (
        ["could not translate host name", "name or service not known", "unknown host"],
        "Não foi possível conectar: host inválido ou inexistente.",
    ),
    (
        ["connection refused", "can't connect", "refused"],
        "Não foi possível conectar: o servidor recusou a conexão.",
    ),
    (
        ["timed out", "timeout"],
        "Não foi possível conectar: tempo de resposta excedido.",
    ),
    (
        ["unknown database", "database does not exist", "ora-12514"],
        "Não foi possível conectar: banco de dados não encontrado.",
    ),
    (
        ["permission denied", "not authorized", "insufficient privilege"],
        "Você não tem permissão para realizar esta operação.",
    ),
]

QUERY_RULES = [
    (
        ["429", "resource_exhausted", "quota exceeded", "rate limit exceeded",
         "too many requests"],
        "⏳ Limite de requisições atingido na API Google. "
        "Aguarde alguns minutos e tente novamente.",
    ),
    (
        ["api_key_invalid", "api key not valid", "invalid api key",
         "permission_denied", "api key"],
        "🔑 API Key inválida ou sem permissão. "
        "Verifique a chave no Google AI Studio.",
    ),
    (
        ["not found", "404", "deprecated", "invalid model",
         "nenhum modelo gemini disponível"],
        "⚠️ Modelo Gemini indisponível para esta API Key. "
        "Certifique-se de que o Gemini API está ativado no Google Cloud.",
    ),
    (
        ["syntax error", "sql syntax", "ora-00933", "ora-00900"],
        "A consulta gerada é inválida para este banco.",
    ),
    (
        ["operação negada", "operação de segurança"],
        "Operação bloqueada por segurança: apenas consultas SELECT são permitidas.",
    ),
]


def _match_rules(msg: str, rules: list) -> str | None:
    for keywords, response in rules:
        if any(k in msg for k in keywords):
            return response
    return None


def friendly_error_message(exc: Exception, context: str = "geral") -> str:
    raw = str(exc)
    msg = raw.lower()

    logger.error(f"[{context}] {type(exc).__name__}: {raw}")

    result = _match_rules(msg, ERROR_RULES)
    if result:
        return result

    if context == "query":
        result = _match_rules(msg, QUERY_RULES)
        if result:
            return result
        # Fallback: mostra o tipo do erro para ajudar no diagnóstico
        return f"Não foi possível executar a consulta. ({type(exc).__name__})"

    return f"Ocorreu um erro inesperado. ({type(exc).__name__})"