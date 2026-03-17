ERROR_RULES = [
    (["password authentication failed", "access denied", "invalid credentials", "login failed"],
     "Não foi possível conectar: usuário ou senha inválidos."),

    (["could not translate host name", "name or service not known", "unknown host"],
     "Não foi possível conectar: host inválido ou inexistente."),

    (["connection refused", "can't connect", "refused"],
     "Não foi possível conectar: o servidor recusou a conexão."),

    (["timed out", "timeout"],
     "Não foi possível conectar: tempo de resposta excedido."),

    (["unknown database", "database does not exist", "ora-12514"],
     "Não foi possível conectar: banco de dados não encontrado."),

    (["permission denied", "not authorized", "insufficient privilege"],
     "Você não tem permissão para realizar esta operação."),
]

QUERY_RULES = [
    (["429", "resource_exhausted", "quota exceeded", "rate limit"],
     "⏳ Limite de requisições atingido na API Google. Tente novamente em alguns minutos ou considere um plano pago."),

    (["erro ao gerar sql", "a ia não retornou", "api key", "invalid api"],
     "Não foi possível gerar a consulta: verifique a API Key ou reformule a pergunta."),

    (["syntax error", "sql syntax", "ora-00933", "ora-00900"],
     "A consulta gerada é inválida para este banco."),
]

def _match_rules(msg: str, rules: list) -> str:
    for keywords, response in rules:
        if any(k in msg for k in keywords):
            return response
    return None

def friendly_error_message(exc: Exception, context: str = "geral") -> str:
    msg = str(exc).lower()

    result = _match_rules(msg, ERROR_RULES)
    if result:
        return result

    if context == "query":
        result = _match_rules(msg, QUERY_RULES)
        if result:
            return result
        return "Não foi possível executar a consulta no banco."

    return "Ocorreu um erro inesperado. Tente novamente."