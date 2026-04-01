import pytest
from src.utils.error_handling import friendly_error_message

# Função helper para criar exceções com mensagens específicas
def err(msg: str) -> Exception:
    return Exception(msg)

# Erros de conexão
class TestConnectionErrors:
    @pytest.mark.parametrize("msg", [
        "password authentication failed", "access denied for user",
        "invalid credentials provided", "login failed for user",
    ])
    def test_invalid_credentials(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "usuário ou senha" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "could not translate host name", "name or service not known",
        "unknown host 'meu-servidor'",
    ])
    def test_invalid_host(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "host" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "connection refused", "can't connect to mysql server",
        "refused to connect",
    ])
    def test_invalid_connection(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "recusou" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "timed out after 30s", "connection timeout",
    ])
    def test_timeout(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "tempo" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "unknown database 'inexistente'", "database does not exist",
        "ora-12514 tns listener",
    ])
    def test_database_not_found(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "banco" in result.lower() or "encontrado" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "permission denied to user", "not authorized to perform",
        "insufficient privilege",
    ])
    def test_insufficient_privilege(self, msg):
        result = friendly_error_message(err(msg), context="connection")
        assert "permissão" in result.lower()
        
# Erros de consulta        
class TestQueryErrors:
    @pytest.mark.parametrize("msg", [
        "429 resource_exhausted quota exceeded", "rate limit exceeded",
        "too many requests",
    ])
    def test_request_limit(self, msg):
        result = friendly_error_message(err(msg), context="query")
        assert "limite" in result.lower() or "aguarde" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "api_key_invalid", "api key not valid please check",
        "invalid api key provided", "permission_denied api key",
    ])
    def test_invalid_api_key(self, msg):
        result = friendly_error_message(err(msg), context="query")
        assert "api key" in result.lower() or "chave" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "not found 404 model", "deprecated model version",
        "invalid model name", "nenhum modelo gemini disponível",
    ])
    def test_unavailable_model(self, msg):
        result = friendly_error_message(err(msg), context="query")
        assert "gemini" in result.lower() or "modelo" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "syntax error near SELECT", "sql syntax error",
        "ora-00933 sql command", "ora-00900 invalid sql",
    ])
    def test_invalid_query(self, msg):
        result = friendly_error_message(err(msg), context="query")
        assert "inválida" in result.lower() or "consulta" in result.lower()
 
    @pytest.mark.parametrize("msg", [
        "operação negada: apenas SELECT", "operação de segurança ativada",
    ])
    def test_security_blocked_operation(self, msg):
        result = friendly_error_message(err(msg), context="query")
        assert "segurança" in result.lower() or "bloqueada" in result.lower()
 
    def test_unknown_error_returns_fallback_with_type(self):
        result = friendly_error_message(
            ValueError("algo muito específico"), context="query"
        )
        assert "ValueError" in result
        
# Erros genéricos        
class TestGenericContext:
    def test_unknown_error_returns_fallback(self):
        result = friendly_error_message(err("xpto totalmente desconhecido"))
        assert "inesperado" in result.lower()
 
    def test_returns_string_in_any_case(self):
        result = friendly_error_message(RuntimeError("falha"), context="geral")
        assert isinstance(result, str)
        assert len(result) > 0