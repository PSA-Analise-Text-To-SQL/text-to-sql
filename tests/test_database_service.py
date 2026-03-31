import pytest
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from unittest.mock import patch, MagicMock


@patch("src.services.database_service.create_engine")
def test_connect_success(mock_create_engine, db_service, mock_params):
    # Arrange
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    # Act
    result = db_service.connect(mock_params)

    # Assert
    assert result is True
    assert db_service.is_connected is True
    mock_engine.connect.assert_called_once()


@patch("src.services.database_service.create_engine")
def test_connect_failure(mock_create_engine, db_service, mock_params):
    # Arrange
    mock_create_engine.side_effect = Exception("Erro de rede")

    # Act & Assert
    with pytest.raises(ConnectionError, match="Falha ao conectar no banco"):
        db_service.connect(mock_params)


def test_sanitize_query_happy_path(db_service):
    # Arrange
    sql_da_llm = "SELECT name FROM users"

    # Act
    result = db_service._sanitize_query(sql_da_llm)

    # Assert
    assert result == sql_da_llm


def test_sanitize_query_sad_path_injection(db_service):
    # Arrange
    sql_malicioso = "SELECT * FROM users; DROP TABLE users"

    # Act & Assert
    with pytest.raises(ValueError, match="O comando restrito 'DROP' não é permitido"):
        db_service._sanitize_query(sql_malicioso)


def test_sanitize_query_sad_path_not_select(db_service):
    # Arrange
    sql_not_read = "UPDATE users SET name = 'Hacker'"

    # Act & Assert
    with pytest.raises(ValueError, match="Apenas comandos de leitura"):
        db_service._sanitize_query(sql_not_read)


@patch("pandas.read_sql")
def test_execute_query_success(mock_read_sql, db_service):
    # Arrange
    db_service._engine = MagicMock()
    mock_df = pd.DataFrame({"id": [1], "name": ["Teste"]})
    mock_read_sql.return_value = mock_df

    # Act
    result = db_service.execute_query("SELECT * FROM users")

    # Assert
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    mock_read_sql.assert_called_once()


@patch("pandas.read_sql")
def test_execute_query_sad_path_hallucination(mock_read_sql, db_service):
    # Arrange
    db_service._engine = MagicMock()
    mock_read_sql.side_effect = ProgrammingError("SELECT col_inexistente", {}, None)

    # Act & Assert
    with pytest.raises(RuntimeError, match="A inteligência artificial gerou uma consulta inválida"):
        db_service.execute_query("SELECT col_inexistente FROM users")


@patch("src.services.database_service.inspect")
def test_get_schema_success(mock_inspect, db_service):
    # Arrange
    mock_engine = MagicMock()
    mock_engine.dialect.name = "postgresql"
    db_service._engine = mock_engine

    mock_inspector = MagicMock()
    mock_inspect.return_value = mock_inspector

    mock_inspector.get_table_names.return_value = ["users"]

    mock_inspector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER"},
        {"name": "email", "type": "VARCHAR(255)"}
    ]

    # Act
    schema_result = db_service.get_schema()

    # Assert
    assert "users(id:INT, email:TEXT)" in schema_result
    assert db_service._schema_cache == schema_result

@patch("src.services.database_service.inspect")
def test_get_schema_uses_cache(mock_inspect, db_service):
    # Arrange
    db_service._engine = MagicMock()
    db_service._schema_cache = "tabela_fake(col:TEXT)"

    # Act
    result = db_service.get_schema()

    # Assert
    assert result == "tabela_fake(col:TEXT)"
    mock_inspect.assert_not_called()