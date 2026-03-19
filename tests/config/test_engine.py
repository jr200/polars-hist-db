from unittest.mock import patch, MagicMock
import pytest
from polars_hist_db.config.engine import DbEngineConfig, SslConfig, _engine_cache


class TestDbEngineConfig:
    @pytest.fixture(autouse=True)
    def clear_engine_cache(self):
        _engine_cache.clear()
        yield
        _engine_cache.clear()

    def test_basic_config(self):
        config = DbEngineConfig(hostname="localhost")
        assert config.hostname == "localhost"
        assert config.backend == "mariadb"
        assert config.port == 3306
        assert config.username is None
        assert config.password is None
        assert config.ssl_config is None

    def test_ssl_config_from_dict(self):
        ssl_dict = {"ssl_ca": "/path/to/ca.pem", "ssl_cert": "/path/to/cert.pem"}
        config = DbEngineConfig(hostname="localhost", ssl_config=ssl_dict)
        assert isinstance(config.ssl_config, SslConfig)
        assert config.ssl_config.ssl_ca == "/path/to/ca.pem"
        assert config.ssl_config.ssl_cert == "/path/to/cert.pem"
        assert config.ssl_config.ssl_key is None

    def test_ssl_config_from_dataclass(self):
        ssl = SslConfig(ssl_ca="/path/to/ca.pem")
        config = DbEngineConfig(hostname="localhost", ssl_config=ssl)
        assert isinstance(config.ssl_config, SslConfig)
        assert config.ssl_config.ssl_ca == "/path/to/ca.pem"

    def test_ssl_config_full(self):
        ssl_dict = {
            "ssl_ca": "/ca.pem",
            "ssl_cert": "/cert.pem",
            "ssl_key": "/key.pem",
        }
        config = DbEngineConfig(hostname="localhost", ssl_config=ssl_dict)
        assert config.ssl_config.ssl_ca == "/ca.pem"
        assert config.ssl_config.ssl_cert == "/cert.pem"
        assert config.ssl_config.ssl_key == "/key.pem"

    @patch("polars_hist_db.config.engine.create_engine")
    def test_get_engine_no_ssl(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DbEngineConfig(
            hostname="127.0.0.1",
            port=3307,
            username="root",
            password="secret",
        )
        engine = config.get_engine()

        assert engine == mock_engine
        mock_create_engine.assert_called_once()
        call_kwargs = mock_create_engine.call_args
        assert "mariadb+pymysql://root:secret@127.0.0.1:3307" == call_kwargs[0][0]
        assert call_kwargs[1]["connect_args"] == {"client_flag": 0}

    @patch("polars_hist_db.config.engine.create_engine")
    def test_get_engine_with_ssl(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DbEngineConfig(
            hostname="127.0.0.1",
            port=3307,
            username="root",
            password="secret",
            ssl_config={"ssl_ca": "/ca.pem", "ssl_cert": "/cert.pem"},
        )
        engine = config.get_engine()

        assert engine == mock_engine
        call_kwargs = mock_create_engine.call_args
        connect_args = call_kwargs[1]["connect_args"]
        assert "ssl" in connect_args
        assert connect_args["ssl"]["ssl_ca"] == "/ca.pem"
        assert connect_args["ssl"]["ssl_cert"] == "/cert.pem"
        # ssl_key is None so should be filtered out
        assert "ssl_key" not in connect_args["ssl"]

    @patch("polars_hist_db.config.engine.create_engine")
    def test_get_engine_no_credentials(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DbEngineConfig(hostname="127.0.0.1", port=3306)
        config.get_engine()

        call_kwargs = mock_create_engine.call_args
        assert "mariadb+pymysql://127.0.0.1:3306" == call_kwargs[0][0]

    @patch("polars_hist_db.config.engine.create_engine")
    def test_get_engine_caching(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DbEngineConfig(
            hostname="127.0.0.1", port=3307, username="root", password="secret"
        )
        engine1 = config.get_engine()
        engine2 = config.get_engine()

        assert engine1 is engine2
        assert mock_create_engine.call_count == 1

    @patch("polars_hist_db.config.engine.create_engine")
    def test_dispose(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        config = DbEngineConfig(
            hostname="127.0.0.1", port=3307, username="root", password="secret"
        )
        config.get_engine()
        config.dispose()

        mock_engine.dispose.assert_called_once()

        # After dispose, get_engine should create a new engine
        config.get_engine()
        assert mock_create_engine.call_count == 2
