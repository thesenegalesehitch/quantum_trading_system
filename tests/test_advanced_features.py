# Tests pour les fonctionnalités avancées du système de trading quantique

import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch
import asyncio
from datetime import datetime, timedelta

# Import des modules à tester
from ml.service import MLService
from analysis.intermarket import InterMarketAnalyzer
from data.realtime import RealTimeDataManager, DataProvider
from data.sentiment import SentimentAnalyzer, SentimentProvider
from risk.manager import RiskManager, VaRMethod
from db.cache import RedisCache
from api.main import app
from fastapi.testclient import TestClient

class TestMLService:
    """Tests pour le service d'apprentissage automatique."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.ml_service = MLService(model_path="tests/models")

    def test_initialization(self):
        """Test l'initialisation du service ML."""
        assert self.ml_service.models is not None
        assert isinstance(self.ml_service.feature_columns, list)
        assert len(self.ml_service.feature_columns) > 0

    def test_predict_signal(self):
        """Test la prédiction de signaux."""
        # Données de test
        features = {
            'rsi': 65.0,
            'macd': 0.5,
            'macd_signal': 0.3,
            'macd_hist': 0.2,
            'bb_upper': 105.0,
            'bb_middle': 100.0,
            'bb_lower': 95.0,
            'stoch_k': 70.0,
            'stoch_d': 65.0,
            'williams_r': -30.0,
            'cci': 100.0,
            'mfi': 75.0,
            'volume_ratio': 1.2,
            'price_change': 0.02,
            'volatility': 0.15,
            'trend_strength': 0.8
        }

        result = self.ml_service.predict_signal(features)

        assert 'signal' in result
        assert 'confidence' in result
        assert 'probabilities' in result
        assert result['signal'] in ['VENTE', 'ACHAT', 'NEUTRE']
        assert 0 <= result['confidence'] <= 1

    def test_feature_importance(self):
        """Test la récupération de l'importance des caractéristiques."""
        importance = self.ml_service.get_feature_importance()

        assert isinstance(importance, dict)
        # Au moins un modèle devrait avoir de l'importance
        assert len(importance) > 0 or any(v for v in importance.values() if v)

class TestInterMarketAnalyzer:
    """Tests pour l'analyseur inter-marchés."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.analyzer = InterMarketAnalyzer()

    def test_correlation_calculation(self):
        """Test le calcul des corrélations."""
        # Données de test synthétiques
        symbols = ['AAPL', 'MSFT', 'GOOGL']
        data_dict = {}

        dates = pd.date_range('2023-01-01', periods=100, freq='D')
        for symbol in symbols:
            np.random.seed(hash(symbol) % 2**32)
            prices = 100 * np.exp(np.cumsum(np.random.normal(0.001, 0.02, 100)))
            df = pd.DataFrame({'Close': prices}, index=dates)
            data_dict[symbol] = df

        correlations = self.analyzer.calculate_correlations(symbols, data_dict)

        assert isinstance(correlations, pd.DataFrame)
        assert correlations.shape == (3, 3)
        # La diagonale devrait être 1
        assert np.allclose(np.diag(correlations), 1.0)

    def test_identify_leaders(self):
        """Test l'identification des leaders."""
        # Matrice de corrélation synthétique
        correlation_matrix = pd.DataFrame({
            'AAPL': [1.0, 0.8, 0.6],
            'MSFT': [0.8, 1.0, 0.7],
            'GOOGL': [0.6, 0.7, 1.0]
        }, index=['AAPL', 'MSFT', 'GOOGL'])

        leaders = self.analyzer.identify_leaders(correlation_matrix)

        assert isinstance(leaders, list)
        assert len(leaders) <= 3

class TestSentimentAnalyzer:
    """Tests pour l'analyseur de sentiment."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.analyzer = SentimentAnalyzer()

    @patch('data.sentiment.aiohttp.ClientSession')
    async def test_news_sentiment(self, mock_session):
        """Test l'analyse de sentiment des news."""
        # Mock de la réponse API
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = asyncio.coroutine(lambda: {
            'articles': [
                {'title': 'AAPL annonce de bons résultats', 'description': 'Cours en hausse'},
                {'title': 'AAPL face à des défis', 'description': 'Préoccupations des investisseurs'}
            ]
        })

        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value = mock_response

        result = await self.analyzer.get_news_sentiment('AAPL', days=1)

        assert 'symbol' in result
        assert 'overall_sentiment' in result
        assert 'news_count' in result
        assert result['symbol'] == 'AAPL'

    def test_text_sentiment_analysis(self):
        """Test l'analyse de sentiment de texte."""
        positive_text = "Les résultats d'AAPL dépassent les attentes, cours en forte hausse"
        negative_text = "AAPL annonce des pertes importantes, panique sur les marchés"

        pos_result = self.analyzer.analyze_text_sentiment(positive_text)
        neg_result = self.analyzer.analyze_text_sentiment(negative_text)

        assert pos_result.sentiment > 0
        assert neg_result.sentiment < 0
        assert 0 <= pos_result.confidence <= 1

class TestRiskManager:
    """Tests pour le gestionnaire de risque."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.manager = RiskManager()

    def test_var_calculation(self):
        """Test le calcul de VaR."""
        portfolio = {'AAPL': 0.6, 'MSFT': 0.4}

        # Données synthétiques
        historical_data = self.manager._generate_synthetic_data(portfolio.keys())

        result = self.manager.calculate_var(portfolio, 0.95, VaRMethod.HISTORICAL, historical_data)

        assert hasattr(result, 'var_95')
        assert hasattr(result, 'volatility')
        assert hasattr(result, 'max_drawdown')
        assert result.confidence_level == 0.95
        assert result.var_95 >= 0

    def test_stress_test(self):
        """Test les tests de stress."""
        portfolio = {'AAPL': 0.5, 'MSFT': 0.5}

        result = self.manager.stress_test(portfolio, 'covid_19')

        assert hasattr(result, 'scenario_name')
        assert hasattr(result, 'portfolio_loss')
        assert hasattr(result, 'var_breach')
        assert result.scenario_name == 'COVID-19'

    def test_portfolio_optimization(self):
        """Test l'optimisation de portefeuille."""
        assets = ['AAPL', 'MSFT', 'GOOGL']
        constraints = {}

        historical_data = self.manager._generate_synthetic_data(assets)

        result = self.manager.optimize_portfolio(assets, constraints, historical_data)

        assert 'weights' in result
        assert 'expected_return' in result
        assert 'volatility' in result
        assert len(result['weights']) == len(assets)
        # Vérifier que les poids somment à environ 1
        total_weight = sum(result['weights'].values())
        assert abs(total_weight - 1.0) < 0.01

class TestRedisCache:
    """Tests pour le cache Redis."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.cache = RedisCache()

    def test_cache_operations(self):
        """Test les opérations de cache de base."""
        # Ces tests peuvent échouer si Redis n'est pas disponible
        # Dans ce cas, ils sont ignorés

        if not self.cache.is_connected():
            pytest.skip("Redis non disponible pour les tests")

        # Test set/get
        success = self.cache.set('test', 'key', {'data': 'value'})
        assert success or not self.cache.is_connected()

        if self.cache.is_connected():
            data = self.cache.get('test', 'key')
            assert data == {'data': 'value'}

            # Test exists
            assert self.cache.exists('test', 'key')

            # Test delete
            assert self.cache.delete('test', 'key')
            assert not self.cache.exists('test', 'key')

class TestAPI:
    """Tests pour l'API FastAPI."""

    def setup_method(self):
        """Configuration avant chaque test."""
        self.client = TestClient(app)

    def test_root_endpoint(self):
        """Test le endpoint racine."""
        response = self.client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert 'message' in data
        assert 'version' in data

    def test_health_endpoint(self):
        """Test le endpoint de santé."""
        response = self.client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert 'status' in data
        assert 'services' in data

    @patch('api.main.downloader')
    @patch('api.main.scorer')
    def test_signals_endpoint(self, mock_scorer, mock_downloader):
        """Test le endpoint des signaux."""
        # Mock des dépendances
        mock_data = pd.DataFrame({
            'Close': [100, 101, 102],
            'High': [102, 103, 104],
            'Low': [98, 99, 100],
            'Open': [99, 100, 101],
            'Volume': [1000000, 1100000, 1200000]
        })

        mock_downloader.get_data_async.return_value = mock_data
        mock_scorer.calculate_signals.return_value = {
            'overall_signal': 'ACHAT',
            'confidence': 0.8,
            'strength': 0.7,
            'indicators': {'rsi': 65}
        }

        response = self.client.get("/api/v1/signals/AAPL")

        assert response.status_code == 200
        data = response.json()
        assert data['success'] is True
        assert 'data' in data

# Tests d'intégration
class TestIntegration:
    """Tests d'intégration des composants."""

    def test_full_ml_pipeline(self):
        """Test le pipeline ML complet."""
        ml_service = MLService()

        # Données de test
        features = {col: 0.5 for col in ml_service.feature_columns}

        # Prédiction
        result = ml_service.predict_signal(features)
        assert result['signal'] in ['VENTE', 'ACHAT', 'NEUTRE']

        # Réentraînement avec données fictives
        new_data = pd.DataFrame({
            **{col: [0.5, 0.6, 0.4] for col in ml_service.feature_columns},
            'target': [1, 0, 2]  # ACHAT, VENTE, NEUTRE
        })

        ml_service.update_models(new_data)
        # Le service devrait gérer le réentraînement sans erreur

    def test_risk_sentiment_integration(self):
        """Test l'intégration risque-sentiment."""
        risk_manager = RiskManager()
        sentiment_analyzer = SentimentAnalyzer()

        portfolio = {'AAPL': 0.7, 'MSFT': 0.3}

        # Analyse de risque
        risk_analysis = risk_manager.run_comprehensive_risk_analysis(portfolio)

        assert 'risk_metrics' in risk_analysis
        assert 'stress_tests' in risk_analysis

        # Analyse de sentiment (synchrone pour test)
        # Note: Dans un vrai test, utiliser asyncio
        # sentiment = asyncio.run(sentiment_analyzer.get_comprehensive_sentiment('AAPL'))

# Configuration pytest
pytest_plugins = ["pytest_asyncio"]

# Fixture pour les tests asynchrones
@pytest.fixture
async def async_setup():
    """Configuration pour les tests asynchrones."""
    # Initialisation des services asynchrones si nécessaire
    yield
    # Nettoyage

if __name__ == "__main__":
    # Exécution des tests
    pytest.main([__file__, "-v"])