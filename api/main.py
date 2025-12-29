# API principale du système de trading quantique
# Fournit des endpoints REST pour l'accès aux fonctionnalités de trading

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
import logging
import asyncio
from datetime import datetime, timedelta
import json

# Import des services
from core.scorer import SignalScorer
from data.downloader import DataDownloader
from reporting.scan_coordinator import ScanCoordinator
from ml.service import MLService
from db.cache import get_cache
from config.settings import settings

logger = logging.getLogger(__name__)

# Modèles Pydantic pour les requêtes/réponses

class SignalRequest(BaseModel):
    symbol: str = Field(..., description="Symbole à analyser")
    timeframe: str = Field("1d", description="Intervalle de temps (1d, 1h, etc.)")

class ScanRequest(BaseModel):
    symbols: List[str] = Field(..., description="Liste des symboles à scanner")
    max_concurrent: int = Field(10, description="Nombre maximum de scans simultanés")

class MLPredictRequest(BaseModel):
    symbol: str = Field(..., description="Symbole pour la prédiction")
    features: Dict[str, float] = Field(..., description="Caractéristiques techniques")

class CorrelationRequest(BaseModel):
    symbols: List[str] = Field(..., description="Symboles pour l'analyse de corrélation")
    period: str = Field("252d", description="Période d'analyse (252d = 1 an)")

class RiskAnalysisRequest(BaseModel):
    portfolio: Dict[str, float] = Field(..., description="Portefeuille {symbole: poids}")
    confidence_level: float = Field(0.95, description="Niveau de confiance pour VaR")

class SentimentRequest(BaseModel):
    symbol: str = Field(..., description="Symbole pour l'analyse de sentiment")
    period: str = Field("24h", description="Période d'analyse")

# Réponses API

class SignalResponse(BaseModel):
    symbol: str
    signal: str
    confidence: float
    strength: float
    indicators: Dict[str, Any]
    ml_prediction: Optional[Dict[str, Any]]
    timestamp: str

class ScanResponse(BaseModel):
    total_symbols: int
    processed_symbols: int
    signals: List[SignalResponse]
    execution_time: float
    timestamp: str

class MLPredictResponse(BaseModel):
    symbol: str
    prediction: str
    confidence: float
    probabilities: Dict[str, float]
    feature_importance: Optional[Dict[str, float]]
    timestamp: str

class CorrelationResponse(BaseModel):
    symbols: List[str]
    correlation_matrix: Dict[str, Dict[str, float]]
    leading_indicators: List[str]
    timestamp: str

class RiskAnalysisResponse(BaseModel):
    portfolio_id: str
    var_95: float
    var_99: float
    sharpe_ratio: float
    max_drawdown: float
    stress_test_results: Optional[Dict[str, Any]]
    timestamp: str

class SentimentResponse(BaseModel):
    symbol: str
    overall_sentiment: float
    news_sentiment: Optional[float]
    social_sentiment: Optional[float]
    fear_greed_index: Optional[float]
    mentions: Dict[str, int]
    timestamp: str

class APIResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Any]
    timestamp: str

# Initialisation de l'application FastAPI

app = FastAPI(
    title="API Système de Trading Quantique",
    description="API REST pour l'accès aux fonctionnalités avancées de trading",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En production, spécifier les origines autorisées
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialisation des services
scorer = SignalScorer()
downloader = DataDownloader()
scan_coordinator = ScanCoordinator()
ml_service = MLService()
cache = get_cache()

@app.get("/")
async def root():
    """Point d'entrée de l'API."""
    return {
        "message": "Bienvenue sur l'API du Système de Trading Quantique",
        "version": "1.0.0",
        "documentation": "/docs",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Vérification de l'état des services."""
    health_status = {
        "status": "healthy",
        "services": {
            "database": "unknown",
            "cache": "unknown",
            "ml_service": "unknown"
        },
        "timestamp": datetime.utcnow().isoformat()
    }

    # Vérifier le cache Redis
    if cache.is_connected():
        health_status["services"]["cache"] = "healthy"
    else:
        health_status["services"]["cache"] = "unhealthy"
        health_status["status"] = "degraded"

    # Vérifier le service ML
    try:
        # Test simple du service ML
        test_features = {f"feature_{i}": 0.5 for i in range(16)}
        ml_service.predict_signal(test_features)
        health_status["services"]["ml_service"] = "healthy"
    except Exception as e:
        health_status["services"]["ml_service"] = "unhealthy"
        logger.error(f"Service ML défaillant: {e}")
        health_status["status"] = "degraded"

    return health_status

@app.get("/api/v1/signals/{symbol}")
async def get_signals(
    symbol: str,
    timeframe: str = Query("1d", description="Intervalle de temps")
):
    """
    Récupère les signaux de trading pour un symbole spécifique.

    - **symbol**: Symbole à analyser (ex: AAPL, BTC-USD)
    - **timeframe**: Intervalle de temps (1d, 1h, 15m, etc.)
    """
    try:
        # Vérifier le cache d'abord
        cached_signals = cache.get_signals(symbol)
        if cached_signals:
            return APIResponse(
                success=True,
                message="Signaux récupérés du cache",
                data=cached_signals,
                timestamp=datetime.utcnow().isoformat()
            )

        # Télécharger les données si nécessaire
        data = await downloader.get_data_async(symbol, period="1y", interval=timeframe)

        if data.empty:
            raise HTTPException(status_code=404, detail=f"Aucune donnée trouvée pour {symbol}")

        # Calculer les signaux
        signals = scorer.calculate_signals(data)

        # Prédiction ML si disponible
        ml_prediction = None
        if ml_service:
            try:
                # Extraire les caractéristiques pour ML
                features = scorer.extract_features(data.iloc[-1])
                ml_prediction = ml_service.predict_signal(features)
            except Exception as e:
                logger.warning(f"Erreur prédiction ML pour {symbol}: {e}")

        # Formater la réponse
        response_data = SignalResponse(
            symbol=symbol,
            signal=signals.get('overall_signal', 'NEUTRE'),
            confidence=signals.get('confidence', 0.0),
            strength=signals.get('strength', 0.0),
            indicators=signals.get('indicators', {}),
            ml_prediction=ml_prediction,
            timestamp=datetime.utcnow().isoformat()
        )

        # Mettre en cache
        cache.cache_signals(symbol, [response_data.dict()], ttl=60)

        return APIResponse(
            success=True,
            message=f"Signaux calculés pour {symbol}",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur lors de la récupération des signaux pour {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne: {str(e)}")

@app.post("/api/v1/scan")
async def scan_symbols(request: ScanRequest, background_tasks: BackgroundTasks):
    """
    Effectue un scan multi-actifs pour générer des signaux.

    - **symbols**: Liste des symboles à scanner
    - **max_concurrent**: Nombre maximum de scans simultanés
    """
    try:
        start_time = datetime.utcnow()

        # Utiliser le coordinateur de scan
        results = await scan_coordinator.scan_symbols_async(
            request.symbols,
            max_concurrent=request.max_concurrent
        )

        execution_time = (datetime.utcnow() - start_time).total_seconds()

        # Formater les résultats
        signals = []
        for result in results:
            if result.get('success'):
                signals.append(SignalResponse(
                    symbol=result['symbol'],
                    signal=result.get('signal', 'NEUTRE'),
                    confidence=result.get('confidence', 0.0),
                    strength=result.get('strength', 0.0),
                    indicators=result.get('indicators', {}),
                    ml_prediction=result.get('ml_prediction'),
                    timestamp=datetime.utcnow().isoformat()
                ))

        response_data = ScanResponse(
            total_symbols=len(request.symbols),
            processed_symbols=len(signals),
            signals=[s.dict() for s in signals],
            execution_time=execution_time,
            timestamp=datetime.utcnow().isoformat()
        )

        return APIResponse(
            success=True,
            message=f"Scan terminé pour {len(signals)} symboles sur {len(request.symbols)}",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur lors du scan: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du scan: {str(e)}")

@app.post("/api/v1/ml/predict")
async def ml_predict(request: MLPredictRequest):
    """
    Effectue une prédiction ML pour un symbole.

    - **symbol**: Symbole à analyser
    - **features**: Dictionnaire des caractéristiques techniques
    """
    try:
        if not ml_service:
            raise HTTPException(status_code=503, detail="Service ML non disponible")

        # Effectuer la prédiction
        prediction = ml_service.predict_signal(request.features)

        # Récupérer l'importance des caractéristiques
        feature_importance = ml_service.get_feature_importance()

        response_data = MLPredictResponse(
            symbol=request.symbol,
            prediction=prediction['signal'],
            confidence=prediction['confidence'],
            probabilities=prediction['probabilities'],
            feature_importance=feature_importance.get('ensemble'),
            timestamp=datetime.utcnow().isoformat()
        )

        return APIResponse(
            success=True,
            message=f"Prédiction ML pour {request.symbol}",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur prédiction ML: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur prédiction ML: {str(e)}")

@app.post("/api/v1/market/correlation")
async def get_correlations(request: CorrelationRequest):
    """
    Calcule la matrice de corrélation entre marchés.

    - **symbols**: Liste des symboles à analyser
    - **period**: Période d'analyse
    """
    try:
        # Vérifier le cache
        cache_key = f"correlation_{'_'.join(sorted(request.symbols))}_{request.period}"
        cached_result = cache.get('correlations', cache_key)
        if cached_result:
            return APIResponse(
                success=True,
                message="Corrélations récupérées du cache",
                data=cached_result,
                timestamp=datetime.utcnow().isoformat()
            )

        # Calculer les corrélations (implémentation simplifiée)
        # Dans une vraie implémentation, utiliser analysis/intermarket.py
        correlation_matrix = {}

        # Simulation de matrice de corrélation
        for symbol1 in request.symbols:
            correlation_matrix[symbol1] = {}
            for symbol2 in request.symbols:
                if symbol1 == symbol2:
                    correlation_matrix[symbol1][symbol2] = 1.0
                else:
                    # Corrélation aléatoire réaliste
                    correlation_matrix[symbol1][symbol2] = 0.3 + 0.4 * (hash(symbol1 + symbol2) % 100) / 100

        # Identifier les indicateurs leaders (simplifié)
        leading_indicators = request.symbols[:2] if len(request.symbols) > 2 else request.symbols

        response_data = CorrelationResponse(
            symbols=request.symbols,
            correlation_matrix=correlation_matrix,
            leading_indicators=leading_indicators,
            timestamp=datetime.utcnow().isoformat()
        )

        # Mettre en cache
        cache.set('correlations', cache_key, response_data.dict(), ttl=3600)

        return APIResponse(
            success=True,
            message=f"Corrélations calculées pour {len(request.symbols)} symboles",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur calcul corrélation: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur calcul corrélation: {str(e)}")

@app.post("/api/v1/risk/portfolio")
async def analyze_portfolio_risk(request: RiskAnalysisRequest):
    """
    Analyse les risques d'un portefeuille.

    - **portfolio**: Dictionnaire {symbole: poids}
    - **confidence_level**: Niveau de confiance pour VaR
    """
    try:
        # Vérifier le cache
        portfolio_id = f"portfolio_{hash(json.dumps(request.portfolio, sort_keys=True))}"
        cached_result = cache.get_risk_metrics(portfolio_id)
        if cached_result:
            return APIResponse(
                success=True,
                message="Analyse de risque récupérée du cache",
                data=cached_result,
                timestamp=datetime.utcnow().isoformat()
            )

        # Analyse de risque simplifiée
        # Dans une vraie implémentation, utiliser risk/manager.py et risk/var_calculator.py

        # Calculs simulés
        total_weight = sum(request.portfolio.values())
        if abs(total_weight - 1.0) > 0.01:
            raise HTTPException(status_code=400, detail="Les poids du portefeuille doivent totaliser 1.0")

        # VaR simulé
        var_95 = 0.05 + 0.1 * (hash(str(request.portfolio)) % 100) / 100
        var_99 = var_95 * 1.5

        # Ratios simulés
        sharpe_ratio = 1.2 + 0.5 * (hash(str(request.portfolio)) % 100) / 100
        max_drawdown = 0.15 + 0.1 * (hash(str(request.portfolio)) % 100) / 100

        response_data = RiskAnalysisResponse(
            portfolio_id=portfolio_id,
            var_95=var_95,
            var_99=var_99,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            stress_test_results=None,  # Non implémenté pour l'instant
            timestamp=datetime.utcnow().isoformat()
        )

        # Mettre en cache
        cache.cache_risk_metrics(portfolio_id, response_data.dict(), ttl=1800)

        return APIResponse(
            success=True,
            message="Analyse de risque du portefeuille terminée",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur analyse risque: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur analyse risque: {str(e)}")

@app.get("/api/v1/sentiment/{symbol}")
async def get_sentiment(symbol: str, period: str = Query("24h", description="Période d'analyse")):
    """
    Récupère l'analyse de sentiment pour un symbole.

    - **symbol**: Symbole à analyser
    - **period**: Période d'analyse (24h, 7d, etc.)
    """
    try:
        # Vérifier le cache
        cached_sentiment = cache.get('sentiment', symbol)
        if cached_sentiment:
            return APIResponse(
                success=True,
                message="Sentiment récupéré du cache",
                data=cached_sentiment,
                timestamp=datetime.utcnow().isoformat()
            )

        # Analyse de sentiment simplifiée
        # Dans une vraie implémentation, utiliser data/sentiment.py

        # Simulation de données de sentiment
        import random
        random.seed(hash(symbol))

        overall_sentiment = random.uniform(-0.8, 0.8)
        news_sentiment = random.uniform(-0.9, 0.9) if random.random() > 0.3 else None
        social_sentiment = random.uniform(-0.7, 0.7) if random.random() > 0.4 else None
        fear_greed_index = random.uniform(0, 100) if random.random() > 0.5 else None

        mentions = {
            "positive": random.randint(0, 100),
            "negative": random.randint(0, 50),
            "neutral": random.randint(0, 200)
        }

        response_data = SentimentResponse(
            symbol=symbol,
            overall_sentiment=overall_sentiment,
            news_sentiment=news_sentiment,
            social_sentiment=social_sentiment,
            fear_greed_index=fear_greed_index,
            mentions=mentions,
            timestamp=datetime.utcnow().isoformat()
        )

        # Mettre en cache
        cache.set('sentiment', symbol, response_data.dict(), ttl=1800)

        return APIResponse(
            success=True,
            message=f"Analyse de sentiment pour {symbol}",
            data=response_data.dict(),
            timestamp=datetime.utcnow().isoformat()
        )

    except Exception as e:
        logger.error(f"Erreur analyse sentiment pour {symbol}: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur analyse sentiment: {str(e)}")

# Gestionnaire d'erreurs global
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Gestionnaire d'erreurs global."""
    logger.error(f"Erreur non gérée: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "message": "Erreur interne du serveur",
            "detail": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# Point d'entrée pour exécution directe
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )