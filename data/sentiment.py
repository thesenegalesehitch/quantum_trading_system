# Analyse de sentiment pour le système de trading quantique
# Utilise NLP pour analyser les sentiments des news et médias sociaux

import requests
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import re
from textblob import TextBlob
import nltk
from transformers import pipeline
import asyncio
import aiohttp
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

# Télécharger les ressources NLTK si nécessaire
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)

try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader', quiet=True)

class SentimentProvider(Enum):
    """Fournisseurs de données de sentiment."""
    NEWSAPI = "newsapi"
    FINNHUB_NEWS = "finnhub_news"
    TEXTBLOB = "textblob"
    TRANSFORMERS = "transformers"

@dataclass
class SentimentResult:
    """Résultat d'analyse de sentiment."""
    text: str
    sentiment: float  # -1 à 1
    confidence: float  # 0 à 1
    provider: str
    timestamp: datetime
    metadata: Dict = None

class SentimentAnalyzer:
    """
    Analyseur de sentiment utilisant multiple sources et méthodes.
    Supporte les news, médias sociaux et indicateurs de peur & cupidité.
    """

    def __init__(self, newsapi_key: Optional[str] = None, finnhub_key: Optional[str] = None):
        """
        Initialise l'analyseur de sentiment.

        Args:
            newsapi_key: Clé API NewsAPI (gratuite)
            finnhub_key: Clé API Finnhub
        """
        self.newsapi_key = newsapi_key
        self.finnhub_key = finnhub_key

        # Modèles NLP
        self.textblob_analyzer = None
        self.transformer_model = None

        # Cache des résultats
        self.sentiment_cache: Dict[str, List[SentimentResult]] = {}
        self.fear_greed_cache = None
        self.cache_expiry = {}

        # Statistiques
        self.stats = {
            'analyses_performed': 0,
            'api_calls': 0,
            'cache_hits': 0,
            'errors': 0
        }

    def _init_models(self):
        """Initialise les modèles NLP."""
        if self.textblob_analyzer is None:
            self.textblob_analyzer = TextBlob

        if self.transformer_model is None:
            try:
                # Utiliser un modèle léger pour l'analyse de sentiment
                self.transformer_model = pipeline(
                    "sentiment-analysis",
                    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                    return_all_scores=True
                )
                logger.info("Modèle Transformers chargé")
            except Exception as e:
                logger.warning(f"Impossible de charger le modèle Transformers: {e}")
                self.transformer_model = None

    def analyze_text_sentiment(self, text: str, provider: SentimentProvider = SentimentProvider.TEXTBLOB) -> SentimentResult:
        """
        Analyse le sentiment d'un texte.

        Args:
            text: Texte à analyser
            provider: Fournisseur/méthode d'analyse

        Returns:
            Résultat de l'analyse
        """
        self._init_models()
        self.stats['analyses_performed'] += 1

        try:
            if provider == SentimentProvider.TEXTBLOB:
                return self._analyze_textblob(text)
            elif provider == SentimentProvider.TRANSFORMERS:
                return self._analyze_transformers(text)
            else:
                return self._analyze_textblob(text)  # Fallback

        except Exception as e:
            logger.error(f"Erreur analyse sentiment: {e}")
            self.stats['errors'] += 1
            return SentimentResult(
                text=text,
                sentiment=0.0,
                confidence=0.0,
                provider=provider.value,
                timestamp=datetime.utcnow(),
                metadata={'error': str(e)}
            )

    def _analyze_textblob(self, text: str) -> SentimentResult:
        """Analyse avec TextBlob."""
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity  # -1 à 1
        confidence = min(abs(sentiment), 1.0)  # Confiance basée sur la force du sentiment

        return SentimentResult(
            text=text,
            sentiment=sentiment,
            confidence=confidence,
            provider=SentimentProvider.TEXTBLOB.value,
            timestamp=datetime.utcnow(),
            metadata={
                'subjectivity': blob.sentiment.subjectivity,
                'method': 'textblob'
            }
        )

    def _analyze_transformers(self, text: str) -> SentimentResult:
        """Analyse avec Transformers."""
        if not self.transformer_model:
            # Fallback vers TextBlob
            return self._analyze_textblob(text)

        try:
            results = self.transformer_model(text)

            # Le modèle retourne des scores pour LABEL_0 (négatif), LABEL_1 (neutre), LABEL_2 (positif)
            scores = {res['label']: res['score'] for res in results[0]}

            # Convertir en échelle -1 à 1
            neg_score = scores.get('LABEL_0', 0)
            neu_score = scores.get('LABEL_1', 0)
            pos_score = scores.get('LABEL_2', 0)

            # Sentiment = positif - négatif
            sentiment = pos_score - neg_score
            confidence = max(neg_score, neu_score, pos_score)

            return SentimentResult(
                text=text,
                sentiment=sentiment,
                confidence=confidence,
                provider=SentimentProvider.TRANSFORMERS.value,
                timestamp=datetime.utcnow(),
                metadata={
                    'negative': neg_score,
                    'neutral': neu_score,
                    'positive': pos_score,
                    'method': 'transformers'
                }
            )

        except Exception as e:
            logger.error(f"Erreur Transformers: {e}")
            return self._analyze_textblob(text)

    async def get_news_sentiment(self, symbol: str, days: int = 7) -> Dict:
        """
        Récupère et analyse le sentiment des news pour un symbole.

        Args:
            symbol: Symbole à analyser
            days: Nombre de jours d'historique

        Returns:
            Analyse de sentiment des news
        """
        cache_key = f"news_{symbol}_{days}d"

        # Vérifier le cache
        if cache_key in self.sentiment_cache and cache_key in self.cache_expiry:
            if datetime.utcnow() < self.cache_expiry[cache_key]:
                self.stats['cache_hits'] += 1
                return self._format_news_sentiment(symbol, self.sentiment_cache[cache_key])

        try:
            news_data = await self._fetch_news(symbol, days)
            sentiment_results = []

            for article in news_data:
                title = article.get('title', '')
                description = article.get('description', '')

                # Analyser le titre et la description
                if title:
                    title_sentiment = self.analyze_text_sentiment(title)
                    sentiment_results.append(title_sentiment)

                if description:
                    desc_sentiment = self.analyze_text_sentiment(description)
                    sentiment_results.append(desc_sentiment)

            # Mettre en cache
            self.sentiment_cache[cache_key] = sentiment_results
            self.cache_expiry[cache_key] = datetime.utcnow() + timedelta(hours=1)

            return self._format_news_sentiment(symbol, sentiment_results)

        except Exception as e:
            logger.error(f"Erreur récupération news pour {symbol}: {e}")
            return self._format_news_sentiment(symbol, [])

    async def _fetch_news(self, symbol: str, days: int) -> List[Dict]:
        """Récupère les news depuis les APIs gratuites."""
        news_data = []

        # NewsAPI (gratuit)
        if self.newsapi_key:
            try:
                from_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
                url = f"https://newsapi.org/v2/everything?q={symbol}&from={from_date}&sortBy=publishedAt&apiKey={self.newsapi_key}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            news_data.extend(data.get('articles', []))
                            self.stats['api_calls'] += 1

            except Exception as e:
                logger.warning(f"Erreur NewsAPI: {e}")

        # Finnhub News (gratuit)
        if self.finnhub_key:
            try:
                url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={(datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')}&to={datetime.utcnow().strftime('%Y-%m-%d')}&token={self.finnhub_key}"

                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            # Convertir le format Finnhub vers format standard
                            for item in data:
                                news_data.append({
                                    'title': item.get('headline', ''),
                                    'description': item.get('summary', ''),
                                    'publishedAt': datetime.fromtimestamp(item.get('datetime', 0)).isoformat()
                                })
                            self.stats['api_calls'] += 1

            except Exception as e:
                logger.warning(f"Erreur Finnhub news: {e}")

        # Si pas d'APIs configurées, retourner des données simulées
        if not news_data:
            logger.warning("Aucune API de news configurée, utilisation de données simulées")
            news_data = self._generate_mock_news(symbol, days)

        return news_data

    def _generate_mock_news(self, symbol: str, days: int) -> List[Dict]:
        """Génère des news simulées pour les tests."""
        import random
        news_templates = [
            f"{symbol} annonce de nouveaux records trimestriels",
            f"Analystes optimistes sur {symbol} malgré la volatilité",
            f"{symbol} fait face à des défis réglementaires",
            f"Nouvelles partnerships pour {symbol} dans le secteur tech",
            f"{symbol} investit massivement dans l'innovation"
        ]

        news = []
        for i in range(min(days * 2, 20)):  # Max 20 articles
            news.append({
                'title': random.choice(news_templates),
                'description': f"Développement important pour {symbol} dans le marché actuel.",
                'publishedAt': (datetime.utcnow() - timedelta(hours=random.randint(1, days*24))).isoformat()
            })

        return news

    def _format_news_sentiment(self, symbol: str, sentiment_results: List[SentimentResult]) -> Dict:
        """Formate les résultats de sentiment des news."""
        if not sentiment_results:
            return {
                'symbol': symbol,
                'overall_sentiment': 0.0,
                'confidence': 0.0,
                'news_count': 0,
                'sentiment_distribution': {'positive': 0, 'neutral': 0, 'negative': 0},
                'timestamp': datetime.utcnow().isoformat()
            }

        sentiments = [r.sentiment for r in sentiment_results]
        overall_sentiment = sum(sentiments) / len(sentiments)

        # Distribution
        positive = sum(1 for s in sentiments if s > 0.1)
        negative = sum(1 for s in sentiments if s < -0.1)
        neutral = len(sentiments) - positive - negative

        # Confiance basée sur la cohérence
        sentiment_std = np.std(sentiments) if len(sentiments) > 1 else 0
        confidence = max(0, 1 - sentiment_std)

        return {
            'symbol': symbol,
            'overall_sentiment': overall_sentiment,
            'confidence': confidence,
            'news_count': len(sentiment_results),
            'sentiment_distribution': {
                'positive': positive,
                'neutral': neutral,
                'negative': negative
            },
            'sentiment_range': {
                'min': min(sentiments),
                'max': max(sentiments),
                'avg': overall_sentiment
            },
            'timestamp': datetime.utcnow().isoformat()
        }

    async def get_fear_greed_index(self) -> Optional[float]:
        """
        Récupère l'indice Fear & Greed (CNN).

        Returns:
            Valeur de l'indice (0-100) ou None si erreur
        """
        # Vérifier le cache (valide 1 heure)
        if self.fear_greed_cache and datetime.utcnow() < self.fear_greed_cache['expiry']:
            return self.fear_greed_cache['value']

        try:
            # API gratuite de CNN pour Fear & Greed Index
            url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Le dernier point de données
                        latest = data['fear_and_greed_historical'][-1]
                        value = latest['y']

                        # Mettre en cache
                        self.fear_greed_cache = {
                            'value': value,
                            'expiry': datetime.utcnow() + timedelta(hours=1)
                        }

                        self.stats['api_calls'] += 1
                        return value

        except Exception as e:
            logger.error(f"Erreur récupération Fear & Greed Index: {e}")

        return None

    async def get_put_call_ratio(self, symbol: str = "SPY") -> Optional[float]:
        """
        Récupère le ratio Put/Call (approximation gratuite).

        Args:
            symbol: Symbole de référence

        Returns:
            Ratio Put/Call ou None
        """
        # Pour une vraie implémentation, utiliser une API payante
        # Ici, on simule basé sur la volatilité récente
        try:
            # Simulation basée sur des données gratuites
            # Dans la réalité, utiliser CBOE API ou autre source
            volatility = 0.2  # Valeur simulée

            # Ratio typique: plus de puts quand volatilité haute
            ratio = 0.7 + (volatility * 0.5) + (0.1 * (hash(symbol) % 100) / 100)

            return min(ratio, 2.0)  # Capped at 2.0

        except Exception as e:
            logger.error(f"Erreur calcul ratio Put/Call: {e}")
            return None

    async def get_comprehensive_sentiment(self, symbol: str, days: int = 7) -> Dict:
        """
        Analyse complète du sentiment pour un symbole.

        Args:
            symbol: Symbole à analyser
            days: Période d'analyse

        Returns:
            Analyse complète de sentiment
        """
        try:
            # News sentiment
            news_sentiment = await self.get_news_sentiment(symbol, days)

            # Fear & Greed Index
            fear_greed = await self.get_fear_greed_index()

            # Put/Call Ratio
            put_call = await self.get_put_call_ratio(symbol)

            # Sentiment global pondéré
            weights = {'news': 0.6, 'fear_greed': 0.2, 'put_call': 0.2}

            overall_sentiment = news_sentiment['overall_sentiment'] * weights['news']

            if fear_greed is not None:
                # Fear & Greed: 0=extreme fear, 100=extreme greed
                # Convertir en sentiment: -1 à 1
                fear_greed_sentiment = (fear_greed - 50) / 50
                overall_sentiment += fear_greed_sentiment * weights['fear_greed']

            if put_call is not None:
                # Put/Call: >1 = bearish, <1 = bullish
                put_call_sentiment = 1 - put_call  # Inverser et normaliser
                overall_sentiment += put_call_sentiment * weights['put_call']

            return {
                'symbol': symbol,
                'overall_sentiment': overall_sentiment,
                'components': {
                    'news_sentiment': news_sentiment,
                    'fear_greed_index': fear_greed,
                    'put_call_ratio': put_call
                },
                'weights': weights,
                'confidence': news_sentiment.get('confidence', 0.0),
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Erreur analyse sentiment complète pour {symbol}: {e}")
            return {
                'symbol': symbol,
                'overall_sentiment': 0.0,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

    def get_stats(self) -> Dict:
        """Retourne les statistiques d'utilisation."""
        return self.stats.copy()

# Instance globale
sentiment_analyzer = SentimentAnalyzer()

def get_sentiment_analyzer() -> SentimentAnalyzer:
    """Retourne l'instance globale de l'analyseur de sentiment."""
    return sentiment_analyzer
