"""
Sources de données multiples avec fallback automatique.
Combine plusieurs APIs gratuites pour une fiabilité maximale.

Sources supportées:
- Yahoo Finance (yfinance) - Illimité
- Alpha Vantage - 25 requêtes/jour gratuit
- Polygon.io - 5 requêtes/min gratuit
- Finnhub - 60 requêtes/min gratuit
- FRED - Données économiques 120 req/min
- CCXT - 100+ exchanges crypto
- Binance - Crypto illimité
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
import time
import os
import sys
import requests
from functools import wraps

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config


def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    """Décorateur de retry avec backoff exponentiel."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    delay = base_delay * (2 ** attempt)
                    print(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée: {e}")
                    if attempt < max_retries - 1:
                        print(f"   Nouvelle tentative dans {delay:.1f}s...")
                        time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


class DataSourceBase(ABC):
    """Classe de base pour toutes les sources de données."""
    
    def __init__(self, name: str, rate_limit: int = 60):
        self.name = name
        self.rate_limit = rate_limit  # requêtes par minute
        self.last_request_time = 0
        self.request_count = 0
    
    def _rate_limit_check(self):
        """Vérifie et applique le rate limiting."""
        current_time = time.time()
        if current_time - self.last_request_time >= 60:
            self.request_count = 0
            self.last_request_time = current_time
        
        if self.request_count >= self.rate_limit:
            wait_time = 60 - (current_time - self.last_request_time)
            if wait_time > 0:
                print(f"⏳ Rate limit atteint pour {self.name}, attente {wait_time:.1f}s")
                time.sleep(wait_time)
                self.request_count = 0
                self.last_request_time = time.time()
        
        self.request_count += 1
    
    @abstractmethod
    def fetch_data(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Récupère les données pour un symbole."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Vérifie si la source est disponible."""
        pass


class YahooFinanceSource(DataSourceBase):
    """Source de données Yahoo Finance (yfinance)."""
    
    def __init__(self):
        super().__init__("Yahoo Finance", rate_limit=2000)
        try:
            import yfinance as yf
            self.yf = yf
            self._available = True
        except ImportError:
            self._available = False
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        if not self._available:
            return pd.DataFrame()
        
        self._rate_limit_check()
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=730))
        
        ticker = self.yf.Ticker(symbol)
        df = ticker.history(
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=True
        )
        
        if df.empty:
            return pd.DataFrame()
        
        return self._normalize_dataframe(df)
    
    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalise le DataFrame au format standard."""
        df.columns = [col.capitalize() for col in df.columns]
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        available_cols = [col for col in required_cols if col in df.columns]
        return df[available_cols].copy()


class PolygonSource(DataSourceBase):
    """Source de données Polygon.io (actions, forex, crypto)."""
    
    def __init__(self, api_key: str = None):
        super().__init__("Polygon.io", rate_limit=5)
        self.api_key = api_key or os.getenv('POLYGON_API_KEY', '')
        self.base_url = "https://api.polygon.io"
        self._available = bool(self.api_key)
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        if not self._available:
            return pd.DataFrame()
        
        self._rate_limit_check()
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=365))
        
        # Convertir l'intervalle
        multiplier, timespan = self._convert_interval(interval)
        
        # Format du symbole
        ticker = symbol.replace("=X", "").replace("/", "")
        if len(ticker) == 6:  # Forex
            ticker = f"C:{ticker}"
        
        url = (
            f"{self.base_url}/v2/aggs/ticker/{ticker}/range/"
            f"{multiplier}/{timespan}/{start_date.strftime('%Y-%m-%d')}/"
            f"{end_date.strftime('%Y-%m-%d')}"
        )
        
        response = requests.get(url, params={'apiKey': self.api_key}, timeout=30)
        
        if response.status_code != 200:
            return pd.DataFrame()
        
        data = response.json()
        
        if 'results' not in data or not data['results']:
            return pd.DataFrame()
        
        df = pd.DataFrame(data['results'])
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
        df = df.set_index('timestamp')
        df = df.rename(columns={
            'o': 'Open',
            'h': 'High',
            'l': 'Low',
            'c': 'Close',
            'v': 'Volume'
        })
        
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    def _convert_interval(self, interval: str) -> Tuple[int, str]:
        """Convertit l'intervalle au format Polygon."""
        mapping = {
            '1m': (1, 'minute'),
            '5m': (5, 'minute'),
            '15m': (15, 'minute'),
            '30m': (30, 'minute'),
            '1h': (1, 'hour'),
            '4h': (4, 'hour'),
            '1d': (1, 'day'),
        }
        return mapping.get(interval, (1, 'hour'))


class FinnhubSource(DataSourceBase):
    """Source de données Finnhub (actions, forex, crypto)."""
    
    def __init__(self, api_key: str = None):
        super().__init__("Finnhub", rate_limit=60)
        self.api_key = api_key or os.getenv('FINNHUB_API_KEY', '')
        self.base_url = "https://finnhub.io/api/v1"
        self._available = bool(self.api_key)
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        if not self._available:
            return pd.DataFrame()
        
        self._rate_limit_check()
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=365))
        
        # Convertir l'intervalle
        resolution = self._convert_interval(interval)
        
        # Format forex pour Finnhub
        finnhub_symbol = symbol.replace("=X", "").replace("/", "")
        if len(finnhub_symbol) == 6:
            finnhub_symbol = f"OANDA:{finnhub_symbol[:3]}_{finnhub_symbol[3:]}"
        
        url = f"{self.base_url}/forex/candle"
        params = {
            'symbol': finnhub_symbol,
            'resolution': resolution,
            'from': int(start_date.timestamp()),
            'to': int(end_date.timestamp()),
            'token': self.api_key
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            return pd.DataFrame()
        
        data = response.json()
        
        if data.get('s') != 'ok' or 'c' not in data:
            return pd.DataFrame()
        
        df = pd.DataFrame({
            'Open': data['o'],
            'High': data['h'],
            'Low': data['l'],
            'Close': data['c'],
            'Volume': data.get('v', [0] * len(data['c']))
        })
        
        df.index = pd.to_datetime(data['t'], unit='s')
        return df
    
    def _convert_interval(self, interval: str) -> str:
        """Convertit l'intervalle au format Finnhub."""
        mapping = {
            '1m': '1',
            '5m': '5',
            '15m': '15',
            '30m': '30',
            '1h': '60',
            '4h': '240',
            '1d': 'D',
            '1wk': 'W',
            '1mo': 'M',
        }
        return mapping.get(interval, '60')


class FREDSource(DataSourceBase):
    """Source de données FRED (Federal Reserve Economic Data)."""
    
    def __init__(self, api_key: str = None):
        super().__init__("FRED", rate_limit=120)
        self.api_key = api_key or os.getenv('FRED_API_KEY', '')
        self.base_url = "https://api.stlouisfed.org/fred"
        self._available = bool(self.api_key)
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1d",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        """
        Récupère les données économiques FRED.
        
        Séries populaires:
        - DEXUSEU: Taux EUR/USD
        - GOLDPMGBD228NLBM: Prix de l'or
        - DGS10: Taux 10 ans US
        - VIXCLS: Indice VIX
        - CPIAUCSL: CPI US
        - UNRATE: Taux de chômage US
        """
        if not self._available:
            return pd.DataFrame()
        
        self._rate_limit_check()
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=365*5))
        
        url = f"{self.base_url}/series/observations"
        params = {
            'series_id': symbol,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date.strftime('%Y-%m-%d'),
            'observation_end': end_date.strftime('%Y-%m-%d'),
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            return pd.DataFrame()
        
        data = response.json()
        
        if 'observations' not in data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data['observations'])
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.rename(columns={'value': 'Close'})
        
        # Simuler OHLC pour compatibilité
        df['Open'] = df['Close']
        df['High'] = df['Close']
        df['Low'] = df['Close']
        df['Volume'] = 0
        
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    def fetch_economic_indicators(self) -> Dict[str, pd.DataFrame]:
        """Récupère les principaux indicateurs économiques."""
        indicators = {
            'EUR_USD': 'DEXUSEU',
            'GOLD': 'GOLDPMGBD228NLBM',
            'US_10Y': 'DGS10',
            'VIX': 'VIXCLS',
            'CPI': 'CPIAUCSL',
            'UNEMPLOYMENT': 'UNRATE',
            'FED_RATE': 'FEDFUNDS',
        }
        
        data = {}
        for name, series_id in indicators.items():
            df = self.fetch_data(series_id)
            if not df.empty:
                data[name] = df
        
        return data


class CCXTSource(DataSourceBase):
    """Source de données CCXT (100+ exchanges crypto)."""
    
    def __init__(self, exchange: str = 'binance'):
        super().__init__(f"CCXT-{exchange}", rate_limit=1200)
        self.exchange_name = exchange
        try:
            import ccxt
            self.ccxt = ccxt
            self.exchange = getattr(ccxt, exchange)()
            self._available = True
        except (ImportError, AttributeError):
            self._available = False
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        if not self._available:
            return pd.DataFrame()
        
        self._rate_limit_check()
        
        # Convertir le symbole au format CCXT
        ccxt_symbol = symbol.replace("-", "/")
        if "/" not in ccxt_symbol:
            ccxt_symbol = f"{ccxt_symbol[:3]}/{ccxt_symbol[3:]}"
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=365))
        
        # Convertir l'intervalle
        timeframe = self._convert_interval(interval)
        
        since = int(start_date.timestamp() * 1000)
        
        try:
            ohlcv = self.exchange.fetch_ohlcv(ccxt_symbol, timeframe, since=since, limit=1000)
        except Exception as e:
            print(f"⚠️ Erreur CCXT: {e}")
            return pd.DataFrame()
        
        if not ohlcv:
            return pd.DataFrame()
        
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')
        
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]
    
    def _convert_interval(self, interval: str) -> str:
        """Convertit l'intervalle au format CCXT."""
        return interval  # CCXT utilise le même format


class BinanceSource(DataSourceBase):
    """Source de données Binance (crypto gratuit illimité)."""
    
    def __init__(self):
        super().__init__("Binance", rate_limit=1200)
        self.base_url = "https://api.binance.com/api/v3"
        self._available = True  # Pas besoin d'API key pour les données publiques
    
    def is_available(self) -> bool:
        return self._available
    
    @retry_with_backoff(max_retries=3)
    def fetch_data(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None
    ) -> pd.DataFrame:
        self._rate_limit_check()
        
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=365))
        
        # Format Binance (ex: BTCUSDT)
        binance_symbol = symbol.replace("/", "").replace("-", "").upper()
        
        url = f"{self.base_url}/klines"
        params = {
            'symbol': binance_symbol,
            'interval': interval,
            'startTime': int(start_date.timestamp() * 1000),
            'endTime': int(end_date.timestamp() * 1000),
            'limit': 1000
        }
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            return pd.DataFrame()
        
        data = response.json()
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data, columns=[
            'timestamp', 'Open', 'High', 'Low', 'Close', 'Volume',
            'close_time', 'quote_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')
        
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = pd.to_numeric(df[col])
        
        return df[['Open', 'High', 'Low', 'Close', 'Volume']]


class MultiSourceDataLoader:
    """
    Gestionnaire de sources multiples avec fallback automatique.
    
    Essaie chaque source dans l'ordre de priorité jusqu'à obtenir des données.
    """
    
    def __init__(self):
        self.sources: List[DataSourceBase] = []
        self._initialize_sources()
    
    def _initialize_sources(self):
        """Initialise toutes les sources de données disponibles."""
        # Ordre de priorité
        source_classes = [
            YahooFinanceSource,
            lambda: PolygonSource(),
            lambda: FinnhubSource(),
            BinanceSource,
            lambda: CCXTSource('binance'),
        ]
        
        for source_factory in source_classes:
            try:
                if callable(source_factory):
                    source = source_factory()
                else:
                    source = source_factory
                if source.is_available():
                    self.sources.append(source)
                    print(f"✅ Source activée: {source.name}")
                else:
                    print(f"⚠️ Source non disponible: {source.name if hasattr(source, 'name') else 'Unknown'}")
            except Exception as e:
                print(f"⚠️ Erreur initialisation source: {e}")
    
    def fetch_with_fallback(
        self,
        symbol: str,
        interval: str = "1h",
        start_date: datetime = None,
        end_date: datetime = None,
        required_rows: int = 100
    ) -> Tuple[pd.DataFrame, str]:
        """
        Récupère les données avec fallback automatique entre sources.
        
        Args:
            symbol: Symbole à récupérer
            interval: Intervalle des bougies
            start_date: Date de début
            end_date: Date de fin
            required_rows: Nombre minimum de lignes requis
        
        Returns:
            Tuple (DataFrame, nom de la source utilisée)
        """
        for source in self.sources:
            try:
                print(f"📡 Tentative avec {source.name}...")
                df = source.fetch_data(symbol, interval, start_date, end_date)
                
                if not df.empty and len(df) >= required_rows:
                    print(f"✅ Données obtenues depuis {source.name}: {len(df)} lignes")
                    return df, source.name
                elif not df.empty:
                    print(f"⚠️ {source.name}: seulement {len(df)} lignes (min: {required_rows})")
                else:
                    print(f"⚠️ {source.name}: aucune donnée")
                    
            except Exception as e:
                print(f"❌ {source.name} échec: {e}")
                continue
        
        print("❌ Toutes les sources ont échoué")
        return pd.DataFrame(), "none"
    
    def get_available_sources(self) -> List[str]:
        """Retourne la liste des sources disponibles."""
        return [s.name for s in self.sources]
    
    def get_fred_data(self) -> Optional[Dict[str, pd.DataFrame]]:
        """Récupère les données économiques FRED si disponible."""
        for source in self.sources:
            if isinstance(source, FREDSource):
                return source.fetch_economic_indicators()
        
        # Essayer d'initialiser FRED
        fred = FREDSource()
        if fred.is_available():
            return fred.fetch_economic_indicators()
        
        return None


if __name__ == "__main__":
    # Test des sources
    print("=" * 60)
    print("TEST DES SOURCES DE DONNÉES")
    print("=" * 60)

    loader = MultiSourceDataLoader()

    print(f"\nSources disponibles: {loader.get_available_sources()}")

    # Test avec plusieurs symboles (Forex, Métaux, Crypto)
    test_symbols = [
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X",  # Forex majeurs
        "GC=F", "SI=F",  # Métaux
        "BTC-USD", "ETH-USD"  # Crypto
    ]

    for symbol in test_symbols:
        print(f"\n{'='*60}")
        print(f"TEST {symbol}")
        print('='*60)

        df, source = loader.fetch_with_fallback(symbol, "1h")
        if not df.empty:
            print(f"\nSource: {source}")
            print(f"Shape: {df.shape}")
            print(f"Range: {df.index.min()} -> {df.index.max()}")
            print(df.tail(3))
        else:
            print("❌ Aucune donnée reçue")
