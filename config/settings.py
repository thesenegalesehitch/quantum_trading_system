"""
Quantum Trading System - Configuration centralisée
==================================================

Configuration centralisée du système de trading quantitatif.
Tous les paramètres modifiables sont regroupés ici pour faciliter l'optimisation.

Author: Alexandre Albert Ndour
Copyright (c) 2026 Alexandre Albert Ndour. All Rights Reserved.
Created: December 2026

AMÉLIORATION v2.0: Ajout de toutes les paires majeures, mineures et crypto populaires.
"""

# Signature: 416c6578616e647265_416c62657274_4e646f7572

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import timedelta
import os


@dataclass
class SymbolConfig:
    """Configuration des symboles tradés - COMPLÈTE avec toutes les paires majeures."""
    
    # ===== FOREX - PAIRES MAJEURES (Les plus tradées) =====
    EURUSD: str = "EURUSD=X"    # Euro / Dollar US
    GBPUSD: str = "GBPUSD=X"    # Livre Sterling / Dollar US
    USDJPY: str = "USDJPY=X"    # Dollar US / Yen Japonais
    USDCHF: str = "USDCHF=X"    # Dollar US / Franc Suisse
    AUDUSD: str = "AUDUSD=X"    # Dollar Australien / Dollar US
    USDCAD: str = "USDCAD=X"    # Dollar US / Dollar Canadien
    NZDUSD: str = "NZDUSD=X"    # Dollar Néo-Zélandais / Dollar US
    
    # ===== FOREX - PAIRES MINEURES (Cross) =====
    EURGBP: str = "EURGBP=X"    # Euro / Livre Sterling
    EURJPY: str = "EURJPY=X"    # Euro / Yen
    GBPJPY: str = "GBPJPY=X"    # Livre / Yen
    AUDJPY: str = "AUDJPY=X"    # Dollar AUD / Yen
    EURAUD: str = "EURAUD=X"    # Euro / Dollar AUD
    EURCHF: str = "EURCHF=X"    # Euro / Franc Suisse
    GBPCHF: str = "GBPCHF=X"    # Livre / Franc Suisse
    CADJPY: str = "CADJPY=X"    # Dollar CAD / Yen
    NZDJPY: str = "NZDJPY=X"    # Dollar NZD / Yen
    AUDCAD: str = "AUDCAD=X"    # Dollar AUD / Dollar CAD
    AUDNZD: str = "AUDNZD=X"    # Dollar AUD / Dollar NZD
    CADCHF: str = "CADCHF=X"    # Dollar CAD / Franc Suisse
    CHFJPY: str = "CHFJPY=X"    # Franc Suisse / Yen
    
    # ===== MÉTAUX PRÉCIEUX =====
    GOLD: str = "GC=F"          # Or (XAU/USD futures)
    SILVER: str = "SI=F"        # Argent (XAG/USD futures)
    PLATINUM: str = "PL=F"      # Platine
    
    # ===== CRYPTO - TOP 10 PAR VOLUME =====
    BTCUSDT: str = "BTC-USD"    # Bitcoin
    ETHUSDT: str = "ETH-USD"    # Ethereum
    BNBUSDT: str = "BNB-USD"    # Binance Coin
    XRPUSDT: str = "XRP-USD"    # Ripple
    ADAUSDT: str = "ADA-USD"    # Cardano
    SOLUSDT: str = "SOL-USD"    # Solana
    DOTUSDT: str = "DOT-USD"    # Polkadot
    DOGEUSDT: str = "DOGE-USD"  # Dogecoin
    MATICUSDT: str = "MATIC-USD"  # Polygon
    AVAXUSDT: str = "AVAX-USD"  # Avalanche
    
    # ===== INDICES MAJEURS =====
    SP500: str = "^GSPC"        # S&P 500
    NASDAQ: str = "^IXIC"       # NASDAQ Composite
    DOW: str = "^DJI"           # Dow Jones
    DAX: str = "^GDAXI"         # DAX 40
    CAC40: str = "^FCHI"        # CAC 40
    NIKKEI: str = "^N225"       # Nikkei 225
    FTSE: str = "^FTSE"         # FTSE 100
    VIX: str = "^VIX"           # Indice de volatilité
    
    # ===== MATIÈRES PREMIÈRES =====
    CRUDE_OIL: str = "CL=F"     # Pétrole WTI
    BRENT: str = "BZ=F"         # Pétrole Brent
    NATURAL_GAS: str = "NG=F"   # Gaz Naturel
    
    # ===== LISTE DES SYMBOLES ACTIFS PAR DÉFAUT =====
    ACTIVE_SYMBOLS: List[str] = field(default_factory=lambda: [
        # Forex Majeurs (7 paires)
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X", 
        "AUDUSD=X", "USDCAD=X", "NZDUSD=X",
        # Métaux
        "GC=F", "SI=F",
        # Crypto principales
        "BTC-USD", "ETH-USD"
    ])
    
    # ===== PAIRES FOREX SEULEMENT =====
    FOREX_PAIRS: List[str] = field(default_factory=lambda: [
        # Majeurs
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X", 
        "AUDUSD=X", "USDCAD=X", "NZDUSD=X",
        # Mineurs populaires
        "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X",
        "EURAUD=X", "EURCHF=X", "GBPCHF=X", "CADJPY=X"
    ])
    
    # ===== CRYPTO SEULEMENT =====
    CRYPTO_PAIRS: List[str] = field(default_factory=lambda: [
        "BTC-USD", "ETH-USD", "BNB-USD", "XRP-USD", "ADA-USD",
        "SOL-USD", "DOT-USD", "DOGE-USD", "MATIC-USD", "AVAX-USD"
    ])
    
    # ===== NOMS AFFICHABLES =====
    DISPLAY_NAMES: Dict[str, str] = field(default_factory=lambda: {
        # Forex Majeurs
        "EURUSD=X": "EUR/USD",
        "GBPUSD=X": "GBP/USD",
        "USDJPY=X": "USD/JPY",
        "USDCHF=X": "USD/CHF",
        "AUDUSD=X": "AUD/USD",
        "USDCAD=X": "USD/CAD",
        "NZDUSD=X": "NZD/USD",
        # Forex Mineurs
        "EURGBP=X": "EUR/GBP",
        "EURJPY=X": "EUR/JPY",
        "GBPJPY=X": "GBP/JPY",
        "AUDJPY=X": "AUD/JPY",
        "EURAUD=X": "EUR/AUD",
        "EURCHF=X": "EUR/CHF",
        "GBPCHF=X": "GBP/CHF",
        "CADJPY=X": "CAD/JPY",
        "NZDJPY=X": "NZD/JPY",
        "AUDCAD=X": "AUD/CAD",
        "AUDNZD=X": "AUD/NZD",
        "CADCHF=X": "CAD/CHF",
        "CHFJPY=X": "CHF/JPY",
        # Métaux
        "GC=F": "XAU/USD (Or)",
        "SI=F": "XAG/USD (Argent)",
        "PL=F": "Platine",
        # Crypto
        "BTC-USD": "BTC/USD",
        "ETH-USD": "ETH/USD",
        "BNB-USD": "BNB/USD",
        "XRP-USD": "XRP/USD",
        "ADA-USD": "ADA/USD",
        "SOL-USD": "SOL/USD",
        "DOT-USD": "DOT/USD",
        "DOGE-USD": "DOGE/USD",
        "MATIC-USD": "MATIC/USD",
        "AVAX-USD": "AVAX/USD",
        # Indices
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ",
        "^DJI": "Dow Jones",
        "^GDAXI": "DAX 40",
        "^FCHI": "CAC 40",
        "^N225": "Nikkei 225",
        "^FTSE": "FTSE 100",
        "^VIX": "VIX",
        # Commodités
        "CL=F": "WTI Crude Oil",
        "BZ=F": "Brent",
        "NG=F": "Natural Gas"
    })
    
    # ===== SESSIONS DE TRADING =====
    TRADING_SESSIONS: Dict[str, Dict] = field(default_factory=lambda: {
        "SYDNEY": {"open": "22:00", "close": "07:00", "timezone": "UTC"},
        "TOKYO": {"open": "00:00", "close": "09:00", "timezone": "UTC"},
        "LONDON": {"open": "08:00", "close": "17:00", "timezone": "UTC"},
        "NEW_YORK": {"open": "13:00", "close": "22:00", "timezone": "UTC"},
    })
    
    # ===== PROPRIÉTÉS DES PAIRES =====
    PAIR_PROPERTIES: Dict[str, Dict] = field(default_factory=lambda: {
        # Forex - Pips et taille de lot
        "EURUSD=X": {"pip_size": 0.0001, "pip_value": 10, "min_lot": 0.01},
        "GBPUSD=X": {"pip_size": 0.0001, "pip_value": 10, "min_lot": 0.01},
        "USDJPY=X": {"pip_size": 0.01, "pip_value": 9.1, "min_lot": 0.01},
        "USDCHF=X": {"pip_size": 0.0001, "pip_value": 10.3, "min_lot": 0.01},
        "AUDUSD=X": {"pip_size": 0.0001, "pip_value": 10, "min_lot": 0.01},
        "USDCAD=X": {"pip_size": 0.0001, "pip_value": 7.4, "min_lot": 0.01},
        "NZDUSD=X": {"pip_size": 0.0001, "pip_value": 10, "min_lot": 0.01},
        # Or et Argent
        "GC=F": {"pip_size": 0.1, "pip_value": 10, "min_lot": 0.01},
        "SI=F": {"pip_size": 0.01, "pip_value": 50, "min_lot": 0.01},
        # Crypto
        "BTC-USD": {"pip_size": 1, "pip_value": 1, "min_lot": 0.001},
        "ETH-USD": {"pip_size": 0.1, "pip_value": 1, "min_lot": 0.01},
    })


@dataclass
class TimeframeConfig:
    """Configuration des unités de temps."""
    # Timeframes utilisés pour l'analyse multi-TF
    TIMEFRAMES: List[str] = field(default_factory=lambda: ["15m", "1h", "4h", "1d"])
    
    # Timeframe principal pour les signaux
    PRIMARY_TIMEFRAME: str = "1h"
    
    # Mapping pour le resampling pandas
    RESAMPLE_MAP: Dict[str, str] = field(default_factory=lambda: {
        "1m": "1T",
        "5m": "5T",
        "15m": "15T",
        "30m": "30T",
        "1h": "1H", 
        "4h": "4H",
        "1d": "1D",
        "1wk": "1W"
    })
    
    # Poids de chaque timeframe pour la convergence
    TIMEFRAME_WEIGHTS: Dict[str, float] = field(default_factory=lambda: {
        "15m": 0.15,
        "1h": 0.30,
        "4h": 0.35,
        "1d": 0.20
    })


@dataclass
class DataConfig:
    """Configuration du moteur de données."""
    # Période historique (en années)
    HISTORICAL_YEARS: int = 3  # Augmenté à 3 ans

    # Sources de données avec clés API
    PRIMARY_DATA_SOURCE: str = "yfinance"
    
    # Clés API (depuis variables d'environnement)
    ALPHA_VANTAGE_API_KEY: str = field(default_factory=lambda: os.getenv('ALPHA_VANTAGE_API_KEY', ''))
    POLYGON_API_KEY: str = field(default_factory=lambda: os.getenv('POLYGON_API_KEY', ''))
    FINNHUB_API_KEY: str = field(default_factory=lambda: os.getenv('FINNHUB_API_KEY', ''))
    FRED_API_KEY: str = field(default_factory=lambda: os.getenv('FRED_API_KEY', ''))
    NEWSAPI_KEY: str = field(default_factory=lambda: os.getenv('NEWSAPI_KEY', ''))
    
    # Telegram/Discord
    TELEGRAM_BOT_TOKEN: str = field(default_factory=lambda: os.getenv('TELEGRAM_BOT_TOKEN', ''))
    TELEGRAM_CHAT_ID: str = field(default_factory=lambda: os.getenv('TELEGRAM_CHAT_ID', ''))
    DISCORD_WEBHOOK_URL: str = field(default_factory=lambda: os.getenv('DISCORD_WEBHOOK_URL', ''))

    # Paramètres du filtre de Kalman
    KALMAN_PROCESS_NOISE: float = 0.01
    KALMAN_MEASUREMENT_NOISE: float = 0.1

    # Paramètres de feature engineering
    VOLATILITY_WINDOW: int = 14
    VOLUME_NORMALIZATION_WINDOW: int = 20

    # Cycles temporels à extraire
    EXTRACT_HOUR: bool = True
    EXTRACT_DAY_OF_WEEK: bool = True
    EXTRACT_MONTH: bool = True
    
    # Cache local
    CACHE_ENABLED: bool = True
    CACHE_EXPIRY_HOURS: int = 24


@dataclass
class StatisticalConfig:
    """Configuration de la couche statistique."""
    # Co-intégration
    COINTEGRATION_LOOKBACK: int = 252  # 1 an trading days
    COINTEGRATION_PVALUE_THRESHOLD: float = 0.05
    
    # Exposant de Hurst
    HURST_WINDOW: int = 100
    HURST_TREND_THRESHOLD: float = 0.55
    HURST_MEAN_REVERT_THRESHOLD: float = 0.45
    
    # Z-Score de Bollinger
    ZSCORE_WINDOW: int = 20
    ZSCORE_EXTREME_THRESHOLD: float = 3.0
    ZSCORE_SIGNAL_THRESHOLD: float = 2.0


@dataclass 
class TechnicalConfig:
    """Configuration de l'analyse technique."""
    # Multi-timeframe convergence
    REQUIRED_TF_CONFIRMATION: int = 3
    
    # Smart Money Concepts
    ORDER_BLOCK_LOOKBACK: int = 50
    FVG_MIN_GAP_PERCENT: float = 0.1
    
    # Ichimoku Kumo
    ICHIMOKU_TENKAN: int = 9
    ICHIMOKU_KIJUN: int = 26
    ICHIMOKU_SENKOU_B: int = 52
    ICHIMOKU_DISPLACEMENT: int = 26
    
    # Divergences
    DIVERGENCE_LOOKBACK: int = 50
    DIVERGENCE_MIN_STRENGTH: float = 0.3
    
    # Wyckoff
    WYCKOFF_LOOKBACK: int = 100
    WYCKOFF_RANGE_THRESHOLD: float = 0.03


@dataclass
class MLConfig:
    """Configuration du modèle Machine Learning."""
    # Seuils de probabilité
    MIN_PROBABILITY_THRESHOLD: float = 0.75  # 75%
    STRONG_SIGNAL_THRESHOLD: float = 0.85   # 85%
    
    # Paramètres XGBoost
    XGBOOST_PARAMS: Dict = field(default_factory=lambda: {
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 100,
        "objective": "binary:logistic",
        "eval_metric": "auc",
        "use_label_encoder": False,
        "random_state": 42
    })
    
    # Paramètres Ensemble
    USE_ENSEMBLE: bool = True
    ENSEMBLE_MODELS: List[str] = field(default_factory=lambda: [
        "xgboost", "lightgbm", "catboost", "random_forest"
    ])
    ENSEMBLE_VOTING: str = "soft"  # "hard" or "soft"
    CALIBRATE_PROBABILITIES: bool = True
    
    # Validation croisée
    CV_SPLITS: int = 5
    TEST_SIZE_RATIO: float = 0.2
    USE_WALK_FORWARD: bool = True
    
    # Features à utiliser
    FEATURE_COLUMNS: List[str] = field(default_factory=lambda: [
        "zscore", "hurst", "cointegration_deviation",
        "rsi", "macd_signal", "atr_normalized",
        "kumo_position", "order_block_proximity",
        "multi_tf_score", "volume_ratio",
        "hour_sin", "hour_cos", "day_of_week",
        "divergence_score", "wyckoff_phase"
    ])
    
    # Optimisation
    USE_OPTUNA: bool = True
    OPTUNA_TRIALS: int = 50

    # Ensemble de modèles
    USE_ENSEMBLE: bool = True  # Utiliser l'ensemble avancé par défaut
    ENSEMBLE_MODELS: List[str] = field(default_factory=lambda: [
        "xgboost", "lightgbm", "random_forest"  # CatBoost désactivé pour compatibilité sklearn
    ])
    ENSEMBLE_OPTIMIZE: bool = True  # Optimiser les hyperparamètres
    ENSEMBLE_TRIALS_PER_MODEL: int = 20  # Essais Optuna par modèle


@dataclass
class RiskConfig:
    """Configuration de la gestion du risque."""
    # Risque par trade (en % du capital)
    RISK_PER_TRADE: float = 0.01  # 1%
    
    # Drawdown maximum avant arrêt
    MAX_DRAWDOWN: float = 0.05  # 5%
    
    # Pertes consécutives maximum
    MAX_CONSECUTIVE_LOSSES: int = 3
    
    # Perte journalière maximum
    MAX_DAILY_LOSS: float = 0.02  # 2%
    
    # ATR pour le Stop-Loss
    ATR_PERIOD: int = 14
    ATR_MULTIPLIER: float = 1.5
    
    # Take-Profit (multiples du risque)
    TP_LEVELS: List[Dict] = field(default_factory=lambda: [
        {"ratio": 1.0, "size_percent": 50},
        {"ratio": 2.0, "size_percent": 30},
        {"ratio": 3.0, "size_percent": 20},
    ])
    
    # Blackout calendrier économique
    ECONOMIC_BLACKOUT_BEFORE: timedelta = timedelta(minutes=30)
    ECONOMIC_BLACKOUT_AFTER: timedelta = timedelta(minutes=30)
    
    # Capital initial pour le backtesting
    INITIAL_CAPITAL: float = 10000.0
    
    # Kelly Criterion
    USE_KELLY: bool = True
    KELLY_FRACTION: float = 0.5  # Demi-Kelly
    
    # VaR
    VAR_CONFIDENCE: float = 0.95
    VAR_METHOD: str = "monte_carlo"  # "historical", "parametric", "monte_carlo"
    
    # Portfolio
    MAX_CORRELATED_POSITIONS: int = 3
    MAX_CORRELATION_THRESHOLD: float = 0.7


@dataclass
class BacktestConfig:
    """Configuration du backtesting."""
    # Monte Carlo
    MONTE_CARLO_SIMULATIONS: int = 10000
    
    # Slippage et spread
    SLIPPAGE_PIPS: float = 0.5
    SPREAD_PIPS: float = 1.0
    
    # Commissions
    COMMISSION_PER_LOT: float = 7.0  # USD par lot standard
    
    # Walk-forward
    WF_IN_SAMPLE_RATIO: float = 0.7
    WF_OPTIMIZATION_WINDOWS: int = 5


@dataclass
class AlertConfig:
    """Configuration des alertes."""
    # Canaux activés
    ENABLE_TELEGRAM: bool = True
    ENABLE_DISCORD: bool = True
    ENABLE_EMAIL: bool = False
    ENABLE_WEBHOOK: bool = False
    
    # Email SMTP
    SMTP_SERVER: str = field(default_factory=lambda: os.getenv('SMTP_SERVER', 'smtp.gmail.com'))
    SMTP_PORT: int = 587
    SMTP_USERNAME: str = field(default_factory=lambda: os.getenv('SMTP_USERNAME', ''))
    SMTP_PASSWORD: str = field(default_factory=lambda: os.getenv('SMTP_PASSWORD', ''))
    EMAIL_FROM: str = field(default_factory=lambda: os.getenv('EMAIL_FROM', ''))
    EMAIL_TO: str = field(default_factory=lambda: os.getenv('EMAIL_TO', ''))
    
    # Webhook personnalisé
    CUSTOM_WEBHOOK_URL: str = field(default_factory=lambda: os.getenv('CUSTOM_WEBHOOK_URL', ''))
    
    # Types d'alertes
    ALERT_ON_SIGNALS: bool = True
    ALERT_ON_RISK_EVENTS: bool = True
    ALERT_DAILY_SUMMARY: bool = True
    DAILY_SUMMARY_TIME: str = "18:00"


@dataclass
class SystemConfig:
    """Configuration système globale."""
    # Mode de fonctionnement
    MODE: str = "backtest"  # "backtest", "paper", "live"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/quantum_trading.log"
    LOG_FORMAT: str = "json"  # "json" or "text"
    LOG_ROTATION: bool = True
    LOG_MAX_SIZE_MB: int = 10
    LOG_BACKUP_COUNT: int = 5
    
    # Chemins de données
    DATA_DIR: str = "data/cache"
    MODEL_DIR: str = "ml/models"
    REPORTS_DIR: str = "reports"
    LOGS_DIR: str = "logs"


# Instance globale de configuration
class Config:
    """Conteneur global pour toutes les configurations."""
    symbols = SymbolConfig()
    timeframes = TimeframeConfig()
    data = DataConfig()
    statistical = StatisticalConfig()
    technical = TechnicalConfig()
    ml = MLConfig()
    risk = RiskConfig()
    backtest = BacktestConfig()
    alerts = AlertConfig()
    system = SystemConfig()


# Accès rapide
config = Config()
