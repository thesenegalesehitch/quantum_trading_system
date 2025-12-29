# Modèles de base de données pour le système de trading quantique
# Utilise SQLAlchemy pour la gestion des données

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func
from typing import Optional
import json

Base = declarative_base()

class Symbol(Base):
    """
    Données maîtresses des symboles.
    Stocke les informations de base sur les actifs tradés.
    """
    __tablename__ = 'symbols'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20), unique=True, nullable=False, index=True)
    name = Column(String(100))
    asset_class = Column(String(50))  # 'equity', 'crypto', 'forex', 'commodity'
    exchange = Column(String(50))
    currency = Column(String(10), default='USD')
    sector = Column(String(50))
    industry = Column(String(50))
    country = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relations
    market_data = relationship("MarketData", back_populates="symbol_rel")
    signals = relationship("Signal", back_populates="symbol_rel")
    trades = relationship("Trade", back_populates="symbol_rel")
    ml_predictions = relationship("MLPrediction", back_populates="symbol_rel")
    sentiment_data = relationship("SentimentData", back_populates="symbol_rel")

    def __repr__(self):
        return f"<Symbol(symbol='{self.symbol}', name='{self.name}')>"

class MarketData(Base):
    """
    Données de marché OHLCV avec timestamps.
    Stocke les données historiques et temps réel.
    """
    __tablename__ = 'market_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    interval = Column(String(10), default='1d')  # '1m', '5m', '1h', '1d', etc.
    source = Column(String(50), default='yfinance')  # 'yfinance', 'polygon', 'finnhub', etc.

    # Données techniques calculées
    rsi = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_hist = Column(Float)
    bb_upper = Column(Float)
    bb_middle = Column(Float)
    bb_lower = Column(Float)
    stoch_k = Column(Float)
    stoch_d = Column(Float)
    williams_r = Column(Float)
    cci = Column(Float)
    mfi = Column(Float)

    created_at = Column(DateTime, default=func.now())

    # Relations
    symbol_rel = relationship("Symbol", back_populates="market_data")

    __table_args__ = (
        Index('idx_market_data_symbol_timestamp', 'symbol_id', 'timestamp'),
        Index('idx_market_data_symbol_interval', 'symbol_id', 'interval'),
    )

    def __repr__(self):
        return f"<MarketData(symbol_id={self.symbol_id}, timestamp={self.timestamp}, close={self.close_price})>"

class Signal(Base):
    """
    Signaux de trading générés.
    Stocke les signaux avec scores de confiance.
    """
    __tablename__ = 'signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    signal_type = Column(String(20), nullable=False)  # 'BUY', 'SELL', 'HOLD'
    strength = Column(Float, nullable=False)  # 0.0 à 1.0
    confidence = Column(Float, nullable=False)  # 0.0 à 1.0

    # Indicateurs techniques
    rsi_signal = Column(String(20))
    macd_signal = Column(String(20))
    bb_signal = Column(String(20))
    stoch_signal = Column(String(20))

    # Prédiction ML
    ml_prediction = Column(String(20))
    ml_confidence = Column(Float)

    # Analyse inter-marchés
    intermarket_score = Column(Float)

    # Sentiment
    sentiment_score = Column(Float)

    # Métadonnées
    strategy = Column(String(100))
    timeframe = Column(String(10), default='1d')
    source = Column(String(50), default='system')

    # JSON pour données supplémentaires
    metadata = Column(Text)  # JSON string

    created_at = Column(DateTime, default=func.now())

    # Relations
    symbol_rel = relationship("Symbol", back_populates="signals")

    __table_args__ = (
        Index('idx_signals_symbol_timestamp', 'symbol_id', 'timestamp'),
        Index('idx_signals_type_strength', 'signal_type', 'strength'),
    )

    def __repr__(self):
        return f"<Signal(symbol_id={self.symbol_id}, type='{self.signal_type}', strength={self.strength})>"

class Trade(Base):
    """
    Historique des trades exécutés.
    Stocke les transactions avec résultats.
    """
    __tablename__ = 'trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    signal_id = Column(Integer, ForeignKey('signals.id'), nullable=True)

    # Détails du trade
    side = Column(String(10), nullable=False)  # 'BUY', 'SELL'
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Gestion des risques
    stop_loss = Column(Float)
    take_profit = Column(Float)
    risk_amount = Column(Float)
    position_size = Column(Float)

    # Résultats
    exit_price = Column(Float)
    exit_timestamp = Column(DateTime)
    pnl = Column(Float)  # Profit/Loss
    pnl_percent = Column(Float)
    status = Column(String(20), default='OPEN')  # 'OPEN', 'CLOSED', 'CANCELLED'

    # Métadonnées
    strategy = Column(String(100))
    broker = Column(String(50))
    order_id = Column(String(100))

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relations
    symbol_rel = relationship("Symbol", back_populates="trades")

    __table_args__ = (
        Index('idx_trades_symbol_timestamp', 'symbol_id', 'timestamp'),
        Index('idx_trades_status', 'status'),
    )

    def __repr__(self):
        return f"<Trade(symbol_id={self.symbol_id}, side='{self.side}', pnl={self.pnl})>"

class RiskMetrics(Base):
    """
    Métriques de risque calculées.
    Stocke les calculs VaR, stress tests, etc.
    """
    __tablename__ = 'risk_metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    portfolio_id = Column(String(100), nullable=False, index=True)  # Identifiant du portefeuille
    timestamp = Column(DateTime, nullable=False, index=True)

    # VaR calculations
    var_95_historical = Column(Float)
    var_99_historical = Column(Float)
    var_95_parametric = Column(Float)
    var_99_parametric = Column(Float)
    var_95_monte_carlo = Column(Float)
    var_99_monte_carlo = Column(Float)

    # Autres métriques
    sharpe_ratio = Column(Float)
    sortino_ratio = Column(Float)
    max_drawdown = Column(Float)
    volatility = Column(Float)
    beta = Column(Float)

    # Stress test results
    stress_test_results = Column(Text)  # JSON string avec scénarios

    # Métadonnées
    calculation_method = Column(String(50))
    confidence_level = Column(Float, default=0.95)
    time_horizon = Column(Integer, default=1)  # jours

    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        Index('idx_risk_metrics_portfolio_timestamp', 'portfolio_id', 'timestamp'),
    )

    def __repr__(self):
        return f"<RiskMetrics(portfolio='{self.portfolio_id}', var_95={self.var_95_historical})>"

class MLPrediction(Base):
    """
    Prédictions des modèles d'apprentissage automatique.
    Stocke les sorties ML avec métadonnées.
    """
    __tablename__ = 'ml_predictions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Prédiction
    prediction = Column(String(20), nullable=False)  # 'BUY', 'SELL', 'HOLD'
    confidence = Column(Float, nullable=False)

    # Probabilités détaillées
    prob_buy = Column(Float)
    prob_sell = Column(Float)
    prob_hold = Column(Float)

    # Métadonnées du modèle
    model_version = Column(String(50))
    feature_set = Column(String(200))
    training_date = Column(DateTime)

    # Caractéristiques utilisées
    features = Column(Text)  # JSON string des caractéristiques

    created_at = Column(DateTime, default=func.now())

    # Relations
    symbol_rel = relationship("Symbol", back_populates="ml_predictions")

    __table_args__ = (
        Index('idx_ml_predictions_symbol_timestamp', 'symbol_id', 'timestamp'),
    )

    def __repr__(self):
        return f"<MLPrediction(symbol_id={self.symbol_id}, prediction='{self.prediction}', confidence={self.confidence})>"

class SentimentData(Base):
    """
    Données d'analyse de sentiment.
    Stocke les scores de sentiment des news et médias sociaux.
    """
    __tablename__ = 'sentiment_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol_id = Column(Integer, ForeignKey('symbols.id'), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Scores de sentiment
    news_sentiment = Column(Float)  # -1 à 1
    social_sentiment = Column(Float)  # -1 à 1
    overall_sentiment = Column(Float)  # -1 à 1

    # Sources
    news_sources = Column(Text)  # JSON array des sources
    social_sources = Column(Text)  # JSON array des plateformes

    # Métriques détaillées
    positive_mentions = Column(Integer, default=0)
    negative_mentions = Column(Integer, default=0)
    neutral_mentions = Column(Integer, default=0)
    total_mentions = Column(Integer, default=0)

    # Fear & Greed Index
    fear_greed_index = Column(Float)

    # Put/Call ratio
    put_call_ratio = Column(Float)

    # Métadonnées
    analysis_period = Column(String(20), default='24h')  # '1h', '24h', '7d'
    language = Column(String(10), default='en')

    created_at = Column(DateTime, default=func.now())

    # Relations
    symbol_rel = relationship("Symbol", back_populates="sentiment_data")

    __table_args__ = (
        Index('idx_sentiment_data_symbol_timestamp', 'symbol_id', 'timestamp'),
    )

    def __repr__(self):
        return f"<SentimentData(symbol_id={self.symbol_id}, overall_sentiment={self.overall_sentiment})>"

# Fonctions utilitaires pour la base de données
def create_database_engine(database_url: str):
    """
    Crée un moteur de base de données SQLAlchemy.

    Args:
        database_url: URL de connexion à la base de données

    Returns:
        Engine SQLAlchemy configuré
    """
    return create_engine(database_url, echo=False)

def create_session_factory(engine):
    """
    Crée une factory de sessions SQLAlchemy.

    Args:
        engine: Moteur SQLAlchemy

    Returns:
        Session factory
    """
    return sessionmaker(bind=engine)

def init_database(engine):
    """
    Initialise la base de données en créant toutes les tables.

    Args:
        engine: Moteur SQLAlchemy
    """
    Base.metadata.create_all(engine)

def get_or_create_symbol(session, symbol: str, **kwargs) -> Symbol:
    """
    Récupère ou crée un symbole dans la base de données.

    Args:
        session: Session SQLAlchemy
        symbol: Symbole à rechercher/créer
        **kwargs: Attributs supplémentaires pour la création

    Returns:
        Instance Symbol
    """
    symbol_obj = session.query(Symbol).filter_by(symbol=symbol).first()
    if not symbol_obj:
        symbol_obj = Symbol(symbol=symbol, **kwargs)
        session.add(symbol_obj)
        session.commit()
    return symbol_obj