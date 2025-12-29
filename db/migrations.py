# Scripts de migration pour la base de données du système de trading quantique
# Gère les mises à jour du schéma de base de données

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import List, Dict, Any
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseMigrator:
    """
    Gestionnaire de migrations de base de données.
    Permet de gérer les évolutions du schéma de manière contrôlée.
    """

    def __init__(self, database_url: str):
        """
        Initialise le migrateur.

        Args:
            database_url: URL de connexion à la base de données
        """
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)

        # Table pour tracker les migrations
        self.migration_table = 'schema_migrations'

    def init_migration_table(self):
        """Crée la table de suivi des migrations si elle n'existe pas."""
        try:
            with self.engine.connect() as conn:
                # Vérifier si la table existe
                result = conn.execute(text("""
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name=:table_name
                """), {'table_name': self.migration_table})

                if not result.fetchone():
                    # Créer la table de migrations
                    conn.execute(text(f"""
                        CREATE TABLE {self.migration_table} (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            migration_name VARCHAR(255) NOT NULL UNIQUE,
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            checksum VARCHAR(64)
                        )
                    """))
                    conn.commit()
                    logger.info("Table de migrations créée")
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de l'initialisation de la table de migrations: {e}")
            raise

    def get_applied_migrations(self) -> List[str]:
        """Récupère la liste des migrations déjà appliquées."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT migration_name FROM {self.migration_table}
                    ORDER BY id ASC
                """))
                return [row[0] for row in result.fetchall()]
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de la récupération des migrations: {e}")
            return []

    def calculate_checksum(self, migration_sql: str) -> str:
        """Calcule le checksum d'une migration."""
        import hashlib
        return hashlib.sha256(migration_sql.encode('utf-8')).hexdigest()

    def apply_migration(self, migration_name: str, migration_sql: str) -> bool:
        """
        Applique une migration.

        Args:
            migration_name: Nom de la migration
            migration_sql: SQL de la migration

        Returns:
            True si appliquée avec succès, False sinon
        """
        try:
            checksum = self.calculate_checksum(migration_sql)

            with self.engine.connect() as conn:
                # Démarrer une transaction
                trans = conn.begin()

                try:
                    # Exécuter la migration
                    logger.info(f"Application de la migration: {migration_name}")
                    conn.execute(text(migration_sql))

                    # Enregistrer la migration
                    conn.execute(text(f"""
                        INSERT INTO {self.migration_table} (migration_name, checksum)
                        VALUES (:name, :checksum)
                    """), {'name': migration_name, 'checksum': checksum})

                    trans.commit()
                    logger.info(f"Migration {migration_name} appliquée avec succès")
                    return True

                except Exception as e:
                    trans.rollback()
                    logger.error(f"Erreur lors de l'application de la migration {migration_name}: {e}")
                    return False

        except SQLAlchemyError as e:
            logger.error(f"Erreur SQL lors de la migration {migration_name}: {e}")
            return False

    def rollback_migration(self, migration_name: str) -> bool:
        """
        Annule une migration (fonctionnalité basique).

        Args:
            migration_name: Nom de la migration à annuler

        Returns:
            True si annulée avec succès, False sinon
        """
        # Pour une implémentation complète, il faudrait des scripts de rollback
        # Pour l'instant, on se contente de marquer comme non appliquée
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"""
                    DELETE FROM {self.migration_table}
                    WHERE migration_name = :name
                """), {'name': migration_name})
                conn.commit()
                logger.info(f"Migration {migration_name} annulée")
                return True
        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de l'annulation de la migration {migration_name}: {e}")
            return False

    def run_migrations(self, migrations_dir: str = 'db/migrations'):
        """
        Exécute toutes les migrations en attente.

        Args:
            migrations_dir: Répertoire contenant les fichiers de migration
        """
        self.init_migration_table()
        applied = set(self.get_applied_migrations())

        # Lister les fichiers de migration
        if not os.path.exists(migrations_dir):
            logger.warning(f"Répertoire de migrations non trouvé: {migrations_dir}")
            return

        migration_files = [f for f in os.listdir(migrations_dir)
                          if f.endswith('.sql') and f not in applied]

        migration_files.sort()  # Trier par nom pour ordre d'application

        for migration_file in migration_files:
            migration_path = os.path.join(migrations_dir, migration_file)
            migration_name = migration_file

            try:
                with open(migration_path, 'r', encoding='utf-8') as f:
                    migration_sql = f.read()

                if self.apply_migration(migration_name, migration_sql):
                    logger.info(f"Migration {migration_name} réussie")
                else:
                    logger.error(f"Échec de la migration {migration_name}")
                    break

            except Exception as e:
                logger.error(f"Erreur lors du chargement de {migration_file}: {e}")
                break

# Migrations SQL prédéfinies

MIGRATION_001_INITIAL_SCHEMA = """
-- Migration 001: Schéma initial de la base de données

-- Table des symboles
CREATE TABLE IF NOT EXISTS symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100),
    asset_class VARCHAR(50),
    exchange VARCHAR(50),
    currency VARCHAR(10) DEFAULT 'USD',
    sector VARCHAR(50),
    industry VARCHAR(50),
    country VARCHAR(50),
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour symbols
CREATE INDEX IF NOT EXISTS idx_symbols_symbol ON symbols(symbol);
CREATE INDEX IF NOT EXISTS idx_symbols_asset_class ON symbols(asset_class);

-- Table des données de marché
CREATE TABLE IF NOT EXISTS market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    volume REAL NOT NULL,
    interval VARCHAR(10) DEFAULT '1d',
    source VARCHAR(50) DEFAULT 'yfinance',
    rsi REAL,
    macd REAL,
    macd_signal REAL,
    macd_hist REAL,
    bb_upper REAL,
    bb_middle REAL,
    bb_lower REAL,
    stoch_k REAL,
    stoch_d REAL,
    williams_r REAL,
    cci REAL,
    mfi REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id)
);

-- Index pour market_data
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_timestamp ON market_data(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_interval ON market_data(symbol_id, interval);

-- Table des signaux
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    signal_type VARCHAR(20) NOT NULL,
    strength REAL NOT NULL,
    confidence REAL NOT NULL,
    rsi_signal VARCHAR(20),
    macd_signal VARCHAR(20),
    bb_signal VARCHAR(20),
    stoch_signal VARCHAR(20),
    ml_prediction VARCHAR(20),
    ml_confidence REAL,
    intermarket_score REAL,
    sentiment_score REAL,
    strategy VARCHAR(100),
    timeframe VARCHAR(10) DEFAULT '1d',
    source VARCHAR(50) DEFAULT 'system',
    metadata TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id)
);

-- Index pour signals
CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp ON signals(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_type_strength ON signals(signal_type, strength);

-- Table des trades
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    signal_id INTEGER,
    side VARCHAR(10) NOT NULL,
    quantity REAL NOT NULL,
    price REAL NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    stop_loss REAL,
    take_profit REAL,
    risk_amount REAL,
    position_size REAL,
    exit_price REAL,
    exit_timestamp TIMESTAMP,
    pnl REAL,
    pnl_percent REAL,
    status VARCHAR(20) DEFAULT 'OPEN',
    strategy VARCHAR(100),
    broker VARCHAR(50),
    order_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id),
    FOREIGN KEY (signal_id) REFERENCES signals(id)
);

-- Index pour trades
CREATE INDEX IF NOT EXISTS idx_trades_symbol_timestamp ON trades(symbol_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);

-- Table des métriques de risque
CREATE TABLE IF NOT EXISTS risk_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    var_95_historical REAL,
    var_99_historical REAL,
    var_95_parametric REAL,
    var_99_parametric REAL,
    var_95_monte_carlo REAL,
    var_99_monte_carlo REAL,
    sharpe_ratio REAL,
    sortino_ratio REAL,
    max_drawdown REAL,
    volatility REAL,
    beta REAL,
    stress_test_results TEXT,
    calculation_method VARCHAR(50),
    confidence_level REAL DEFAULT 0.95,
    time_horizon INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour risk_metrics
CREATE INDEX IF NOT EXISTS idx_risk_metrics_portfolio_timestamp ON risk_metrics(portfolio_id, timestamp);

-- Table des prédictions ML
CREATE TABLE IF NOT EXISTS ml_predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    prediction VARCHAR(20) NOT NULL,
    confidence REAL NOT NULL,
    prob_buy REAL,
    prob_sell REAL,
    prob_hold REAL,
    model_version VARCHAR(50),
    feature_set VARCHAR(200),
    training_date TIMESTAMP,
    features TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id)
);

-- Index pour ml_predictions
CREATE INDEX IF NOT EXISTS idx_ml_predictions_symbol_timestamp ON ml_predictions(symbol_id, timestamp);

-- Table des données de sentiment
CREATE TABLE IF NOT EXISTS sentiment_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    news_sentiment REAL,
    social_sentiment REAL,
    overall_sentiment REAL,
    news_sources TEXT,
    social_sources TEXT,
    positive_mentions INTEGER DEFAULT 0,
    negative_mentions INTEGER DEFAULT 0,
    neutral_mentions INTEGER DEFAULT 0,
    total_mentions INTEGER DEFAULT 0,
    fear_greed_index REAL,
    put_call_ratio REAL,
    analysis_period VARCHAR(20) DEFAULT '24h',
    language VARCHAR(10) DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol_id) REFERENCES symbols(id)
);

-- Index pour sentiment_data
CREATE INDEX IF NOT EXISTS idx_sentiment_data_symbol_timestamp ON sentiment_data(symbol_id, timestamp);

-- Table de suivi des migrations
CREATE TABLE IF NOT EXISTS schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum VARCHAR(64)
);
"""

MIGRATION_002_ADD_PERFORMANCE_INDEXES = """
-- Migration 002: Ajout d'index de performance

-- Index composites pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_market_data_composite ON market_data(symbol_id, interval, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_signals_composite ON signals(symbol_id, signal_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_composite ON trades(symbol_id, status, timestamp DESC);

-- Index pour les analyses temporelles
CREATE INDEX IF NOT EXISTS idx_market_data_timestamp_only ON market_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_signals_timestamp_only ON signals(timestamp);
"""

def run_initial_migrations(database_url: str):
    """
    Exécute les migrations initiales.

    Args:
        database_url: URL de connexion à la base de données
    """
    migrator = DatabaseMigrator(database_url)

    # Migration 001: Schéma initial
    if '001_initial_schema.sql' not in migrator.get_applied_migrations():
        if migrator.apply_migration('001_initial_schema.sql', MIGRATION_001_INITIAL_SCHEMA):
            logger.info("Migration initiale appliquée")
        else:
            logger.error("Échec de la migration initiale")
            return False

    # Migration 002: Index de performance
    if '002_add_performance_indexes.sql' not in migrator.get_applied_migrations():
        if migrator.apply_migration('002_add_performance_indexes.sql', MIGRATION_002_ADD_PERFORMANCE_INDEXES):
            logger.info("Migration des index appliquée")
        else:
            logger.error("Échec de la migration des index")
            return False

    return True

if __name__ == '__main__':
    # Configuration de base pour les tests
    database_url = 'sqlite:///quantum_trading.db'

    if run_initial_migrations(database_url):
        print("Migrations exécutées avec succès")
    else:
        print("Erreur lors des migrations")