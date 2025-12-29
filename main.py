"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        QUANTUM TRADING SYSTEM                                 ║
║                                                                              ║
║  Author: Alexandre Albert Ndour                                               ║
║  Copyright (c) 2026 Alexandre Albert Ndour. All Rights Reserved.             ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Quantum Trading System - Point d'entrée principal.

Système quantitatif de trading haute précision pour Forex, Crypto et Indices.
Combine analyse statistique avancée, indicateurs techniques et Machine Learning.

Conception et développement: Alexandre Albert Ndour
Date de création: Décembre 2026

Usage:
    python main.py --mode backtest          # Backtesting sur historique
    python main.py --mode analyze           # Analyse en temps réel
    python main.py --mode train             # Entraîner le modèle ML
    python main.py --mode signal            # Générer un signal
    python main.py --mode correlation       # Analyse de corrélation

⚠️ AVERTISSEMENT: Le trading comporte des risques. Utilisez ce système à vos propres risques.
"""

# Quantum Trading System - Conceived and Developed by Alexandre Albert Ndour - 2026
# Signature: QVROLVFUUy1BTEVYQU5EUkUtQUxCRVJULU5ET1VSLTI0

import argparse
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from tqdm import tqdm

# Configuration du path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config.settings import config
from data.downloader import DataDownloader
from data.kalman_filter import KalmanFilter
from data.feature_engine import FeatureEngine
from core.cointegration import CointegrationAnalyzer
from core.hurst import HurstExponent
from core.zscore import BollingerZScore
from analysis.multi_tf import MultiTimeframeAnalyzer
from analysis.smc import SmartMoneyConceptsAnalyzer
from analysis.ichimoku import IchimokuAnalyzer
from ml.features import MLFeaturesPreparer
from ml.model import SignalClassifier
from ml.trainer import ModelTrainer, EnsembleTrainer
from risk.manager import RiskManager
from risk.circuit_breaker import CircuitBreaker
from risk.calendar import EconomicCalendar
from reporting.interface import TradingInterface
from reporting.scan_coordinator import ScanCoordinator
from backtest.engine import BacktestEngine


class QuantumTradingSystem:
    """
    Système principal orchestrant tous les modules.
    """
    
    def __init__(self):
        print("🚀 Initialisation du Quantum Trading System...")

        try:
            # Composants data
            print("Initialisation des composants data...")
            self.downloader = DataDownloader()
            print("✅ DataDownloader initialisé")
            self.kalman = KalmanFilter()
            print("✅ KalmanFilter initialisé")
            self.feature_engine = FeatureEngine()
            print("✅ FeatureEngine initialisé")

            # Composants analyse
            print("Initialisation des composants analyse...")
            self.coint_analyzer = CointegrationAnalyzer()
            print("✅ CointegrationAnalyzer initialisé")
            self.hurst_calc = HurstExponent()
            print("✅ HurstExponent initialisé")
            self.zscore_calc = BollingerZScore()
            print("✅ BollingerZScore initialisé")
            self.mtf_analyzer = MultiTimeframeAnalyzer()
            print("✅ MultiTimeframeAnalyzer initialisé")
            self.smc_analyzer = SmartMoneyConceptsAnalyzer()
            print("✅ SmartMoneyConceptsAnalyzer initialisé")
            self.ichimoku = IchimokuAnalyzer()
            print("✅ IchimokuAnalyzer initialisé")

            # Composants ML
            print("Initialisation des composants ML...")
            self.ml_preparer = MLFeaturesPreparer()
            print("✅ MLFeaturesPreparer initialisé")
            self.ml_classifier = SignalClassifier()
            print("✅ SignalClassifier initialisé")
            self.ml_trainer = ModelTrainer()
            print("✅ ModelTrainer initialisé")

            # Composants risque
            print("Initialisation des composants risque...")
            self.risk_manager = RiskManager()
            print("✅ RiskManager initialisé")
            self.circuit_breaker = CircuitBreaker()
            print("✅ CircuitBreaker initialisé")
            self.calendar = EconomicCalendar()
            print("✅ EconomicCalendar initialisé")

            # Interface
            print("Initialisation de l'interface...")
            self.interface = TradingInterface()
            print("✅ TradingInterface initialisé")
            self.scan_coordinator = ScanCoordinator(self)
            print("✅ ScanCoordinator initialisé")
            self.backtest_engine = BacktestEngine()
            print("✅ BacktestEngine initialisé")

            # Données chargées
            self.data = {}

            print("✅ Système initialisé avec succès")

        except Exception as e:
            print(f"❌ Erreur lors de l'initialisation: {e}")
            raise
    
    def load_data(self, symbol: str, force_download: bool = False) -> pd.DataFrame:
        """Charge les données pour un symbole."""
        print(f"📊 Chargement des données pour {symbol}...")

        try:
            df = self.downloader.get_data(symbol, interval="1h", force_download=force_download)
            print(f"Données téléchargées: {len(df)} lignes")
        except Exception as e:
            print(f"❌ Erreur lors du téléchargement des données pour {symbol}: {e}")
            return pd.DataFrame()

        if df.empty:
            print(f"❌ Aucune donnée reçue pour {symbol}")
            return df

        try:
            # Appliquer le filtre de Kalman
            print("Application du filtre de Kalman...")
            df = self.kalman.filter_dataframe(df, columns=['Close'])
            print("✅ Filtre de Kalman appliqué")
        except Exception as e:
            print(f"❌ Erreur filtre de Kalman: {e}")
            return pd.DataFrame()

        try:
            # Créer les features
            print("Création des features...")
            df = self.feature_engine.create_all_features(df)
            print("✅ Features créées")
        except Exception as e:
            print(f"❌ Erreur création features: {e}")
            return pd.DataFrame()

        self.data[symbol] = df
        print(f"✅ {len(df)} bougies chargées et traitées pour {symbol}")

        return df
    
    def analyze_symbol(self, symbol: str) -> dict:
        """Analyse complète d'un symbole."""
        if symbol not in self.data:
            df = self.load_data(symbol)
        else:
            df = self.data[symbol]

        if df.empty:
            return {"error": "Pas de données"}

        analysis = {}

        # 1. Hurst Exponent
        hurst = self.hurst_calc.calculate(df['Close'])
        analysis['hurst'] = {
            'value': hurst,
            'regime': self.hurst_calc.get_regime(hurst)
        }

        # 2. Z-Score
        zscore_data = self.zscore_calc.get_current_status(df['Close'])
        analysis['zscore'] = zscore_data

        # 3. Ichimoku
        ichi_signal = self.ichimoku.get_signal(df)
        analysis['ichimoku'] = ichi_signal

        # 4. SMC
        smc_analysis = self.smc_analyzer.analyze(df)
        analysis['smc'] = smc_analysis['current_analysis']

        # 5. Signal combiné
        signal, confidence = self._combine_signals(analysis)
        analysis['combined_signal'] = signal
        analysis['confidence'] = confidence

        # Générer setup de trade si signal valide
        trade_setup = None
        if signal in ['BUY', 'SELL']:
            trade_setup = self.risk_manager.create_trade_setup(
                df,
                symbol,
                signal
            )
            trade_setup = {
                'entry_price': trade_setup.entry_price,
                'stop_loss': trade_setup.stop_loss,
                'take_profits': trade_setup.take_profits
            }

        # Afficher le rapport amélioré
        self.interface.print_analysis(symbol, analysis, trade_setup)
        
        return analysis
    
    def _combine_signals(self, analysis: dict) -> tuple:
        """Combine tous les signaux en un signal final avec haute précision."""
        signals = []
        weights = []
        confirmations = 0

        # Ichimoku (poids élevé - indicateur principal)
        ichi_signal = analysis.get('ichimoku', {}).get('signal')
        if ichi_signal == 'BUY':
            signals.append(1)
            weights.append(3)  # Augmenté
            confirmations += 1
        elif ichi_signal == 'SELL':
            signals.append(-1)
            weights.append(3)
            confirmations += 1

        # Z-Score (poids moyen - confirmation)
        zscore = analysis.get('zscore', {}).get('zscore', 0)
        if zscore < -2.5:  # Seuil plus strict
            signals.append(1)
            weights.append(2)
            confirmations += 1
        elif zscore > 2.5:
            signals.append(-1)
            weights.append(2)
            confirmations += 1

        # SMC (poids moyen - smart money)
        smc_signal = analysis.get('smc', {}).get('signal', '')
        if smc_signal == 'BUY':
            signals.append(1)
            weights.append(2)
            confirmations += 1
        elif smc_signal == 'SELL':
            signals.append(-1)
            weights.append(2)
            confirmations += 1

        # Hurst pour filtrer le régime
        hurst_regime = analysis.get('hurst', {}).get('regime', '')
        regime_filter = 1.0 if hurst_regime == 'TRENDING' else 0.7  # Réduire confiance en mean-revert

        if not signals:
            return "WAIT", 30.0

        # Score pondéré
        weighted_signal = sum(s * w for s, w in zip(signals, weights)) / sum(weights)
        base_confidence = abs(weighted_signal) * 40 + 50  # Ajusté

        # Bonus pour multiple confirmations
        confirmation_bonus = min(confirmations - 1, 2) * 10  # +10% par confirmation supplémentaire
        confidence = min(base_confidence + confirmation_bonus, 95) * regime_filter

        # Seuils plus stricts pour haute fiabilité
        if weighted_signal > 0.5 and confirmations >= 2:  # Au moins 2 confirmations
            return "BUY", confidence
        elif weighted_signal < -0.5 and confirmations >= 2:
            return "SELL", confidence
        else:
            return "WAIT", max(confidence - 20, 20)  # Réduire confiance pour WAIT
    
    def generate_signal(self, symbol: str) -> dict:
        """Génère un signal de trading complet."""
        # Vérifications de sécurité
        can_trade = self.circuit_breaker.can_trade()
        if not can_trade['allowed']:
            return {"signal": "BLOCKED", "reason": can_trade['reason']}
        
        calendar_check = self.calendar.can_trade()
        if not calendar_check['allowed']:
            return {"signal": "BLOCKED", "reason": calendar_check['reason']}
        
        # Analyse
        analysis = self.analyze_symbol(symbol)
        
        if 'error' in analysis:
            return analysis
        
        # Trade setup si signal valide
        trade_setup = None
        signal = analysis['combined_signal']
        
        if signal in ['BUY', 'SELL']:
            trade_setup = self.risk_manager.create_trade_setup(
                self.data[symbol],
                symbol,
                signal
            )
            trade_setup = {
                'entry_price': trade_setup.entry_price,
                'stop_loss': trade_setup.stop_loss,
                'take_profits': trade_setup.take_profits
            }
        
        # Afficher le rapport
        self.interface.print_signal(
            symbol=symbol,
            analysis=analysis,
            trade_setup=trade_setup
        )
        
        return {
            'signal': signal,
            'confidence': analysis['confidence'],
            'analysis': analysis,
            'trade_setup': trade_setup
        }

    def scan_all_symbols(self) -> dict:
        """Analyse tous les symboles actifs et génère un rapport complet."""
        results = self.scan_coordinator.scan_all_symbols()
        self.interface.print_scan_report(results)
        return results

    def run_backtest(self, symbol: str) -> dict:
        """Exécute un backtest sur le symbole."""
        print(f"\n🔬 Backtest de {symbol}...")
        
        if symbol not in self.data:
            self.load_data(symbol)
        
        df = self.data[symbol]
        
        if df.empty:
            return {"error": "Pas de données"}
        
        # Stratégie simple pour le test
        # Acheter quand RSI < 30, vendre quand RSI > 70
        if 'rsi' not in df.columns:
            df['rsi'] = self._calculate_rsi(df['Close'])
        
        entries = (df['rsi'] < 30) & (df['rsi'].shift(1) >= 30)
        exits = (df['rsi'] > 70) & (df['rsi'].shift(1) <= 70)
        
        results = self.backtest_engine.run(df, entries.fillna(False), exits.fillna(False))
        self.backtest_engine.print_report()
        
        return results
    
    def _calculate_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        """Calcule le RSI."""
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / (loss + 1e-10)
        return 100 - (100 / (1 + rs))
    
    def train_model(self, symbol: str, use_ensemble: bool = True) -> dict:
        """Entraîne le modèle ML avec ensemble avancé."""
        print(f"\n🧠 Entraînement du modèle ML sur {symbol}...")
        print(f"Mode: {'Ensemble avancé' if use_ensemble else 'XGBoost simple'}")

        if symbol not in self.data:
            self.load_data(symbol)

        df = self.data[symbol]

        if len(df) < 1000:
            return {"error": "Données insuffisantes (min 1000 bougies)"}

        # Utiliser l'ensemble avancé par défaut
        results = self.ml_trainer.train_with_cross_validation(df, use_ensemble=use_ensemble)

        print(f"\n=== Résultats de l'entraînement ({results.get('model_type', 'unknown')}) ===")
        print(f"Accuracy moyenne CV: {results['cv_summary']['mean_accuracy']:.3f}")
        print(f"AUC moyenne: {results['cv_summary']['mean_auc']:.4f}")

        if 'walk_forward' in results:
            wf = results['walk_forward']
            print(f"Validation walk-forward: AUC = {wf['mean_auc']:.4f} ± {wf['std_auc']:.4f}")

        # Stats de trading
        if use_ensemble and self.ml_trainer.ensemble_trainer:
            # Pour l'ensemble, utiliser les stats de l'ensemble
            try:
                X, y = self.ml_trainer.preparer.prepare_train_data(df)
                probas = self.ml_trainer.ensemble_trainer.predict_proba(X)
                stats = self._calculate_trading_stats(probas, y)
            except:
                stats = {"error": "Impossible de calculer les stats de trading"}
        else:
            stats = self.ml_trainer.get_trading_statistics(df)

        print("\n=== Statistiques de Trading ===")
        if isinstance(stats, dict):
            for k, v in stats.items():
                if isinstance(v, float):
                    print(f"  {k}: {v:.2f}")
                else:
                    print(f"  {k}: {v}")

        return results

    def _calculate_trading_stats(self, probas: np.ndarray, y_true: pd.Series) -> dict:
        """Calcule les statistiques de trading pour l'ensemble."""
        min_proba = config.ml.MIN_PROBABILITY_THRESHOLD

        # Trades au-dessus du seuil
        above_threshold = probas >= min_proba
        trades_taken = above_threshold.sum()

        if trades_taken == 0:
            return {
                "trades_taken": 0,
                "message": "Aucun trade au-dessus du seuil"
            }

        # Win rate sur les trades pris
        wins = (y_true[above_threshold] == 1).sum()
        win_rate = wins / trades_taken

        # Comparaison avec random
        overall_win_rate = y_true.mean()

        return {
            "trades_taken": int(trades_taken),
            "total_opportunities": len(y_true),
            "selectivity": trades_taken / len(y_true) * 100,
            "wins": int(wins),
            "losses": int(trades_taken - wins),
            "win_rate": win_rate * 100,
            "baseline_win_rate": overall_win_rate * 100,
            "edge": (win_rate - overall_win_rate) * 100,
            "threshold_used": min_proba * 100
        }
    
    def analyze_correlation(self):
        """Analyse la corrélation entre les paires de devises actives."""
        print("\n🔗 Analyse de co-intégration entre paires de devises...")

        symbols = config.symbols.ACTIVE_SYMBOLS

        for symbol in symbols:
            if symbol not in self.data:
                self.load_data(symbol)

        if len(self.data) < 2:
            return {"error": "Besoin d'au moins deux symboles"}

        # Analyser toutes les paires possibles
        results = {}
        symbol_list = list(self.data.keys())

        for i in range(len(symbol_list)):
            for j in range(i+1, len(symbol_list)):
                s1_name, s2_name = symbol_list[i], symbol_list[j]
                s1, s2 = self.data[s1_name]['Close'], self.data[s2_name]['Close']

                print(f"\n🔍 Test {s1_name} vs {s2_name}...")

                # Test de co-intégration
                result = self.coint_analyzer.test_cointegration(s1, s2)

                cointegrated = result.get('is_cointegrated', False)
                pvalue = result.get('pvalue', 'N/A')

                print(f"  Co-intégration: {'OUI' if cointegrated else 'NON'}")
                print(f"  P-value: {pvalue}")

                if cointegrated:
                    arb = self.coint_analyzer.detect_arbitrage_opportunity(s1, s2)
                    print(f"  Signal d'arbitrage: {arb['signal']}")
                    if arb['signal'] != 'NEUTRAL':
                        print(f"  Action: {arb['action']}")

                results[f"{s1_name}_{s2_name}"] = result

        return results


def select_symbol_interactive() -> str:
    """Sélection interactive d'un symbole."""
    symbols = config.symbols.ACTIVE_SYMBOLS

    print("\n" + "="*60)
    print("           SÉLECTION DU SYMBOLE À ANALYSER")
    print("="*60)
    print("Symboles disponibles :")
    print()

    for i, symbol in enumerate(symbols, 1):
        display_name = config.symbols.DISPLAY_NAMES.get(symbol, symbol)
        print(f"  {i:2d}. {display_name} ({symbol})")

    print()
    print("  0. Mode scan (analyser tous les symboles)")
    print()

    while True:
        try:
            choice = input("Choisissez un numéro (1-11) ou 0 pour scan: ").strip()

            if choice == "0":
                return "SCAN_MODE"

            choice_num = int(choice)
            if 1 <= choice_num <= len(symbols):
                selected = symbols[choice_num - 1]
                display_name = config.symbols.DISPLAY_NAMES.get(selected, selected)
                print(f"\n✅ Sélection: {display_name} ({selected})")
                return selected
            else:
                print("❌ Numéro invalide. Réessayez.")

        except ValueError:
            print("❌ Entrée invalide. Entrez un numéro.")
        except KeyboardInterrupt:
            print("\n\nAu revoir !")
            sys.exit(0)


def main():
    """Point d'entrée principal."""
    parser = argparse.ArgumentParser(description="Quantum Trading System")
    parser.add_argument(
        "--mode",
        choices=["backtest", "analyze", "train", "correlation", "signal", "scan"],
        default="analyze",
        help="Mode d'exécution"
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Symbole à analyser (optionnel - menu interactif si non spécifié)"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Forcer le téléchargement des données"
    )
    parser.add_argument(
        "--simple-model",
        action="store_true",
        help="Utiliser le modèle XGBoost simple au lieu de l'ensemble avancé"
    )

    args = parser.parse_args()

    # Sélection du symbole si non spécifié
    if args.symbol is None and args.mode != "scan":
        selected = select_symbol_interactive()
        if selected == "SCAN_MODE":
            args.mode = "scan"
        else:
            args.symbol = selected

    # Initialiser le système
    system = QuantumTradingSystem()

    # Pour le mode scan, pas besoin de symbole spécifique
    if args.mode != "scan":
        # Charger les données
        system.load_data(args.symbol, force_download=args.download)
    
    # Exécuter selon le mode
    if args.mode == "backtest":
        system.run_backtest(args.symbol)
    
    elif args.mode == "analyze":
        system.analyze_symbol(args.symbol)
    
    elif args.mode == "train":
        use_ensemble = not args.simple_model
        system.train_model(args.symbol, use_ensemble=use_ensemble)
    
    elif args.mode == "correlation":
        system.analyze_correlation()
    
    elif args.mode == "signal":
        system.generate_signal(args.symbol)

    elif args.mode == "scan":
        system.scan_all_symbols()

    print("\n✅ Exécution terminée")


if __name__ == "__main__":
    main()
