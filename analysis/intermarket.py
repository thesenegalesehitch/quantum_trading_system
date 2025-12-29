"""
Inter-Market Analysis for Quantum Trading System.

Analyzes correlations between different asset classes, identifies leading indicators,
and detects market spillover effects.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from data.data_sources import get_historical_data

class InterMarketAnalyzer:
    """
    Analyse les relations inter-marchés pour identifier les corrélations,
    les indicateurs leaders et les effets de spillover.
    """

    def __init__(self):
        self.correlation_window = 252  # 1 an de données de trading
        self.min_correlation_threshold = 0.3

    def calculate_correlations(self, symbols: List[str], window: int = None) -> pd.DataFrame:
        """
        Calcule la matrice de corrélations entre les symboles.

        Args:
            symbols: Liste des symboles à analyser
            window: Fenêtre de calcul (jours)

        Returns:
            DataFrame de corrélations
        """
        if window is None:
            window = self.correlation_window

        # Récupérer les données pour tous les symboles
        price_data = {}
        for symbol in symbols:
            try:
                data = get_historical_data(symbol, period=f"{window}d", interval="1d")
                if not data.empty:
                    price_data[symbol] = data['close']
            except Exception as e:
                print(f"Erreur récupération données {symbol}: {e}")
                continue

        if not price_data:
            return pd.DataFrame()

        # Créer un DataFrame avec toutes les séries
        combined_data = pd.DataFrame(price_data)

        # Calculer les rendements
        returns_data = combined_data.pct_change().dropna()

        # Calculer la matrice de corrélations
        correlation_matrix = returns_data.corr()

        return correlation_matrix

    def identify_leaders(self, correlations: pd.DataFrame) -> List[Dict]:
        """
        Identifie les indicateurs leaders basés sur les corrélations.

        Args:
            correlations: Matrice de corrélations

        Returns:
            Liste des indicateurs leaders avec leurs scores
        """
        leaders = []

        for symbol in correlations.columns:
            # Calculer la force de leadership
            # Un leader a des corrélations fortes avec les autres mais est moins influencé
            correlations_with_others = correlations[symbol].drop(symbol)

            # Score basé sur la moyenne des corrélations absolues
            leadership_score = correlations_with_others.abs().mean()

            # Nombre de corrélations fortes
            strong_correlations = (correlations_with_others.abs() > self.min_correlation_threshold).sum()

            leaders.append({
                'symbol': symbol,
                'leadership_score': leadership_score,
                'strong_correlations': int(strong_correlations),
                'avg_correlation': correlations_with_others.mean(),
                'max_correlation': correlations_with_others.max(),
                'min_correlation': correlations_with_others.min()
            })

        # Trier par score de leadership décroissant
        leaders.sort(key=lambda x: x['leadership_score'], reverse=True)

        return leaders

    def detect_spillover(self, symbol: str, all_symbols: List[str] = None) -> Dict:
        """
        Détecte les effets de spillover pour un symbole.

        Args:
            symbol: Symbole cible
            all_symbols: Liste de tous les symboles (optionnel)

        Returns:
            Dictionnaire avec les effets de spillover détectés
        """
        if all_symbols is None:
            all_symbols = config.symbols.ACTIVE_SYMBOLS

        # Calculer les corrélations
        correlations = self.calculate_correlations(all_symbols)

        if correlations.empty or symbol not in correlations.columns:
            return {'error': 'Données insuffisantes'}

        # Analyser les corrélations du symbole
        symbol_correlations = correlations[symbol].drop(symbol)

        # Identifier les spillovers forts
        strong_spillovers = symbol_correlations[symbol_correlations.abs() > self.min_correlation_threshold]

        # Calculer des métriques de spillover
        spillover_metrics = {
            'symbol': symbol,
            'total_correlations': len(symbol_correlations),
            'strong_spillovers': len(strong_spillovers),
            'avg_correlation': symbol_correlations.mean(),
            'max_spillover': symbol_correlations.abs().max(),
            'spillover_sources': strong_spillovers.to_dict(),
            'volatility_spillover': self._calculate_volatility_spillover(symbol, all_symbols),
            'timestamp': datetime.now().isoformat()
        }

        return spillover_metrics

    def _calculate_volatility_spillover(self, symbol: str, all_symbols: List[str]) -> Dict:
        """
        Calcule les effets de spillover sur la volatilité.
        """
        # Placeholder pour le calcul de spillover de volatilité
        # À implémenter avec GARCH ou autres modèles
        return {
            'volatility_transmission': 0.0,
            'note': 'Volatility spillover analysis - implementation in progress'
        }

    def get_market_network(self, symbols: List[str]) -> Dict:
        """
        Construit un réseau de connexions inter-marchés.

        Args:
            symbols: Liste des symboles

        Returns:
            Structure de réseau avec nœuds et arêtes
        """
        correlations = self.calculate_correlations(symbols)

        if correlations.empty:
            return {'error': 'Impossible de calculer le réseau'}

        # Créer la structure de réseau
        nodes = []
        edges = []

        for symbol in correlations.columns:
            # Informations sur le nœud
            degree = (correlations[symbol].abs() > self.min_correlation_threshold).sum() - 1  # Exclure soi-même
            strength = correlations[symbol].drop(symbol).abs().mean()

            nodes.append({
                'id': symbol,
                'degree': int(degree),
                'strength': strength,
                'asset_class': self._get_asset_class(symbol)
            })

            # Arêtes (corrélations fortes)
            for other_symbol in correlations.columns:
                if symbol != other_symbol:
                    corr_value = correlations.loc[symbol, other_symbol]
                    if abs(corr_value) > self.min_correlation_threshold:
                        edges.append({
                            'source': symbol,
                            'target': other_symbol,
                            'weight': abs(corr_value),
                            'type': 'positive' if corr_value > 0 else 'negative'
                        })

        return {
            'nodes': nodes,
            'edges': edges,
            'metadata': {
                'total_nodes': len(nodes),
                'total_edges': len(edges),
                'correlation_threshold': self.min_correlation_threshold,
                'timestamp': datetime.now().isoformat()
            }
        }

    def _get_asset_class(self, symbol: str) -> str:
        """Détermine la classe d'actif d'un symbole."""
        if symbol.endswith('=X'):
            return 'forex'
        elif symbol in ['GC=F', 'SI=F', 'PL=F']:
            return 'commodity'
        elif symbol.endswith('-USD') or symbol.endswith('-USDT'):
            return 'crypto'
        elif symbol.startswith('^'):
            return 'index'
        else:
            return 'equity'

    def analyze_market_regime(self, symbols: List[str]) -> Dict:
        """
        Analyse le régime de marché actuel basé sur les corrélations inter-marchés.

        Args:
            symbols: Liste des symboles

        Returns:
            Analyse du régime de marché
        """
        correlations = self.calculate_correlations(symbols)

        if correlations.empty:
            return {'error': 'Données insuffisantes pour l'analyse de régime'}

        # Calculer des métriques de régime
        avg_correlation = correlations.values[np.triu_indices_from(correlations.values, k=1)].mean()
        correlation_volatility = correlations.values[np.triu_indices_from(correlations.values, k=1)].std()

        # Déterminer le régime
        if avg_correlation > 0.7:
            regime = 'high_correlation'
            description = 'Marché en régime de forte corrélation - risque systémique élevé'
        elif avg_correlation > 0.4:
            regime = 'moderate_correlation'
            description = 'Marché en régime de corrélation modérée'
        else:
            regime = 'low_correlation'
            description = 'Marché en régime de faible corrélation - opportunités de diversification'

        return {
            'regime': regime,
            'description': description,
            'avg_correlation': avg_correlation,
            'correlation_volatility': correlation_volatility,
            'symbols_analyzed': len(symbols),
            'timestamp': datetime.now().isoformat()
        }