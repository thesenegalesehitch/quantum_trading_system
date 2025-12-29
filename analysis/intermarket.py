# Analyse inter-marchés pour le système de trading quantique
# Étudie les corrélations et les relations entre différents marchés

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging
from scipy import stats
from statsmodels.tsa.stattools import grangercausalitytests
import networkx as nx
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class InterMarketAnalyzer:
    """
    Analyse les relations inter-marchés, les corrélations croisées et les effets de débordement.
    Identifie les indicateurs leaders et les patterns de rotation sectorielle.
    """

    def __init__(self):
        """
        Initialise l'analyseur inter-marchés.
        """
        self.correlation_cache = {}
        self.leading_indicators = {}
        self.market_graph = None

    def calculate_correlations(self, symbols: List[str], data_dict: Optional[Dict[str, pd.DataFrame]] = None,
                              period: str = "252d") -> pd.DataFrame:
        """
        Calcule la matrice de corrélation entre les symboles.

        Args:
            symbols: Liste des symboles à analyser
            data_dict: Dictionnaire {symbole: DataFrame} avec données OHLCV
            period: Période d'analyse (252d = 1 an)

        Returns:
            DataFrame de la matrice de corrélation
        """
        try:
            if not data_dict:
                logger.warning("Aucune donnée fournie pour le calcul des corrélations")
                return pd.DataFrame()

            # Extraire les prix de clôture pour chaque symbole
            price_data = {}
            for symbol in symbols:
                if symbol in data_dict and not data_dict[symbol].empty:
                    # Utiliser les prix de clôture
                    prices = data_dict[symbol]['Close'].pct_change().dropna()
                    price_data[symbol] = prices

            if not price_data:
                logger.error("Aucune donnée de prix valide trouvée")
                return pd.DataFrame()

            # Créer un DataFrame avec tous les rendements
            returns_df = pd.DataFrame(price_data)

            # Calculer la matrice de corrélation
            correlation_matrix = returns_df.corr()

            # Mettre en cache
            cache_key = f"corr_{'_'.join(sorted(symbols))}_{period}"
            self.correlation_cache[cache_key] = correlation_matrix

            logger.info(f"Matrice de corrélation calculée pour {len(symbols)} symboles")
            return correlation_matrix

        except Exception as e:
            logger.error(f"Erreur lors du calcul des corrélations: {e}")
            return pd.DataFrame()

    def identify_leaders(self, correlation_matrix: pd.DataFrame,
                        threshold: float = 0.7) -> List[str]:
        """
        Identifie les indicateurs leaders basés sur les corrélations.

        Args:
            correlation_matrix: Matrice de corrélation
            threshold: Seuil de corrélation pour considérer un leader

        Returns:
            Liste des symboles leaders
        """
        try:
            if correlation_matrix.empty:
                return []

            leaders = []

            # Pour chaque symbole, compter combien d'autres sont fortement corrélés
            for symbol in correlation_matrix.columns:
                correlated_count = 0
                for other_symbol in correlation_matrix.columns:
                    if symbol != other_symbol:
                        corr = abs(correlation_matrix.loc[symbol, other_symbol])
                        if corr >= threshold:
                            correlated_count += 1

                # Si le symbole influence beaucoup d'autres, c'est un leader
                if correlated_count >= len(correlation_matrix.columns) // 3:
                    leaders.append(symbol)

            # Trier par nombre de corrélations
            leaders.sort(key=lambda x: sum(abs(correlation_matrix.loc[x, y])
                                         for y in correlation_matrix.columns if x != y),
                        reverse=True)

            logger.info(f"Indicateurs leaders identifiés: {leaders}")
            return leaders

        except Exception as e:
            logger.error(f"Erreur lors de l'identification des leaders: {e}")
            return []

    def detect_spillover(self, symbol: str, data_dict: Dict[str, pd.DataFrame],
                        window: int = 20) -> Dict:
        """
        Détecte les effets de débordement d'un symbole vers les autres marchés.

        Args:
            symbol: Symbole source du débordement
            data_dict: Dictionnaire des données de marché
            window: Fenêtre de calcul en jours

        Returns:
            Dictionnaire avec les effets de débordement détectés
        """
        try:
            spillover_effects = {
                'symbol': symbol,
                'spillover_targets': [],
                'granger_causality': {},
                'correlation_changes': {},
                'timestamp': datetime.utcnow().isoformat()
            }

            if symbol not in data_dict or data_dict[symbol].empty:
                return spillover_effects

            source_returns = data_dict[symbol]['Close'].pct_change().dropna()

            # Analyser chaque autre symbole
            for target_symbol, target_data in data_dict.items():
                if target_symbol == symbol or target_data.empty:
                    continue

                target_returns = target_data['Close'].pct_change().dropna()

                # Aligner les données sur les dates communes
                common_dates = source_returns.index.intersection(target_returns.index)
                if len(common_dates) < window * 2:
                    continue

                source_aligned = source_returns.loc[common_dates]
                target_aligned = target_returns.loc[common_dates]

                # Test de causalité de Granger
                try:
                    # Préparer les données pour Granger
                    combined_data = pd.DataFrame({
                        'source': source_aligned,
                        'target': target_aligned
                    }).dropna()

                    if len(combined_data) >= window * 2:
                        # Test si source cause target
                        granger_result = grangercausalitytests(combined_data, maxlag=5, verbose=False)

                        # Prendre le résultat du lag 1 (plus conservateur)
                        if 1 in granger_result:
                            p_value = granger_result[1][0]['ssr_ftest'][1]
                            if p_value < 0.05:  # Seuil de signification
                                spillover_effects['granger_causality'][target_symbol] = {
                                    'p_value': p_value,
                                    'significant': True
                                }
                                spillover_effects['spillover_targets'].append(target_symbol)

                except Exception as e:
                    logger.warning(f"Erreur test Granger {symbol} -> {target_symbol}: {e}")

                # Analyser les changements de corrélation
                try:
                    # Corrélation récente vs historique
                    recent_corr = source_aligned.tail(window).corr(target_aligned.tail(window))
                    historical_corr = source_aligned.head(len(source_aligned) - window).corr(
                        target_aligned.head(len(target_aligned) - window))

                    if abs(recent_corr - historical_corr) > 0.2:  # Changement significatif
                        spillover_effects['correlation_changes'][target_symbol] = {
                            'recent': recent_corr,
                            'historical': historical_corr,
                            'change': recent_corr - historical_corr
                        }

                except Exception as e:
                    logger.warning(f"Erreur analyse corrélation {symbol} -> {target_symbol}: {e}")

            logger.info(f"Analyse de débordement terminée pour {symbol}: {len(spillover_effects['spillover_targets'])} cibles détectées")
            return spillover_effects

        except Exception as e:
            logger.error(f"Erreur lors de la détection du débordement pour {symbol}: {e}")
            return {'symbol': symbol, 'error': str(e)}

    def analyze_sector_rotation(self, sector_data: Dict[str, pd.DataFrame],
                               lookback_periods: List[int] = [20, 60, 120]) -> Dict:
        """
        Analyse la rotation sectorielle basée sur les performances relatives.

        Args:
            sector_data: Dictionnaire {secteur: DataFrame}
            lookback_periods: Périodes de lookback en jours

        Returns:
            Analyse de rotation sectorielle
        """
        try:
            rotation_analysis = {
                'periods': {},
                'leading_sectors': {},
                'sector_momentum': {},
                'timestamp': datetime.utcnow().isoformat()
            }

            for period in lookback_periods:
                period_key = f"{period}d"
                rotation_analysis['periods'][period_key] = {}

                sector_performance = {}

                # Calculer la performance de chaque secteur
                for sector, data in sector_data.items():
                    if data.empty or len(data) < period:
                        continue

                    try:
                        # Performance sur la période
                        start_price = data['Close'].iloc[-period]
                        end_price = data['Close'].iloc[-1]
                        performance = (end_price - start_price) / start_price

                        # Volatilité
                        returns = data['Close'].pct_change().tail(period).dropna()
                        volatility = returns.std() * np.sqrt(252)  # Annualisée

                        sector_performance[sector] = {
                            'performance': performance,
                            'volatility': volatility,
                            'sharpe_ratio': performance / volatility if volatility > 0 else 0
                        }

                    except Exception as e:
                        logger.warning(f"Erreur calcul performance secteur {sector}: {e}")

                # Identifier les secteurs leaders
                if sector_performance:
                    sorted_sectors = sorted(sector_performance.items(),
                                          key=lambda x: x[1]['performance'], reverse=True)

                    rotation_analysis['periods'][period_key] = {
                        'sector_performance': sector_performance,
                        'top_performers': [s[0] for s in sorted_sectors[:3]],
                        'worst_performers': [s[0] for s in sorted_sectors[-3:]]
                    }

            logger.info("Analyse de rotation sectorielle terminée")
            return rotation_analysis

        except Exception as e:
            logger.error(f"Erreur lors de l'analyse de rotation sectorielle: {e}")
            return {'error': str(e)}

    def build_market_network(self, correlation_matrix: pd.DataFrame,
                           threshold: float = 0.5) -> nx.Graph:
        """
        Construit un graphe de réseau de marché basé sur les corrélations.

        Args:
            correlation_matrix: Matrice de corrélation
            threshold: Seuil minimum de corrélation pour créer un lien

        Returns:
            Graphe NetworkX des connexions marché
        """
        try:
            # Créer le graphe
            G = nx.Graph()

            # Ajouter les nœuds
            for symbol in correlation_matrix.columns:
                G.add_node(symbol)

            # Ajouter les arêtes pour les corrélations fortes
            for i, symbol1 in enumerate(correlation_matrix.columns):
                for j, symbol2 in enumerate(correlation_matrix.columns):
                    if i < j:  # Éviter les doublons
                        corr = abs(correlation_matrix.loc[symbol1, symbol2])
                        if corr >= threshold:
                            G.add_edge(symbol1, symbol2, weight=corr)

            self.market_graph = G

            # Calculer les métriques de réseau
            centrality = nx.degree_centrality(G)
            betweenness = nx.betweenness_centrality(G)

            logger.info(f"Réseau de marché construit: {len(G.nodes)} nœuds, {len(G.edges)} arêtes")
            return G

        except Exception as e:
            logger.error(f"Erreur lors de la construction du réseau: {e}")
            return nx.Graph()

    def get_network_metrics(self) -> Dict:
        """
        Calcule les métriques du réseau de marché.

        Returns:
            Dictionnaire avec les métriques de réseau
        """
        if not self.market_graph:
            return {}

        try:
            metrics = {
                'nodes': len(self.market_graph.nodes),
                'edges': len(self.market_graph.edges),
                'density': nx.density(self.market_graph),
                'average_clustering': nx.average_clustering(self.market_graph),
                'degree_centrality': dict(nx.degree_centrality(self.market_graph)),
                'betweenness_centrality': dict(nx.betweenness_centrality(self.market_graph)),
                'timestamp': datetime.utcnow().isoformat()
            }

            # Identifier les hubs (nœuds les plus centraux)
            if metrics['degree_centrality']:
                sorted_centrality = sorted(metrics['degree_centrality'].items(),
                                         key=lambda x: x[1], reverse=True)
                metrics['market_hubs'] = [node for node, _ in sorted_centrality[:5]]

            return metrics

        except Exception as e:
            logger.error(f"Erreur lors du calcul des métriques réseau: {e}")
            return {}

    def detect_market_regime(self, symbols: List[str], data_dict: Dict[str, pd.DataFrame],
                           window: int = 60) -> Dict:
        """
        Détecte le régime de marché actuel basé sur les corrélations.

        Args:
            symbols: Liste des symboles
            data_dict: Données de marché
            window: Fenêtre d'analyse en jours

        Returns:
            Analyse du régime de marché
        """
        try:
            regime_analysis = {
                'current_regime': 'unknown',
                'correlation_level': 0.0,
                'volatility_regime': 'normal',
                'market_state': {},
                'timestamp': datetime.utcnow().isoformat()
            }

            # Calculer la corrélation moyenne récente
            correlations = self.calculate_correlations(symbols, data_dict)
            if not correlations.empty:
                avg_correlation = correlations.values[np.triu_indices_from(correlations.values, k=1)].mean()
                regime_analysis['correlation_level'] = avg_correlation

                # Déterminer le régime basé sur la corrélation
                if avg_correlation > 0.7:
                    regime_analysis['current_regime'] = 'high_correlation'
                elif avg_correlation > 0.4:
                    regime_analysis['current_regime'] = 'moderate_correlation'
                else:
                    regime_analysis['current_regime'] = 'low_correlation'

            # Analyser la volatilité
            volatilities = []
            for symbol, data in data_dict.items():
                if not data.empty and len(data) >= window:
                    returns = data['Close'].pct_change().tail(window).dropna()
                    vol = returns.std() * np.sqrt(252)
                    volatilities.append(vol)

            if volatilities:
                avg_volatility = np.mean(volatilities)
                if avg_volatility > 0.4:
                    regime_analysis['volatility_regime'] = 'high_volatility'
                elif avg_volatility > 0.2:
                    regime_analysis['volatility_regime'] = 'moderate_volatility'
                else:
                    regime_analysis['volatility_regime'] = 'low_volatility'

            # État général du marché
            regime_analysis['market_state'] = {
                'correlation_regime': regime_analysis['current_regime'],
                'volatility_regime': regime_analysis['volatility_regime'],
                'overall_regime': f"{regime_analysis['current_regime']}_{regime_analysis['volatility_regime']}"
            }

            logger.info(f"Régime de marché détecté: {regime_analysis['market_state']['overall_regime']}")
            return regime_analysis

        except Exception as e:
            logger.error(f"Erreur lors de la détection du régime de marché: {e}")
            return {'error': str(e)}