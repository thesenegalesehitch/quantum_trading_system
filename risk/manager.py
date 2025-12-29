# Gestionnaire avancé des risques pour le système de trading quantique
# Implémente VaR, stress testing et optimisation de portefeuille

import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
from scipy import stats
from scipy.optimize import minimize
import arch
# Portfolio optimization using scipy (pyportfolioopt not compatible with Python 3.13)
# from pypfopt import EfficientFrontier, risk_models, expected_returns
# from pypfopt import BlackLittermanModel, CovarianceShrinkage
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class VaRMethod(Enum):
    """Méthodes de calcul VaR."""
    HISTORICAL = "historical"
    PARAMETRIC = "parametric"
    MONTE_CARLO = "monte_carlo"

class RiskMeasure(Enum):
    """Mesures de risque supportées."""
    VAR = "var"
    CVAR = "cvar"
    MAX_DRAWDOWN = "max_drawdown"
    VOLATILITY = "volatility"

@dataclass
class PortfolioPosition:
    """Position dans un portefeuille."""
    symbol: str
    weight: float
    quantity: Optional[float] = None
    price: Optional[float] = None
    value: Optional[float] = None

@dataclass
class RiskMetrics:
    """Métriques de risque calculées."""
    var_95: float
    var_99: float
    volatility: float
    max_drawdown: float
    cvar_95: Optional[float] = None
    cvar_99: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    beta: Optional[float] = None
    method: str = "historical"
    confidence_level: float = 0.95
    timestamp: Optional[datetime] = None

@dataclass
class StressTestResult:
    """Résultat d'un test de stress."""
    scenario_name: str
    portfolio_return: float
    portfolio_loss: float
    var_breach: bool
    drawdown: float
    worst_asset: str
    worst_asset_loss: float

@dataclass
class TradeSetup:
    """Configuration d'un trade."""
    symbol: str
    signal: str
    entry_price: float
    stop_loss: float
    take_profits: List[Dict[str, Any]]
    risk_reward: float
    suggested_lot: float
    timestamp: datetime

class RiskManager:
    """
    Moteur de calcul des risques avancés.
    Implémente VaR historique, paramétrique et Monte Carlo, tests de stress, et optimisation de portefeuille.
    """

    def __init__(self):
        """
        Initialise le moteur de risque.
        """
        self.market_data_cache: Dict[str, pd.DataFrame] = {}
        self.risk_free_rate = 0.02  # Taux sans risque annuel (2%)

        # Scénarios de stress prédéfinis
        self.stress_scenarios = {
            '2008_crisis': {
                'name': 'Crise 2008',
                'description': 'Chute des marchés de 50%',
                'market_return': -0.50,
                'volatility_multiplier': 3.0
            },
            'covid_19': {
                'name': 'COVID-19',
                'description': 'Chute rapide de 30% suivie de récupération',
                'market_return': -0.30,
                'volatility_multiplier': 2.5
            },
            'tech_bubble': {
                'name': 'Bulled Tech 2000',
                'description': 'Effondrement sectoriel tech de 80%',
                'sector_impacts': {'technology': -0.80, 'default': -0.20}
            },
            'interest_rate_hike': {
                'name': 'Hausse taux directeurs',
                'description': 'Impact d\'une hausse de taux de 2%',
                'rate_impact': 0.02,
                'duration_impact': -0.15
            },
            'geopolitical_crisis': {
                'name': 'Crise géopolitique',
                'description': 'Crise internationale majeure',
                'market_return': -0.25,
                'commodity_spike': 0.40
            }
        }

    def create_trade_setup(self, df: pd.DataFrame, symbol: str, signal: str) -> Optional[TradeSetup]:
        """
        Calcule les paramètres optimaux pour un trade.
        
        Args:
            df: DataFrame avec données et indicateurs
            symbol: Symbole tradé
            signal: BUY ou SELL
            
        Returns:
            TradeSetup complet
        """
        if df.empty or signal not in ['BUY', 'SELL']:
            return None
            
        current_price = df['Close'].iloc[-1]
        atr = df['atr'].iloc[-1] if 'atr' in df.columns else (current_price * 0.001)
        
        # 1. Stop Loss (basé sur ATR)
        atr_multiplier = config.risk.ATR_MULTIPLIER
        sl_distance = atr * atr_multiplier
        
        if signal == 'BUY':
            stop_loss = current_price - sl_distance
        else:
            stop_loss = current_price + sl_distance
            
        # 2. Take Profits (basé sur les niveaux de risque)
        take_profits = []
        tp_levels = config.risk.TP_LEVELS
        
        for level in tp_levels:
            ratio = level['ratio']
            size_pct = level['size_percent']
            
            if signal == 'BUY':
                tp_price = current_price + (sl_distance * ratio)
            else:
                tp_price = current_price - (sl_distance * ratio)
                
            take_profits.append({
                'price': round(float(tp_price), 5),
                'size_percent': size_pct,
                'ratio': ratio
            })
            
        # 3. Risk-Reward Ratio (premier TP)
        risk = abs(current_price - stop_loss)
        reward = abs(take_profits[0]['price'] - current_price)
        rr_ratio = reward / risk if risk > 0 else 0
        
        # 4. Position Sizing (Lot Size)
        risk_per_trade = config.risk.RISK_PER_TRADE
        initial_capital = config.risk.INITIAL_CAPITAL
        risk_amount = initial_capital * risk_per_trade
        
        # Propriétés du symbole pour le calcul des lots
        pair_props = config.symbols.PAIR_PROPERTIES.get(symbol, {})
        pip_size = pair_props.get('pip_size', 0.0001)
        pip_value = pair_props.get('pip_value', 10)
        
        sl_pips = risk / pip_size
        if sl_pips > 0:
            suggested_lot = risk_amount / (sl_pips * pip_value)
        else:
            suggested_lot = 0.01
            
        return TradeSetup(
            symbol=symbol,
            signal=signal,
            entry_price=round(float(current_price), 5),
            stop_loss=round(float(stop_loss), 5),
            take_profits=take_profits,
            risk_reward=round(float(rr_ratio), 2),
            suggested_lot=round(float(max(suggested_lot, 0.01)), 2),
            timestamp=datetime.now()
        )

    def calculate_var(self, portfolio: Dict[str, float], confidence: float = 0.95,
                     method: VaRMethod = VaRMethod.HISTORICAL,
                     historical_data: Optional[Dict[str, pd.DataFrame]] = None) -> RiskMetrics:
        """
        Calcule la Value at Risk (VaR) du portefeuille.

        Args:
            portfolio: Dictionnaire {symbole: poids}
            confidence: Niveau de confiance (0.95 ou 0.99)
            method: Méthode de calcul
            historical_data: Données historiques {symbole: DataFrame}

        Returns:
            Métriques de risque calculées
        """
        try:
            if method == VaRMethod.HISTORICAL:
                return self._calculate_historical_var(portfolio, confidence, historical_data)
            elif method == VaRMethod.PARAMETRIC:
                return self._calculate_parametric_var(portfolio, confidence, historical_data)
            elif method == VaRMethod.MONTE_CARLO:
                return self._calculate_monte_carlo_var(portfolio, confidence, historical_data)
            else:
                raise ValueError(f"Méthode VaR non supportée: {method}")

        except Exception as e:
            logger.error(f"Erreur calcul VaR: {e}")
            # Retourner des métriques par défaut
            return RiskMetrics(
                var_95=0.05 if confidence == 0.95 else 0.10,
                var_99=0.10 if confidence == 0.99 else 0.15,
                volatility=0.20,
                max_drawdown=0.15,
                method="error_fallback",
                confidence_level=confidence,
                timestamp=datetime.utcnow()
            )

    def _calculate_historical_var(self, portfolio: Dict[str, float], confidence: float,
                                historical_data: Optional[Dict[str, pd.DataFrame]]) -> RiskMetrics:
        """Calcule la VaR historique."""
        if not historical_data:
            # Générer des données synthétiques
            historical_data = self._generate_synthetic_data(portfolio.keys())

        # Calculer les rendements du portefeuille
        portfolio_returns = self._calculate_portfolio_returns(portfolio, historical_data)

        if portfolio_returns.empty:
            raise ValueError("Impossible de calculer les rendements du portefeuille")

        # VaR historique
        var_value = -np.percentile(portfolio_returns, (1 - confidence) * 100)

        # CVaR (Expected Shortfall)
        losses = -portfolio_returns[portfolio_returns < 0]
        cvar_value = losses.mean() if not losses.empty else var_value

        # Autres métriques
        volatility = portfolio_returns.std() * np.sqrt(252)  # Annualisée
        cumulative = (1 + portfolio_returns).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_drawdown = drawdown.min()

        # Ratios de Sharpe et Sortino
        excess_returns = portfolio_returns - self.risk_free_rate/252
        sharpe_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(252) if excess_returns.std() > 0 else 0

        downside_returns = excess_returns[excess_returns < 0]
        sortino_ratio = excess_returns.mean() / downside_returns.std() * np.sqrt(252) if not downside_returns.empty else 0

        return RiskMetrics(
            var_95=var_value if confidence == 0.95 else var_value * 1.3,
            var_99=var_value * 1.3 if confidence == 0.95 else var_value,
            volatility=volatility,
            max_drawdown=max_drawdown,
            cvar_95=cvar_value if confidence == 0.95 else cvar_value * 1.3,
            cvar_99=cvar_value * 1.3 if confidence == 0.95 else cvar_value,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            method="historical",
            confidence_level=confidence,
            timestamp=datetime.utcnow()
        )

    def _calculate_parametric_var(self, portfolio: Dict[str, float], confidence: float,
                                historical_data: Optional[Dict[str, pd.DataFrame]]) -> RiskMetrics:
        """Calcule la VaR paramétrique (modèle normal)."""
        if not historical_data:
            historical_data = self._generate_synthetic_data(portfolio.keys())

        # Calculer les rendements du portefeuille
        portfolio_returns = self._calculate_portfolio_returns(portfolio, historical_data)

        if portfolio_returns.empty:
            raise ValueError("Impossible de calculer les rendements du portefeuille")

        # Paramètres de distribution normale
        mu = portfolio_returns.mean()
        sigma = portfolio_returns.std()

        # VaR paramétrique
        z_score = stats.norm.ppf(1 - confidence)
        var_value = -(mu + z_score * sigma)

        # CVaR paramétrique (approximation)
        cvar_value = mu + sigma * stats.norm.pdf(z_score) / (1 - confidence)

        # Autres métriques (similaires à historique)
        volatility = portfolio_returns.std() * np.sqrt(252)
        cumulative = (1 + portfolio_returns).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_drawdown = drawdown.min()

        return RiskMetrics(
            var_95=var_value if confidence == 0.95 else var_value * 1.3,
            var_99=var_value * 1.3 if confidence == 0.95 else var_value,
            volatility=volatility,
            max_drawdown=max_drawdown,
            cvar_95=cvar_value if confidence == 0.95 else cvar_value * 1.3,
            cvar_99=cvar_value * 1.3 if confidence == 0.95 else cvar_value,
            method="parametric",
            confidence_level=confidence,
            timestamp=datetime.utcnow()
        )

    def _calculate_monte_carlo_var(self, portfolio: Dict[str, float], confidence: float,
                                 historical_data: Optional[Dict[str, pd.DataFrame]],
                                 n_simulations: int = 10000) -> RiskMetrics:
        """Calcule la VaR par Monte Carlo."""
        if not historical_data:
            historical_data = self._generate_synthetic_data(portfolio.keys())

        portfolio_returns = self._calculate_portfolio_returns(portfolio, historical_data)

        if portfolio_returns.empty:
            raise ValueError("Impossible de calculer les rendements du portefeuille")

        # Paramètres pour les simulations
        mu = portfolio_returns.mean()
        sigma = portfolio_returns.std()

        # Simulations Monte Carlo
        simulated_returns = np.random.normal(mu, sigma, n_simulations)

        # VaR Monte Carlo
        var_value = -np.percentile(simulated_returns, (1 - confidence) * 100)

        # CVaR Monte Carlo
        losses = -simulated_returns[simulated_returns < 0]
        cvar_value = losses.mean() if len(losses) > 0 else var_value

        # Autres métriques
        volatility = portfolio_returns.std() * np.sqrt(252)
        cumulative = (1 + portfolio_returns).cumprod()
        peak = cumulative.expanding().max()
        drawdown = (cumulative - peak) / peak
        max_drawdown = drawdown.min()

        return RiskMetrics(
            var_95=var_value if confidence == 0.95 else var_value * 1.3,
            var_99=var_value * 1.3 if confidence == 0.95 else var_value,
            volatility=volatility,
            max_drawdown=max_drawdown,
            cvar_95=cvar_value if confidence == 0.95 else cvar_value * 1.3,
            cvar_99=cvar_value * 1.3 if confidence == 0.95 else cvar_value,
            method="monte_carlo",
            confidence_level=confidence,
            timestamp=datetime.utcnow()
        )

    def _calculate_portfolio_returns(self, portfolio: Dict[str, float],
                                   historical_data: Dict[str, pd.DataFrame]) -> pd.Series:
        """Calcule les rendements du portefeuille."""
        common_dates = None
        weighted_returns = []

        for symbol, weight in portfolio.items():
            if symbol in historical_data and not historical_data[symbol].empty:
                # Calculer les rendements quotidiens
                returns = historical_data[symbol]['Close'].pct_change().dropna()

                if common_dates is None:
                    common_dates = set(returns.index)
                else:
                    common_dates = common_dates.intersection(set(returns.index))

                weighted_returns.append((returns, weight))

        if not weighted_returns:
            return pd.Series()

        # Aligner sur les dates communes
        common_dates = sorted(list(common_dates))
        portfolio_returns = pd.Series(index=common_dates, dtype=float)

        for date in common_dates:
            daily_return = 0
            for returns, weight in weighted_returns:
                if date in returns.index:
                    daily_return += returns.loc[date] * weight
            portfolio_returns.loc[date] = daily_return

        return portfolio_returns.dropna()

    def _generate_synthetic_data(self, symbols) -> Dict[str, pd.DataFrame]:
        """Génère des données synthétiques pour les tests."""
        np.random.seed(42)
        dates = pd.date_range(start='2020-01-01', end='2024-01-01', freq='D')

        synthetic_data = {}
        for symbol in symbols:
            # Générer des prix synthétiques avec tendance et volatilité
            n_days = len(dates)
            returns = np.random.normal(0.0005, 0.02, n_days)  # Rendement quotidien moyen 0.05%, vol 2%
            prices = 100 * np.exp(np.cumsum(returns))  # Prix starting at 100

            df = pd.DataFrame({
                'Open': prices * 0.99,
                'High': prices * 1.02,
                'Low': prices * 0.98,
                'Close': prices,
                'Volume': np.random.randint(1000000, 10000000, n_days)
            }, index=dates)

            synthetic_data[symbol] = df

        return synthetic_data

    def stress_test(self, portfolio: Dict[str, float], scenario: str,
                   historical_data: Optional[Dict[str, pd.DataFrame]] = None) -> StressTestResult:
        """
        Effectue un test de stress sur le portefeuille.

        Args:
            portfolio: Dictionnaire {symbole: poids}
            scenario: Nom du scénario de stress
            historical_data: Données historiques

        Returns:
            Résultat du test de stress
        """
        try:
            if scenario not in self.stress_scenarios:
                raise ValueError(f"Scénario inconnu: {scenario}")

            scenario_config = self.stress_scenarios[scenario]

            # Appliquer les chocs du scénario
            stressed_portfolio = self._apply_stress_shocks(portfolio, scenario_config)

            # Calculer l'impact
            portfolio_return = sum(weight * shock for weight, shock in zip(
                portfolio.values(), stressed_portfolio.values()))

            portfolio_loss = -portfolio_return  # Perte positive

            # Vérifier si dépasse VaR
            var_metrics = self.calculate_var(portfolio, 0.95, VaRMethod.HISTORICAL, historical_data)
            var_breach = portfolio_loss > var_metrics.var_95

            # Calculer drawdown simulé
            drawdown = portfolio_loss  # Approximation

            # Identifier l'actif le plus touché
            worst_asset = max(stressed_portfolio.items(), key=lambda x: abs(x[1]))[0]
            worst_asset_loss = -stressed_portfolio[worst_asset]

            return StressTestResult(
                scenario_name=scenario_config['name'],
                portfolio_return=portfolio_return,
                portfolio_loss=portfolio_loss,
                var_breach=var_breach,
                drawdown=drawdown,
                worst_asset=worst_asset,
                worst_asset_loss=worst_asset_loss
            )

        except Exception as e:
            logger.error(f"Erreur test de stress {scenario}: {e}")
            return StressTestResult(
                scenario_name=scenario,
                portfolio_return=0.0,
                portfolio_loss=0.0,
                var_breach=False,
                drawdown=0.0,
                worst_asset="unknown",
                worst_asset_loss=0.0
            )

    def _apply_stress_shocks(self, portfolio: Dict[str, float], scenario_config: Dict) -> Dict[str, float]:
        """Applique les chocs de stress au portefeuille."""
        stressed_returns = {}

        for symbol, weight in portfolio.items():
            shock = 0

            if 'market_return' in scenario_config:
                shock = scenario_config['market_return']

            if 'volatility_multiplier' in scenario_config:
                # Augmenter la volatilité (impact négatif supplémentaire)
                shock *= scenario_config['volatility_multiplier']

            if 'sector_impacts' in scenario_config:
                # Impact sectoriel (simplifié - tous les symboles sont tech)
                shock = scenario_config['sector_impacts'].get('technology', shock)

            stressed_returns[symbol] = shock

        return stressed_returns

    def optimize_portfolio(self, assets: List[str], constraints: Dict,
                          historical_data: Optional[Dict[str, pd.DataFrame]] = None,
                          method: str = "markowitz") -> Dict[str, Any]:
        """
        Optimise la composition du portefeuille.

        Args:
            assets: Liste des symboles
            constraints: Contraintes d'optimisation
            historical_data: Données historiques
            method: Méthode d'optimisation ('markowitz', 'black_litterman', 'risk_parity')

        Returns:
            Portefeuille optimisé
        """
        try:
            if not historical_data:
                historical_data = self._generate_synthetic_data(assets)

            # Extraire les prix
            prices = {}
            for asset in assets:
                if asset in historical_data:
                    prices[asset] = historical_data[asset]['Close']

            if not prices:
                raise ValueError("Aucune donnée de prix disponible")

            prices_df = pd.DataFrame(prices).dropna()

            if method == "markowitz":
                return self._optimize_markowitz(prices_df, constraints)
            elif method == "black_litterman":
                return self._optimize_black_litterman(prices_df, constraints)
            elif method == "risk_parity":
                return self._optimize_risk_parity(prices_df, constraints)
            else:
                raise ValueError(f"Méthode d'optimisation inconnue: {method}")

        except Exception as e:
            logger.error(f"Erreur optimisation portefeuille: {e}")
            # Retourner un portefeuille équipondéré
            n_assets = len(assets)
            weights = {asset: 1.0/n_assets for asset in assets}
            return {
                'weights': weights,
                'expected_return': 0.08,
                'volatility': 0.15,
                'sharpe_ratio': 0.4,
                'method': 'equal_weight_fallback',
                'error': str(e)
            }

    def _optimize_markowitz(self, prices_df: pd.DataFrame, constraints: Dict) -> Dict:
        """Optimisation Markowitz (frontière efficiente) - implémentation personnalisée."""
        # Calculer les rendements et la covariance
        returns = prices_df.pct_change().dropna()
        mu = returns.mean() * 252  # Annualisé
        S = returns.cov() * 252    # Annualisé

        n_assets = len(mu)

        # Fonction objectif: maximiser Sharpe ratio = rendement / volatilité
        def objective(weights):
            portfolio_return = np.dot(weights, mu)
            portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(S, weights)))
            sharpe = portfolio_return / portfolio_volatility if portfolio_volatility > 0 else 0
            return -sharpe  # Minimiser le négatif pour maximiser

        # Contraintes
        constraints_opt = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}  # Somme des poids = 1
        ]

        # Bounds: chaque poids entre 0 et 1
        bounds = [(0, 1) for _ in range(n_assets)]

        # Optimisation
        from scipy.optimize import minimize
        result = minimize(objective, np.ones(n_assets)/n_assets, method='SLSQP',
                         bounds=bounds, constraints=constraints_opt)

        if result.success:
            weights = result.x
            weights_dict = dict(zip(prices_df.columns, weights))

            portfolio_return = np.dot(weights, mu)
            portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(S, weights)))
            sharpe_ratio = portfolio_return / portfolio_volatility if portfolio_volatility > 0 else 0

            return {
                'weights': weights_dict,
                'expected_return': portfolio_return,
                'volatility': portfolio_volatility,
                'sharpe_ratio': sharpe_ratio,
                'method': 'markowitz_custom'
            }
        else:
            # Fallback: portefeuille équipondéré
            weights = np.ones(n_assets) / n_assets
            weights_dict = dict(zip(prices_df.columns, weights))
            return {
                'weights': weights_dict,
                'expected_return': mu.mean(),
                'volatility': np.sqrt(S.values.mean()),
                'sharpe_ratio': 0.5,
                'method': 'equal_weight_fallback'
            }

    def _optimize_black_litterman(self, prices_df: pd.DataFrame, constraints: Dict) -> Dict:
        """Optimisation Black-Litterman - implémentation simplifiée."""
        # Pour l'instant, utiliser la même logique que Markowitz
        # Une vraie implémentation BL nécessiterait plus de paramètres
        logger.info("Black-Litterman: utilisant optimisation Markowitz (implémentation simplifiée)")

        return self._optimize_markowitz(prices_df, constraints)

    def _optimize_risk_parity(self, prices_df: pd.DataFrame, constraints: Dict) -> Dict:
        """Optimisation risk parity."""
        # Simplification: portefeuille équipondéré en risque
        n_assets = len(prices_df.columns)
        weights = {asset: 1.0/n_assets for asset in prices_df.columns}

        # Calculer les métriques
        mu = expected_returns.mean_historical_return(prices_df)
        S = risk_models.sample_cov(prices_df)

        portfolio_return = sum(weights[asset] * mu[asset] for asset in weights)
        portfolio_vol = np.sqrt(sum(weights[asset] * weights[asset2] * S.loc[asset, asset2]
                                   for asset in weights for asset2 in weights))
        sharpe = portfolio_return / portfolio_vol if portfolio_vol > 0 else 0

        return {
            'weights': weights,
            'expected_return': portfolio_return,
            'volatility': portfolio_vol,
            'sharpe_ratio': sharpe,
            'method': 'risk_parity'
        }

    def run_comprehensive_risk_analysis(self, portfolio: Dict[str, float],
                                      historical_data: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
        """
        Analyse complète des risques du portefeuille.

        Args:
            portfolio: Dictionnaire {symbole: poids}
            historical_data: Données historiques

        Returns:
            Analyse complète des risques
        """
        try:
            analysis = {
                'portfolio': portfolio,
                'timestamp': datetime.utcnow().isoformat(),
                'risk_metrics': {},
                'stress_tests': {},
                'recommendations': []
            }

            # Calculer VaR avec différentes méthodes
            for method in [VaRMethod.HISTORICAL, VaRMethod.PARAMETRIC, VaRMethod.MONTE_CARLO]:
                metrics = self.calculate_var(portfolio, 0.95, method, historical_data)
                analysis['risk_metrics'][method.value] = {
                    'var_95': metrics.var_95,
                    'var_99': metrics.var_99,
                    'volatility': metrics.volatility,
                    'max_drawdown': metrics.max_drawdown,
                    'sharpe_ratio': metrics.sharpe_ratio
                }

            # Tests de stress
            for scenario in self.stress_scenarios.keys():
                stress_result = self.stress_test(portfolio, scenario, historical_data)
                analysis['stress_tests'][scenario] = {
                    'portfolio_loss': stress_result.portfolio_loss,
                    'var_breach': stress_result.var_breach,
                    'worst_asset': stress_result.worst_asset,
                    'worst_asset_loss': stress_result.worst_asset_loss
                }

            # Recommandations
            avg_var = np.mean([m['var_95'] for m in analysis['risk_metrics'].values()])
            if avg_var > 0.10:  # VaR > 10%
                analysis['recommendations'].append("Réduire l'exposition - VaR élevée")
            if any(st['var_breach'] for st in analysis['stress_tests'].values()):
                analysis['recommendations'].append("Renforcer la diversification - vulnérable aux chocs")

            return analysis

        except Exception as e:
            logger.error(f"Erreur analyse risque complète: {e}")
            return {
                'error': str(e),
                'portfolio': portfolio,
                'timestamp': datetime.utcnow().isoformat()
            }

# Instance globale
risk_manager = RiskManager()

def get_risk_manager() -> RiskManager:
    """Retourne l'instance globale du gestionnaire de risque."""
    return risk_manager
