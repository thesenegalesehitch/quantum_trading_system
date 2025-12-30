"""
Moteur d'ingénierie des features pour le machine learning.
Transforme les données brutes en features exploitables.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Any
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config


class FeatureEngine:
    """
    Crée et gère les features pour l'analyse et le ML.
    
    Categories de features:
    1. Prix et dérivés (returns, log-returns)
    2. Cycles temporels (heure, jour, mois)
    3. Volatilité (ATR, écart-type rolling)
    4. Volume (ratios, moyennes mobiles)
    5. Indicateurs techniques (via pandas-ta)
    """
    
    def __init__(self):
        """Initialise le moteur de features."""
        self.feature_names = []
    
    def create_all_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crée toutes les features à partir du DataFrame OHLCV.

        Args:
            df: DataFrame avec colonnes OHLCV et index DatetimeIndex

        Returns:
            DataFrame enrichi avec toutes les features
        """
        result = df.copy()

        try:
            # 1. Features de prix
            result = self._add_price_features(result)

            # 2. Features temporelles
            result = self._add_time_features(result)

            # 3. Features de volatilité
            result = self._add_volatility_features(result)

            # 4. Features de volume
            result = self._add_volume_features(result)

            # 5. Indicateurs techniques
            result = self._add_technical_indicators(result)

            # Stocker les noms des features
            self.feature_names = [col for col in result.columns if col not in df.columns]

        except Exception as e:
            print(f"⚠️ Erreur lors de la création des features ({e}), utilisation des features basiques seulement")
            result = self._add_basic_price_features(result)
            result = self._add_basic_indicators(result)
            self.feature_names = [col for col in result.columns if col not in df.columns]

        return result
    
    def _add_basic_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute des features de prix basiques sans calculs complexes.
        """
        result = df.copy()

        # Rendements simples
        result['returns'] = result['Close'].pct_change()

        # Amplitudes
        result['high_low_range'] = (result['High'] - result['Low']) / result['Close']

        # Prix dérivés
        result['typical_price'] = (result['High'] + result['Low'] + result['Close']) / 3

        return result

    def _add_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les features dérivées des prix.
        
        Features créées:
        - returns: Rendements simples
        - log_returns: Rendements logarithmiques
        - high_low_range: Amplitude haut-bas
        - close_open_range: Amplitude close-open
        - typical_price: Prix typique (H+L+C)/3
        """
        result = df.copy()
        
        # Rendements
        result['returns'] = result['Close'].pct_change()
        result['log_returns'] = np.log(result['Close'] / result['Close'].shift(1))
        
        # Amplitudes
        result['high_low_range'] = (result['High'] - result['Low']) / result['Close']
        result['close_open_range'] = (result['Close'] - result['Open']) / result['Open']
        
        # Prix dérivés
        result['typical_price'] = (result['High'] + result['Low'] + result['Close']) / 3
        result['weighted_close'] = (result['High'] + result['Low'] + 2 * result['Close']) / 4
        
        # Prix normalisé dans la range du jour
        result['price_position'] = (result['Close'] - result['Low']) / (result['High'] - result['Low'] + 1e-10)
        
        return result
    
    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les features temporelles cycliques.

        Utilise des encodages sin/cos pour capturer la nature cyclique du temps.
        """
        result = df.copy()

        # Assurer que l'index est DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                result.index = pd.to_datetime(result.index)
                print("✅ Index converti en DatetimeIndex")
            except Exception as e:
                print(f"⚠️ Impossible de convertir l'index en DatetimeIndex ({e}), features temporelles ignorées")
                return result

        # Heure du jour (cyclique)
        if config.data.EXTRACT_HOUR:
            try:
                hour = result.index.hour
                result['hour_sin'] = np.sin(2 * np.pi * hour / 24)
                result['hour_cos'] = np.cos(2 * np.pi * hour / 24)
            except Exception as e:
                print(f"⚠️ Erreur extraction heure ({e}), ignoré")

        # Jour de la semaine (cyclique)
        if config.data.EXTRACT_DAY_OF_WEEK:
            try:
                day = result.index.dayofweek
                result['day_sin'] = np.sin(2 * np.pi * day / 7)
                result['day_cos'] = np.cos(2 * np.pi * day / 7)
                result['day_of_week'] = day
            except Exception as e:
                print(f"⚠️ Erreur extraction jour ({e}), ignoré")

        # Mois (cyclique)
        if config.data.EXTRACT_MONTH:
            try:
                month = result.index.month
                result['month_sin'] = np.sin(2 * np.pi * month / 12)
                result['month_cos'] = np.cos(2 * np.pi * month / 12)
            except Exception as e:
                print(f"⚠️ Erreur extraction mois ({e}), ignoré")

        # Session de trading
        try:
            # Créer une série avec l'index correct pour éviter les problèmes d'alignement
            hours_series = pd.Series(result.index.hour, index=result.index)
            result['session'] = self._get_trading_session(hours_series)
        except Exception as e:
            print(f"⚠️ Erreur extraction session ({e}), ignoré")

        return result
    
    def _add_volatility_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les features de volatilité.
        
        Features:
        - ATR (Average True Range)
        - Volatilité historique (écart-type rolling)
        - Parkinson volatility
        - Garman-Klass volatility
        """
        result = df.copy()
        window = config.data.VOLATILITY_WINDOW
        
        # True Range
        result['tr'] = self._true_range(result)
        
        # ATR (Average True Range)
        result['atr'] = result['tr'].rolling(window=window).mean()
        result['atr_normalized'] = result['atr'] / result['Close']
        
        # Volatilité historique (écart-type des rendements)
        result['volatility'] = result['returns'].rolling(window=window).std()
        result['volatility_annualized'] = result['volatility'] * np.sqrt(252)
        
        # Parkinson Volatility (basée sur High-Low)
        result['parkinson_vol'] = np.sqrt(
            (1 / (4 * np.log(2))) * 
            ((np.log(result['High'] / result['Low'])) ** 2).rolling(window=window).mean()
        )
        
        # Garman-Klass Volatility (plus précise)
        result['gk_vol'] = self._garman_klass_volatility(result, window)
        
        # Ratio de volatilité (court terme vs long terme)
        result['vol_ratio'] = (
            result['volatility'].rolling(5).mean() / 
            result['volatility'].rolling(20).mean()
        )
        
        return result
    
    def _add_volume_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les features basées sur le volume.
        """
        result = df.copy()
        
        if 'Volume' not in result.columns or result['Volume'].sum() == 0:
            # Pas de données de volume
            result['volume_ratio'] = 1.0
            result['volume_ma'] = 1.0
            result['volume_std'] = 0.0
            return result
        
        window = config.data.VOLUME_NORMALIZATION_WINDOW
        
        # Moyenne mobile du volume
        result['volume_ma'] = result['Volume'].rolling(window=window).mean()
        
        # Volume normalisé (ratio vs moyenne)
        result['volume_ratio'] = result['Volume'] / (result['volume_ma'] + 1e-10)
        
        # Écart-type du volume
        result['volume_std'] = result['Volume'].rolling(window=window).std()
        
        # Z-score du volume
        result['volume_zscore'] = (
            (result['Volume'] - result['volume_ma']) / 
            (result['volume_std'] + 1e-10)
        )
        
        # On-Balance Volume simplifié
        result['obv'] = (np.sign(result['returns']) * result['Volume']).cumsum()
        
        # Volume-Weighted Average Price (VWAP) approximé
        result['vwap'] = (
            (result['typical_price'] * result['Volume']).rolling(window=window).sum() /
            (result['Volume'].rolling(window=window).sum() + 1e-10)
        )
        
        return result
    
    def _add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute les indicateurs techniques via pandas-ta.
        """
        result = df.copy()
        
        try:
            import pandas_ta as ta

            # RSI
            result['rsi'] = ta.rsi(result['Close'], length=14)

            # MACD
            macd = ta.macd(result['Close'])
            if macd is not None:
                result['macd'] = macd['MACD_12_26_9']
                result['macd_signal'] = macd['MACDs_12_26_9']
                result['macd_hist'] = macd['MACDh_12_26_9']

            # Bollinger Bands
            bb = ta.bbands(result['Close'], length=20, std=2)
            if bb is not None:
                # Fallback pour les noms de colonnes pandas-ta qui peuvent varier
                upper_col = 'BBU_20_2.0' if 'BBU_20_2.0' in bb.columns else bb.columns[2]
                mid_col = 'BBM_20_2.0' if 'BBM_20_2.0' in bb.columns else bb.columns[1]
                lower_col = 'BBL_20_2.0' if 'BBL_20_2.0' in bb.columns else bb.columns[0]
                
                result['bb_upper'] = bb[upper_col]
                result['bb_middle'] = bb[mid_col]
                result['bb_lower'] = bb[lower_col]
                result['bb_width'] = (result['bb_upper'] - result['bb_lower']) / result['bb_middle']
                result['bb_position'] = (result['Close'] - result['bb_lower']) / (result['bb_upper'] - result['bb_lower'] + 1e-10)

            # Stochastic
            stoch = ta.stoch(result['High'], result['Low'], result['Close'])
            if stoch is not None:
                result['stoch_k'] = stoch['STOCHk_14_3_3']
                result['stoch_d'] = stoch['STOCHd_14_3_3']

            # ADX
            adx = ta.adx(result['High'], result['Low'], result['Close'])
            if adx is not None:
                result['adx'] = adx['ADX_14']
                result['di_plus'] = adx['DMP_14']
                result['di_minus'] = adx['DMN_14']

            # CCI
            result['cci'] = ta.cci(result['High'], result['Low'], result['Close'])

            # Williams %R
            result['willr'] = ta.willr(result['High'], result['Low'], result['Close'])

            # MFI (si volume disponible)
            if 'Volume' in result.columns and result['Volume'].sum() > 0:
                result['mfi'] = ta.mfi(result['High'], result['Low'], result['Close'], result['Volume'])

        except Exception as e:
            print(f"⚠️ Erreur pandas-ta ({e}), indicateurs techniques basiques seulement")
            result = self._add_basic_indicators(result)
        
        return result
    
    def _add_basic_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ajoute des indicateurs techniques basiques sans pandas-ta.
        """
        result = df.copy()
        
        # RSI simplifié
        delta = result['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 1e-10)
        result['rsi'] = 100 - (100 / (1 + rs))
        
        # Moyennes mobiles
        result['sma_20'] = result['Close'].rolling(20).mean()
        result['sma_50'] = result['Close'].rolling(50).mean()
        result['ema_12'] = result['Close'].ewm(span=12).mean()
        result['ema_26'] = result['Close'].ewm(span=26).mean()
        
        # MACD basique
        result['macd'] = result['ema_12'] - result['ema_26']
        result['macd_signal'] = result['macd'].ewm(span=9).mean()
        result['macd_hist'] = result['macd'] - result['macd_signal']
        
        return result
    
    def _true_range(self, df: pd.DataFrame) -> pd.Series:
        """Calcule le True Range."""
        high_low = df['High'] - df['Low']
        high_close = abs(df['High'] - df['Close'].shift(1))
        low_close = abs(df['Low'] - df['Close'].shift(1))
        
        return pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    
    def _garman_klass_volatility(self, df: pd.DataFrame, window: int) -> pd.Series:
        """
        Calcule la volatilité de Garman-Klass.
        Plus efficace que la volatilité historique classique.
        """
        log_hl = np.log(df['High'] / df['Low']) ** 2
        log_co = np.log(df['Close'] / df['Open']) ** 2
        
        gk = 0.5 * log_hl - (2 * np.log(2) - 1) * log_co
        
        return np.sqrt(gk.rolling(window=window).mean() * 252)
    
    def _get_trading_session(self, hours: pd.Series) -> pd.Series:
        """
        Détermine la session de trading.
        0: Asie, 1: Europe, 2: US, 3: Overlap
        """
        sessions = pd.Series(index=hours.index, data=0, dtype=int)
        
        for i in range(len(hours)):
            hour = hours.iloc[i]
            if 0 <= hour < 8:
                sessions.iloc[i] = 0  # Asie
            elif 8 <= hour < 13:
                sessions.iloc[i] = 1  # Europe
            elif 13 <= hour < 17:
                sessions.iloc[i] = 3  # Overlap EU/US
            elif 17 <= hour < 22:
                sessions.iloc[i] = 2  # US
            else:
                sessions.iloc[i] = 0  # Asie (fin)
        
        return sessions
    
    def get_feature_importance(
        self,
        df: pd.DataFrame,
        target_col: str = 'returns'
    ) -> pd.DataFrame:
        """
        Calcule l'importance des features par corrélation avec la cible.
        
        Args:
            df: DataFrame avec features
            target_col: Colonne cible
        
        Returns:
            DataFrame avec importance des features
        """
        if target_col not in df.columns:
            raise ValueError(f"Colonne cible '{target_col}' non trouvée")
        
        correlations = []
        
        for col in self.feature_names:
            if col in df.columns and col != target_col:
                corr = df[col].corr(df[target_col])
                correlations.append({
                    'feature': col,
                    'correlation': corr,
                    'abs_correlation': abs(corr)
                })
        
        importance_df = pd.DataFrame(correlations)
        importance_df = importance_df.sort_values('abs_correlation', ascending=False)
        
        return importance_df
    
    def prepare_ml_features(
        self,
        df: pd.DataFrame,
        feature_cols: List[str] = None,
        dropna: bool = True
    ) -> pd.DataFrame:
        """
        Prépare les features pour le ML (normalisation, gestion NaN).
        
        Args:
            df: DataFrame avec features
            feature_cols: Colonnes à utiliser (défaut: config.ml.FEATURE_COLUMNS)
            dropna: Supprimer les lignes avec NaN
        
        Returns:
            DataFrame prêt pour le ML
        """
        feature_cols = feature_cols or config.ml.FEATURE_COLUMNS
        
        # Filtrer les colonnes existantes
        available_cols = [col for col in feature_cols if col in df.columns]
        
        if not available_cols:
            print("⚠️ Aucune feature demandée n'est disponible")
            return pd.DataFrame()
        
        result = df[available_cols].copy()
        
        if dropna:
            result = result.dropna()
        
        return result


if __name__ == "__main__":
    # Test du feature engine
    import numpy as np
    
    # Créer des données de test
    dates = pd.date_range(start='2023-01-01', periods=100, freq='1H')
    np.random.seed(42)
    
    test_df = pd.DataFrame({
        'Open': 100 + np.cumsum(np.random.randn(100) * 0.5),
        'High': 100 + np.cumsum(np.random.randn(100) * 0.5) + 0.5,
        'Low': 100 + np.cumsum(np.random.randn(100) * 0.5) - 0.5,
        'Close': 100 + np.cumsum(np.random.randn(100) * 0.5),
        'Volume': np.random.randint(1000, 10000, 100)
    }, index=dates)
    
    # Créer les features
    engine = FeatureEngine()
    features_df = engine.create_all_features(test_df)
    
    print(f"Nombre de features créées: {len(engine.feature_names)}")
    print(f"\nFeatures créées:")
    for name in engine.feature_names[:20]:
        print(f"  - {name}")
    
    print(f"\nShape finale: {features_df.shape}")
    print(f"\n{features_df.tail()}")
