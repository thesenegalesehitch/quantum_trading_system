"""
Analyse Wyckoff - Détection des phases d'accumulation et distribution.

Méthode Wyckoff:
- Accumulation: Les gros investisseurs achètent discrètement
- Distribution: Les gros investisseurs vendent discrètement
- Markup: Phase de hausse
- Markdown: Phase de baisse

Patterns clés:
- Spring: Faux breakout à la baisse (signal d'achat)
- UTAD: Upthrust After Distribution (signal de vente)
- Sign of Strength (SOS): Confirmation de demande
- Sign of Weakness (SOW): Confirmation d'offre
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config


class WyckoffPhase(Enum):
    """Phases du cycle Wyckoff."""
    ACCUMULATION = "accumulation"
    MARKUP = "markup"
    DISTRIBUTION = "distribution"
    MARKDOWN = "markdown"
    UNKNOWN = "unknown"


class WyckoffEvent(Enum):
    """Événements Wyckoff."""
    PS = "preliminary_support"  # Support préliminaire
    SC = "selling_climax"       # Climax de vente
    AR = "automatic_rally"      # Rally automatique
    ST = "secondary_test"       # Test secondaire
    SPRING = "spring"           # Faux breakout baissier
    SOS = "sign_of_strength"    # Signe de force
    LPS = "last_point_support"  # Dernier point de support
    
    PSY = "preliminary_supply"  # Offre préliminaire
    BC = "buying_climax"        # Climax d'achat
    AR_D = "automatic_reaction" # Réaction automatique
    ST_D = "secondary_test_distribution"
    UTAD = "upthrust_after_distribution"
    SOW = "sign_of_weakness"    # Signe de faiblesse
    LPSY = "last_point_supply"  # Dernier point d'offre


@dataclass
class WyckoffStructure:
    """Structure Wyckoff identifiée."""
    phase: WyckoffPhase
    events: List[Dict]
    support_level: float
    resistance_level: float
    current_position: str  # "above_range", "in_range", "below_range"
    signal: str
    confidence: float


class WyckoffAnalyzer:
    """
    Analyse les structures Wyckoff pour identifier accumulation/distribution.
    
    L'analyse se base sur:
    1. Volume spread analysis
    2. Identification des ranges
    3. Détection des events clés
    """
    
    def __init__(
        self,
        lookback: int = 100,
        range_threshold: float = 0.03  # 3% pour définir un range
    ):
        self.lookback = lookback
        self.range_threshold = range_threshold
    
    def analyze(self, df: pd.DataFrame) -> WyckoffStructure:
        """
        Analyse complète Wyckoff.
        
        Args:
            df: DataFrame OHLCV
        
        Returns:
            WyckoffStructure avec phase, events et signal
        """
        if len(df) < self.lookback:
            return WyckoffStructure(
                phase=WyckoffPhase.UNKNOWN,
                events=[],
                support_level=0,
                resistance_level=0,
                current_position="unknown",
                signal="WAIT",
                confidence=0
            )
        
        df_work = df.tail(self.lookback).copy()
        
        # 1. Identifier le range (support/résistance)
        support, resistance = self._identify_range(df_work)
        
        # 2. Analyser le volume spread
        vsa = self._volume_spread_analysis(df_work)
        
        # 3. Détecter les events Wyckoff
        events = self._detect_events(df_work, support, resistance, vsa)
        
        # 4. Déterminer la phase
        phase = self._determine_phase(df_work, events, vsa)
        
        # 5. Position actuelle
        current_price = df_work['Close'].iloc[-1]
        if current_price > resistance:
            current_position = "above_range"
        elif current_price < support:
            current_position = "below_range"
        else:
            current_position = "in_range"
        
        # 6. Générer le signal
        signal, confidence = self._generate_signal(phase, events, current_position)
        
        return WyckoffStructure(
            phase=phase,
            events=events,
            support_level=support,
            resistance_level=resistance,
            current_position=current_position,
            signal=signal,
            confidence=confidence
        )
    
    def _identify_range(self, df: pd.DataFrame) -> Tuple[float, float]:
        """Identifie les niveaux de support et résistance du range."""
        # Utiliser les pivots récents
        highs = df['High'].values
        lows = df['Low'].values
        
        # Trouver les swing highs et lows significatifs
        swing_highs = []
        swing_lows = []
        
        window = 5
        for i in range(window, len(df) - window):
            # Swing high
            if all(highs[i] >= highs[i-j] for j in range(1, window+1)) and \
               all(highs[i] >= highs[i+j] for j in range(1, window+1)):
                swing_highs.append(highs[i])
            
            # Swing low
            if all(lows[i] <= lows[i-j] for j in range(1, window+1)) and \
               all(lows[i] <= lows[i+j] for j in range(1, window+1)):
                swing_lows.append(lows[i])
        
        if not swing_highs or not swing_lows:
            return df['Low'].min(), df['High'].max()
        
        # Clustering pour trouver les niveaux clés
        resistance = np.median(sorted(swing_highs)[-3:]) if len(swing_highs) >= 3 else max(swing_highs)
        support = np.median(sorted(swing_lows)[:3]) if len(swing_lows) >= 3 else min(swing_lows)
        
        return support, resistance
    
    def _volume_spread_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyse Volume Spread.
        
        Classifie chaque bougie selon:
        - Effort (volume)
        - Résultat (spread)
        """
        vsa = pd.DataFrame(index=df.index)
        
        # Spread (taille de la bougie)
        vsa['spread'] = df['High'] - df['Low']
        vsa['spread_pct'] = vsa['spread'] / df['Close']
        
        # Comparaison aux moyennes
        avg_spread = vsa['spread'].rolling(20).mean()
        avg_volume = df['Volume'].rolling(20).mean() if 'Volume' in df.columns else pd.Series(1, index=df.index)
        
        vsa['spread_vs_avg'] = vsa['spread'] / (avg_spread + 1e-10)
        vsa['volume_vs_avg'] = df.get('Volume', pd.Series(1, index=df.index)) / (avg_volume + 1e-10)
        
        # Close position dans le spread
        vsa['close_position'] = (df['Close'] - df['Low']) / (vsa['spread'] + 1e-10)
        
        # Classification VSA
        vsa['effort'] = pd.cut(vsa['volume_vs_avg'], bins=[0, 0.7, 1.3, float('inf')], 
                               labels=['low', 'normal', 'high'])
        vsa['result'] = pd.cut(vsa['spread_vs_avg'], bins=[0, 0.7, 1.3, float('inf')],
                               labels=['narrow', 'normal', 'wide'])
        
        # Signaux VSA
        # No Demand: Volume faible, spread étroit, close bas
        vsa['no_demand'] = (vsa['volume_vs_avg'] < 0.7) & (vsa['spread_vs_avg'] < 0.7) & (vsa['close_position'] < 0.3)
        
        # No Supply: Volume faible, spread étroit, close haut
        vsa['no_supply'] = (vsa['volume_vs_avg'] < 0.7) & (vsa['spread_vs_avg'] < 0.7) & (vsa['close_position'] > 0.7)
        
        # Stopping Volume: Volume très élevé après baisse
        vsa['stopping_volume'] = (vsa['volume_vs_avg'] > 1.5) & (vsa['close_position'] > 0.5)
        
        # Climax: Volume très élevé avec wide spread
        vsa['climax'] = (vsa['volume_vs_avg'] > 2) & (vsa['spread_vs_avg'] > 1.5)
        
        return vsa
    
    def _detect_events(
        self,
        df: pd.DataFrame,
        support: float,
        resistance: float,
        vsa: pd.DataFrame
    ) -> List[Dict]:
        """Détecte les events Wyckoff clés."""
        events = []
        close = df['Close'].values
        low = df['Low'].values
        high = df['High'].values
        
        range_size = resistance - support
        tolerance = range_size * 0.02  # 2% de tolérance
        
        for i in range(10, len(df)):
            idx = df.index[i]
            
            # SPRING: Break sous le support puis retour
            if i >= 2:
                if low[i-1] < support - tolerance and close[i] > support:
                    if vsa['stopping_volume'].iloc[i] or vsa['no_supply'].iloc[i-1]:
                        events.append({
                            'event': WyckoffEvent.SPRING.value,
                            'index': i,
                            'timestamp': idx,
                            'price': close[i],
                            'strength': min((support - low[i-1]) / range_size * 10, 1.0)
                        })
            
            # UTAD: Break au-dessus de la résistance puis retour
            if i >= 2:
                if high[i-1] > resistance + tolerance and close[i] < resistance:
                    if vsa['climax'].iloc[i-1]:
                        events.append({
                            'event': WyckoffEvent.UTAD.value,
                            'index': i,
                            'timestamp': idx,
                            'price': close[i],
                            'strength': min((high[i-1] - resistance) / range_size * 10, 1.0)
                        })
            
            # SOS (Sign of Strength): Break haussier avec fort volume
            if close[i] > resistance and vsa['volume_vs_avg'].iloc[i] > 1.3:
                if close[i-1] <= resistance:
                    events.append({
                        'event': WyckoffEvent.SOS.value,
                        'index': i,
                        'timestamp': idx,
                        'price': close[i],
                        'strength': min(vsa['volume_vs_avg'].iloc[i] / 2, 1.0)
                    })
            
            # SOW (Sign of Weakness): Break baissier avec fort volume
            if close[i] < support and vsa['volume_vs_avg'].iloc[i] > 1.3:
                if close[i-1] >= support:
                    events.append({
                        'event': WyckoffEvent.SOW.value,
                        'index': i,
                        'timestamp': idx,
                        'price': close[i],
                        'strength': min(vsa['volume_vs_avg'].iloc[i] / 2, 1.0)
                    })
            
            # Selling Climax: Volume extrême avec wide spread baissier
            if vsa['climax'].iloc[i] and close[i] < df['Open'].iloc[i]:
                if close[i] <= support + tolerance:
                    events.append({
                        'event': WyckoffEvent.SC.value,
                        'index': i,
                        'timestamp': idx,
                        'price': close[i],
                        'strength': 0.8
                    })
            
            # Buying Climax: Volume extrême avec wide spread haussier
            if vsa['climax'].iloc[i] and close[i] > df['Open'].iloc[i]:
                if close[i] >= resistance - tolerance:
                    events.append({
                        'event': WyckoffEvent.BC.value,
                        'index': i,
                        'timestamp': idx,
                        'price': close[i],
                        'strength': 0.8
                    })
        
        return events
    
    def _determine_phase(
        self,
        df: pd.DataFrame,
        events: List[Dict],
        vsa: pd.DataFrame
    ) -> WyckoffPhase:
        """Détermine la phase Wyckoff actuelle."""
        if not events:
            # Analyser la tendance générale
            returns = df['Close'].pct_change()
            trend = returns.tail(20).mean()
            
            if trend > 0.001:
                return WyckoffPhase.MARKUP
            elif trend < -0.001:
                return WyckoffPhase.MARKDOWN
            else:
                return WyckoffPhase.UNKNOWN
        
        # Compter les events récents par type
        recent_events = events[-10:] if len(events) > 10 else events
        
        accumulation_events = [WyckoffEvent.SC.value, WyckoffEvent.SPRING.value, 
                               WyckoffEvent.SOS.value, WyckoffEvent.ST.value]
        distribution_events = [WyckoffEvent.BC.value, WyckoffEvent.UTAD.value,
                               WyckoffEvent.SOW.value]
        
        acc_count = sum(1 for e in recent_events if e['event'] in accumulation_events)
        dist_count = sum(1 for e in recent_events if e['event'] in distribution_events)
        
        if acc_count > dist_count:
            # Vérifier si on est en breakout (markup)
            last_event = events[-1]
            if last_event['event'] == WyckoffEvent.SOS.value:
                return WyckoffPhase.MARKUP
            return WyckoffPhase.ACCUMULATION
        elif dist_count > acc_count:
            last_event = events[-1]
            if last_event['event'] == WyckoffEvent.SOW.value:
                return WyckoffPhase.MARKDOWN
            return WyckoffPhase.DISTRIBUTION
        
        return WyckoffPhase.UNKNOWN
    
    def _generate_signal(
        self,
        phase: WyckoffPhase,
        events: List[Dict],
        current_position: str
    ) -> Tuple[str, float]:
        """Génère un signal de trading basé sur l'analyse Wyckoff."""
        if not events:
            return "WAIT", 0.3
        
        last_events = events[-3:] if len(events) >= 3 else events
        last_event = events[-1]
        
        # Signaux forts
        if last_event['event'] == WyckoffEvent.SPRING.value:
            return "STRONG_BUY", min(0.7 + last_event['strength'] * 0.3, 1.0)
        
        if last_event['event'] == WyckoffEvent.UTAD.value:
            return "STRONG_SELL", min(0.7 + last_event['strength'] * 0.3, 1.0)
        
        if last_event['event'] == WyckoffEvent.SOS.value:
            if phase == WyckoffPhase.ACCUMULATION or phase == WyckoffPhase.MARKUP:
                return "BUY", 0.7
        
        if last_event['event'] == WyckoffEvent.SOW.value:
            if phase == WyckoffPhase.DISTRIBUTION or phase == WyckoffPhase.MARKDOWN:
                return "SELL", 0.7
        
        # Signaux basés sur la phase
        if phase == WyckoffPhase.ACCUMULATION:
            if current_position == "in_range":
                return "BUY", 0.65  # Augmenté de LEAN_BUY 0.5
        elif phase == WyckoffPhase.DISTRIBUTION:
            if current_position == "in_range":
                return "SELL", 0.65  # Augmenté de LEAN_SELL 0.5
        elif phase == WyckoffPhase.MARKUP:
            return "BUY", 0.75  # Augmenté de 0.6
        elif phase == WyckoffPhase.MARKDOWN:
            return "SELL", 0.75  # Augmenté de 0.6
        
        return "WAIT", 0.3
    
    def get_analysis_summary(self, df: pd.DataFrame) -> Dict:
        """Retourne un résumé de l'analyse Wyckoff."""
        structure = self.analyze(df)
        
        return {
            "phase": structure.phase.value,
            "signal": structure.signal,
            "confidence": round(structure.confidence, 2),
            "support": round(structure.support_level, 5),
            "resistance": round(structure.resistance_level, 5),
            "position": structure.current_position,
            "events_count": len(structure.events),
            "recent_events": [
                {"event": e['event'], "strength": round(e['strength'], 2)}
                for e in structure.events[-5:]
            ]
        }


if __name__ == "__main__":
    print("=" * 60)
    print("TEST ANALYSE WYCKOFF")
    print("=" * 60)
    
    # Données de test - simuler une accumulation
    np.random.seed(42)
    n = 150
    
    # Créer un range avec quelques breakouts
    base = 100
    noise = np.random.randn(n) * 0.5
    
    # Phase de range
    prices = []
    for i in range(n):
        if i < 50:
            prices.append(base + noise[i])
        elif i < 70:
            # Spring
            if i == 60:
                prices.append(base - 3)  # Break sous support
            else:
                prices.append(base - 1 + noise[i] * 0.5)
        elif i < 100:
            prices.append(base + noise[i])
        else:
            # Breakout
            prices.append(base + (i - 100) * 0.2 + noise[i] * 0.3)
    
    df = pd.DataFrame({
        'Open': np.array(prices) - 0.2,
        'High': np.array(prices) + abs(np.random.randn(n) * 0.5),
        'Low': np.array(prices) - abs(np.random.randn(n) * 0.5),
        'Close': prices,
        'Volume': np.random.randint(1000, 10000, n)
    }, index=pd.date_range(start='2024-01-01', periods=n, freq='1h'))
    
    # Analyser
    wyckoff = WyckoffAnalyzer()
    summary = wyckoff.get_analysis_summary(df)
    
    print(f"\n=== Résumé Wyckoff ===")
    print(f"Phase: {summary['phase']}")
    print(f"Signal: {summary['signal']} (confiance: {summary['confidence']})")
    print(f"Support: {summary['support']:.2f}")
    print(f"Résistance: {summary['resistance']:.2f}")
    print(f"Position: {summary['position']}")
    print(f"\nÉvénements récents:")
    for e in summary['recent_events']:
        print(f"  - {e['event']}: force={e['strength']}")
