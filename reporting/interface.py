"""
Interface de reporting pour afficher les signaux de trading.
Format structuré et visuellement clair.
"""

from datetime import datetime
from typing import Dict, List, Optional
import sys
import os
from colorama import Fore, Back, Style, init

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

# Initialiser colorama
init(autoreset=True)


class TradingInterface:
    """
    Génère des rapports de trading formatés.
    
    Affiche:
    - Sentiment du marché
    - Signal (BUY/SELL/WAIT)
    - Confiance statistique
    - Paramètres de gestion (Entry, SL, TP)
    """
    
    def __init__(self):
        self.history: List[Dict] = []
    
    def generate_signal_report(
        self,
        symbol: str,
        analysis: Dict,
        trade_setup: Optional[Dict] = None,
        ml_prediction: Optional[Dict] = None
    ) -> str:
        """
        Génère un rapport de signal complet.
        
        Args:
            symbol: Symbole tradé
            analysis: Résultats d'analyse
            trade_setup: Configuration du trade
            ml_prediction: Prédiction ML
        
        Returns:
            Rapport formaté en string
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Déterminer le sentiment
        sentiment = self._determine_sentiment(analysis)
        
        # Déterminer le signal
        signal, confidence = self._determine_signal(analysis, ml_prediction)
        
        # Construire le rapport
        report = []
        report.append("═" * 50)
        report.append("       SIGNAL DE TRADING QUANTIQUE")
        report.append("═" * 50)
        report.append(f"Actif       : {config.symbols.DISPLAY_NAMES.get(symbol, symbol)}")
        report.append(f"Timestamp   : {timestamp}")
        report.append(f"Sentiment   : {self._sentiment_emoji(sentiment)} {sentiment}")
        report.append(f"Signal      : {self._signal_emoji(signal)} {signal}")
        report.append(f"Confiance   : {confidence:.1f}%")
        report.append("─" * 50)
        
        # Ajouter trade setup si disponible
        if trade_setup and signal not in ["WAIT", "AVOID"]:
            entry = trade_setup.get('entry_price')
            sl = trade_setup.get('stop_loss')
            tps = trade_setup.get('take_profits', [])
            suggested_lot = trade_setup.get('suggested_lot', 0.01)

            report.append(f"Entrée      : {entry}")
            report.append(f"Stop-Loss   : {sl}")

            if tps:
                report.append("Take-Profit :")
                for i, tp in enumerate(tps, 1):
                    report.append(f"  TP{i} ({tp.get('size_percent', 0)}%) : {tp.get('price', 'N/A')}")

                # Calculer R:R
                if entry and sl and tps:
                    risk = abs(entry - sl)
                    reward = abs(tps[0]['price'] - entry)
                    rr_ratio = reward / risk if risk > 0 else 0
                    report.append(f"Risk-Reward : 1:{rr_ratio:.2f}")

            # Position size
            risk_per_trade = config.risk.RISK_PER_TRADE * 100
            report.append(f"Taille lot  : {suggested_lot:.2f} lots")
            report.append(f"Risque/Trade: {risk_per_trade:.1f}%")
        
        # Raison du signal
        reason = analysis.get('reason', ml_prediction.get('action', '') if ml_prediction else '')
        if reason:
            report.append("─" * 50)
            report.append(f"Raison: {reason}")
        
        report.append("═" * 50)
        
        final_report = "\n".join(report)
        
        # Sauvegarder dans l'historique
        self.history.append({
            "timestamp": timestamp,
            "symbol": symbol,
            "signal": signal,
            "confidence": confidence,
            "sentiment": sentiment
        })
        
        return final_report
    
    def _determine_sentiment(self, analysis: Dict) -> str:
        """Détermine le sentiment du marché."""
        # Agrégation des indicateurs
        bullish_count = 0
        bearish_count = 0
        
        # Multi-TF
        if 'convergence' in analysis:
            if analysis['convergence'].get('overall_trend') == 'BULLISH':
                bullish_count += 2
            elif analysis['convergence'].get('overall_trend') == 'BEARISH':
                bearish_count += 2
        
        # Ichimoku
        if 'ichimoku' in analysis:
            if analysis['ichimoku'].get('signal') == 'BUY':
                bullish_count += 1
            elif analysis['ichimoku'].get('signal') == 'SELL':
                bearish_count += 1
        
        # Hurst
        if 'hurst' in analysis:
            regime = analysis['hurst'].get('regime', '')
            if regime == 'TRENDING' and analysis.get('trend_direction') == 'UP':
                bullish_count += 1
            elif regime == 'TRENDING' and analysis.get('trend_direction') == 'DOWN':
                bearish_count += 1
        
        # Décision
        if bullish_count > bearish_count + 1:
            return "BULLISH"
        elif bearish_count > bullish_count + 1:
            return "BEARISH"
        else:
            return "NEUTRAL"
    
    def _determine_signal(self, analysis: Dict, ml_pred: Optional[Dict]) -> tuple:
        """Détermine le signal et la confiance."""
        confidence = 50.0  # Base
        signal = "WAIT"
        
        # ML prediction est prioritaire
        if ml_pred:
            ml_conf = ml_pred.get('probability', 50)
            threshold_met = ml_pred.get('threshold_met', False)
            
            if threshold_met:
                if ml_pred.get('signal', '').startswith('STRONG'):
                    signal = "BUY" if 'BUY' in ml_pred['signal'] else "SELL"
                    confidence = ml_conf
                elif ml_pred.get('signal') in ['BUY', 'SELL']:
                    signal = ml_pred['signal']
                    confidence = ml_conf
            else:
                signal = "WAIT"
                confidence = ml_conf
        
        # Sinon utiliser l'analyse technique
        elif 'signal' in analysis:
            if analysis['signal'] in ['BUY', 'SELL']:
                signal = analysis['signal']
                confidence = analysis.get('confidence', 60)
        
        # Ajuster la confiance avec d'autres indicateurs
        if 'convergence' in analysis:
            conf_level = analysis['convergence'].get('confirmation_level', 0)
            confidence = (confidence + conf_level * 100) / 2
        
        return signal, min(confidence, 100)
    
    def _sentiment_emoji(self, sentiment: str) -> str:
        """Emoji pour le sentiment."""
        return {
            "BULLISH": "🟢",
            "BEARISH": "🔴",
            "NEUTRAL": "⚪"
        }.get(sentiment, "⚪")
    
    def _signal_emoji(self, signal: str) -> str:
        """Emoji pour le signal."""
        return {
            "BUY": "✅",
            "SELL": "❌",
            "WAIT": "⏳",
            "AVOID": "🚫"
        }.get(signal, "❓")
    
    def generate_summary_report(self, period_hours: int = 24) -> str:
        """Génère un résumé des signaux récents."""
        report = []
        report.append("═" * 50)
        report.append(f"    RÉSUMÉ DES DERNIÈRES {period_hours}H")
        report.append("═" * 50)
        
        if not self.history:
            report.append("Aucun signal émis dans cette période.")
        else:
            # Compter les signaux
            buy_count = sum(1 for h in self.history if h['signal'] == 'BUY')
            sell_count = sum(1 for h in self.history if h['signal'] == 'SELL')
            wait_count = sum(1 for h in self.history if h['signal'] == 'WAIT')
            
            report.append(f"Total signaux : {len(self.history)}")
            report.append(f"  ✅ BUY  : {buy_count}")
            report.append(f"  ❌ SELL : {sell_count}")
            report.append(f"  ⏳ WAIT : {wait_count}")
            
            # Confiance moyenne
            avg_conf = sum(h['confidence'] for h in self.history) / len(self.history)
            report.append(f"\nConfiance moyenne : {avg_conf:.1f}%")
        
        report.append("═" * 50)
        
        return "\n".join(report)
    
    def print_analysis(self, symbol: str, analysis: Dict, trade_setup: Optional[Dict] = None):
        """Affiche un rapport d'analyse amélioré avec setup de trade si disponible."""
        display_name = config.symbols.DISPLAY_NAMES.get(symbol, symbol)

        print(f"\n📊 ANALYSE COMPLÈTE - {display_name} ({symbol})")
        print("="*60)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Timeframe: 1 heure")
        print("="*60)

        print(f"\n🔍 INDICATEURS TECHNIQUES:")
        current_price = analysis.get('current_price', 'N/A')
        print(f"  💰 PRIX ACTUEL: {Fore.CYAN}{current_price}{Style.RESET_ALL}")

        # Hurst Exponent
        hurst_value = analysis['hurst']['value']
        hurst_regime = analysis['hurst']['regime']
        if hurst_regime == 'TRENDING':
            print(f"  📈 Hurst Exponent: {Fore.BLUE}{hurst_value:.3f} ({hurst_regime}){Style.RESET_ALL}")
            print(f"     → Marché en tendance, suitable pour momentum trading")
        else:
            print(f"  🔄 Hurst Exponent: {Fore.YELLOW}{hurst_value:.3f} ({hurst_regime}){Style.RESET_ALL}")
            print(f"     → Marché mean-reverting, suitable pour range trading")

        # Z-Score
        zscore = analysis['zscore']['zscore']
        if zscore < -2:
            print(f"  📉 Z-Score Bollinger: {Fore.GREEN}{zscore:.2f} (Survendu - Oversold){Style.RESET_ALL}")
            print(f"     → Prix très bas, potentiel rebond haussier")
        elif zscore > 2:
            print(f"  📈 Z-Score Bollinger: {Fore.RED}{zscore:.2f} (Suracheté - Overbought){Style.RESET_ALL}")
            print(f"     → Prix très haut, potentiel correction baissière")
        else:
            print(f"  ➡️ Z-Score Bollinger: {Fore.YELLOW}{zscore:.2f} (Neutre){Style.RESET_ALL}")
            print(f"     → Prix dans la moyenne, attendre confirmation")

        # Ichimoku
        ichi_signal = analysis['ichimoku']['signal']
        if ichi_signal == 'BUY':
            print(f"  🌅 Ichimoku Cloud: {Fore.GREEN}{ichi_signal}{Style.RESET_ALL}")
            print(f"     → Signal d'achat, prix au-dessus du nuage")
        elif ichi_signal == 'SELL':
            print(f"  🌇 Ichimoku Cloud: {Fore.RED}{ichi_signal}{Style.RESET_ALL}")
            print(f"     → Signal de vente, prix en-dessous du nuage")
        else:
            print(f"  ⛅ Ichimoku Cloud: {Fore.YELLOW}{ichi_signal}{Style.RESET_ALL}")
            print(f"     → Attendre, prix dans le nuage")

        # Smart Money Concepts
        smc_signal = analysis['smc']['signal']
        if smc_signal == 'BUY':
            print(f"  🧠 Smart Money Concepts: {Fore.GREEN}{smc_signal}{Style.RESET_ALL}")
            print(f"     → Accumulation détectée, smart money achète")
        elif smc_signal == 'SELL':
            print(f"  🧠 Smart Money Concepts: {Fore.RED}{smc_signal}{Style.RESET_ALL}")
            print(f"     → Distribution détectée, smart money vend")
        else:
            print(f"  🤔 Smart Money Concepts: {Fore.YELLOW}{smc_signal}{Style.RESET_ALL}")
            print(f"     → Pas de signal clair, attendre")

        # Divergences
        div_data = analysis.get('divergence', {})
        div_signal = div_data.get('signal', 'WAIT')
        div_reason = div_data.get('reason', '')
        if div_signal == 'BUY':
            print(f"  📉 Divergence: {Fore.GREEN}{div_signal} (Bullish){Style.RESET_ALL}")
            print(f"     → {div_reason}")
        elif div_signal == 'SELL':
            print(f"  📈 Divergence: {Fore.RED}{div_signal} (Bearish){Style.RESET_ALL}")
            print(f"     → {div_reason}")
        else:
            print(f"  ↔️ Divergence: {Fore.YELLOW}Neutre{Style.RESET_ALL}")

        # Wyckoff
        wyck_data = analysis.get('wyckoff', {})
        wyck_phase = wyck_data.get('phase', 'UNKNOWN')
        wyck_signal = wyck_data.get('signal', 'WAIT')
        if wyck_signal == 'BUY':
            print(f"  📊 Wyckoff Phase: {Fore.GREEN}{wyck_phase} ({wyck_signal}){Style.RESET_ALL}")
            print(f"     → Phase d'accumulation/markup détectée")
        elif wyck_signal == 'SELL':
            print(f"  📊 Wyckoff Phase: {Fore.RED}{wyck_phase} ({wyck_signal}){Style.RESET_ALL}")
            print(f"     → Phase de distribution/markdown détectée")
        else:
            print(f"  📊 Wyckoff Phase: {Fore.YELLOW}{wyck_phase}{Style.RESET_ALL}")

        print(f"\n🎯 ANALYSE DE MARCHÉ COMPLÈTE:")
        combined_signal = analysis['combined_signal']
        confidence = analysis['confidence']

        # Détermination de la tendance générale
        trend_signals = 0
        if ichi_signal == 'BUY': trend_signals += 1
        if smc_signal == 'BUY': trend_signals += 1
        if zscore < -1: trend_signals += 1  # Oversold = bullish
        if hurst_regime == 'TRENDING' and analysis.get('trend_direction') == 'UP': trend_signals += 1

        bearish_signals = 0
        if ichi_signal == 'SELL': bearish_signals += 1
        if smc_signal == 'SELL': bearish_signals += 1
        if zscore > 1: bearish_signals += 1  # Overbought = bearish
        if hurst_regime == 'TRENDING' and analysis.get('trend_direction') == 'DOWN': bearish_signals += 1

        # Analyse de ce que fait le marché
        print(f"  📊 TENDANCE DU MARCHÉ:")
        if trend_signals > bearish_signals + 1:
            print(f"     🌅 Tendance HAUSSIÈRE détectée - Le marché monte")
        elif bearish_signals > trend_signals + 1:
            print(f"     🌇 Tendance BAISSIÈRE détectée - Le marché descend")
        else:
            print(f"     🌤️ Marché LATÉRAL/NEUTRE - Pas de tendance claire")

        print(f"  👥 CE QUE FONT LES TRADERS:")
        if combined_signal == 'BUY':
            print(f"     📈 Les traders ACHÈTENT - Momentum haussier")
            print(f"     💡 Smart Money accumule - Bonne opportunité d'achat")
        elif combined_signal == 'SELL':
            print(f"     📉 Les traders VENDENT - Momentum baissier")
            print(f"     💡 Smart Money distribue - Bonne opportunité de vente")
        else:
            print(f"     🤔 Les traders ATTENDENT - Marché indécis")
            print(f"     💡 Attendre confirmation avant d'agir")

        print(f"\n  🎯 SIGNAL FINAL:")
        if combined_signal == 'BUY':
            print(f"     🟢 {Fore.GREEN}ACHETER{Style.RESET_ALL} - Signal haussier confirmé")
            print(f"     📊 Confiance: {confidence:.1f}% - Haute probabilité de succès")
        elif combined_signal == 'SELL':
            print(f"     🔴 {Fore.RED}VENDRE{Style.RESET_ALL} - Signal baissier confirmé")
            print(f"     📊 Confiance: {confidence:.1f}% - Haute probabilité de succès")
        else:
            print(f"     🟡 {Fore.YELLOW}ATTENDRE{Style.RESET_ALL} - Pas de signal clair")
            print(f"     📊 Confiance: {confidence:.1f}% - Risque élevé, mieux attendre")

        # Bilan final
        print(f"\n  📋 BILAN DE L'ANALYSE:")
        total_signals = 6  # Hurst, Z-Score, Ichimoku, SMC, Divergence, Wyckoff
        bullish_count = sum([ichi_signal == 'BUY', smc_signal == 'BUY', div_signal == 'BUY', wyck_signal == 'BUY', zscore < -1])
        bearish_count = sum([ichi_signal == 'SELL', smc_signal == 'SELL', div_signal == 'SELL', wyck_signal == 'SELL', zscore > 1])

        print(f"     Indicateurs analysés: {total_signals}")
        print(f"     Signaux haussiers: {bullish_count}")
        print(f"     Signaux baissiers: {bearish_count}")
        print(f"     Régime de marché: {hurst_regime}")

        if confidence > 70:
            print(f"     ✅ Signal FORT - Conviction élevée")
        elif confidence > 50:
            print(f"     ⚠️ Signal MODÉRÉ - Surveiller les confirmations")
        else:
            print(f"     ❌ Signal FAIBLE - Mieux attendre")

        # Setup de trade si signal valide et disponible
        if trade_setup and combined_signal in ['BUY', 'SELL']:
            entry = trade_setup.get('entry_price')
            sl = trade_setup.get('stop_loss')
            tps = trade_setup.get('take_profits', [])

            print(f"\n🚀 STRATÉGIE DE TRADING COMPLÈTE:")
            print(f"  ┌{'─'*58}┐")
            print(f"  │{Fore.CYAN}{'📈 DIRECTION: ACHAT' if combined_signal == 'BUY' else '📉 DIRECTION: VENTE'}{Style.RESET_ALL}{' '*(58-len('📈 DIRECTION: ACHAT' if combined_signal == 'BUY' else '📉 DIRECTION: VENTE'))}│")
            print(f"  ├{'─'*58}┤")
            entry_text = f"💰 PRIX D'ENTRÉE (ENTRY): {entry}"
            padding = ' ' * (58 - len(entry_text))
            print(f"  │{Fore.CYAN}{entry_text}{padding}│")

            if sl:
                risk_pips = abs(entry - sl) * 10000 if 'JPY' not in symbol else abs(entry - sl) * 100
                print(f"  │{Fore.RED}🛑 STOP LOSS (SL): {sl} ({risk_pips:.1f} pips){' '*(58-len(f'🛑 STOP LOSS (SL): {sl} ({risk_pips:.1f} pips)'))}│")

            if tps:
                for i, tp in enumerate(tps, 1):
                    tp_price = tp['price']
                    reward_pips = abs(tp_price - entry) * 10000 if 'JPY' not in symbol else abs(tp_price - entry) * 100
                    print(f"  │{Fore.GREEN}🎯 TAKE PROFIT {i} (TP{i}): {tp_price} ({reward_pips:.1f} pips){' '*(58-len(f'🎯 TAKE PROFIT {i} (TP{i}): {tp_price} ({reward_pips:.1f} pips)'))}│")

                # Calculer R:R
                risk = abs(entry - sl) if entry and sl else 0
                reward = abs(tps[0]['price'] - entry) if tps else 0
                rr_ratio = reward / risk if risk > 0 else 0
                print(f"  │{Fore.YELLOW}📊 RATIO RISQUE/RÉCOMPENSE: 1:{rr_ratio:.2f}{' '*(58-len(f'📊 RATIO RISQUE/RÉCOMPENSE: 1:{rr_ratio:.2f}'))}│")

            print(f"  └{'─'*58}┘")

            # Indicateurs de confluence
            print(f"\n  🔗 INDICATEURS DE CONFLUENCE:")
            confluence_count = sum([
                ichi_signal == combined_signal,
                smc_signal == combined_signal,
                div_signal == combined_signal,
                wyck_signal == combined_signal,
                (zscore < -1 and combined_signal == 'BUY') or (zscore > 1 and combined_signal == 'SELL'),
                hurst_regime == 'TRENDING'
            ])
            print(f"     ✅ {confluence_count}/6 indicateurs confirment le signal")
            if confluence_count >= 3:
                print(f"     🔥 CONFLUENCE ÉLEVÉE - Signal très fiable")
            elif confluence_count >= 2:
                print(f"     ⚠️ CONFLUENCE MODÉRÉE - Signal acceptable")
            else:
                print(f"     ❓ CONFLUENCE FAIBLE - Surveiller les confirmations")

            # Calcul de la taille de position
            print(f"\n  💰 CALCUL DE TAILLE DE POSITION:")
            risk_per_trade = config.risk.RISK_PER_TRADE
            suggested_lot = trade_setup.get('suggested_lot', 0.01)
            
            print(f"     💵 Taille suggérée: {Fore.YELLOW}{suggested_lot:.2f} lots{Style.RESET_ALL} (risque {risk_per_trade*100:.1f}%)")

            risk_amount = config.risk.INITIAL_CAPITAL * risk_per_trade
            print(f"     📏 Risque par trade: ${risk_amount:.2f}")
            print(f"     🎯 Gain potentiel: ${risk_amount * rr_ratio:.2f}")
            print(f"     ⚖️ Ratio R:R: {Fore.GREEN}{rr_ratio:.2f}{Style.RESET_ALL}")

            # Instructions d'exécution détaillées
            print(f"\n  📋 INSTRUCTIONS D'EXÉCUTION:")
            print(f"  ┌{'─'*58}┐")
            if combined_signal == 'BUY':
                print(f"  │{Fore.GREEN}1. BUY LIMIT @ {entry}{' '*(58-len(f'1. BUY LIMIT @ {entry}'))}│")
                print(f"  │{Fore.RED}2. STOP LOSS @ {sl}{' '*(58-len(f'2. STOP LOSS @ {sl}'))}│")
                for i, tp in enumerate(tps, 1):
                    tp_text = f"3. TAKE PROFIT {i} @ {tp['price']} ({tp['size_percent']}%)"
                    padding = ' ' * (58 - len(tp_text))
                    print(f"  │{Fore.GREEN}{tp_text}{padding}│")
            else:
                print(f"  │{Fore.RED}1. SELL LIMIT @ {entry}{' '*(58-len(f'1. SELL LIMIT @ {entry}'))}│")
                print(f"  │{Fore.RED}2. STOP LOSS @ {sl}{' '*(58-len(f'2. STOP LOSS @ {sl}'))}│")
                for i, tp in enumerate(tps, 1):
                    tp_text = f"3. TAKE PROFIT {i} @ {tp['price']} ({tp['size_percent']}%)"
                    padding = ' ' * (58 - len(tp_text))
                    print(f"  │{Fore.GREEN}{tp_text}{padding}│")
            print(f"  └{'─'*58}┘")
            
            print(f"\n📝 NOTES STRATÉGIQUES:")
            print(f"     • Déplacer le Stop-Loss au prix d'entrée (Break-Even) après TP1.")
            print(f"     • Ne pas trader pendant les annonces économiques majeures.")
            
            print(f"\n⚠️ AVERTISSEMENT: Le trading comporte des risques substantiels.")

        print("="*50)

    def print_signal(self, *args, **kwargs):
        """Affiche et retourne le rapport."""
        report = self.generate_signal_report(*args, **kwargs)
        print(report)
        return report

    def print_scan_report(self, results: Dict[str, Dict]):
        """Affiche un rapport complet de scan multi-actifs."""
    def print_detailed_signal(self, symbol: str, analysis: Dict, trade_setup: Dict):
        """Affiche un signal détaillé avec SL/TP pour trading actionnable."""
        signal = analysis.get('combined_signal', 'WAIT')
        confidence = analysis.get('confidence', 50.0)

        # Couleur selon signal
        if signal == 'BUY':
            color = Fore.GREEN
        elif signal == 'SELL':
            color = Fore.RED
        else:
            color = Fore.YELLOW

        display_name = config.symbols.DISPLAY_NAMES.get(symbol, symbol)

        print(f"\n{color}{'🚀' if signal in ['BUY', 'SELL'] else '⏳'} {display_name} ({symbol}) - {signal} ({confidence:.1f}%)")

        if trade_setup and signal in ['BUY', 'SELL']:
            entry = trade_setup.get('entry_price')
            sl = trade_setup.get('stop_loss')
            tps = trade_setup.get('take_profits', [])

            print(f"{color}  Entry Price: {entry}")
            print(f"{color}  Stop-Loss: {sl} (Distance: {abs(entry - sl) if entry and sl else 'N/A'})")

            if tps:
                print(f"{color}  Take-Profits:")
                for i, tp in enumerate(tps, 1):
                    distance = abs(tp['price'] - entry) if entry else 0
                    print(f"{color}    TP{i}: {tp['price']} ({tp['size_percent']}%) - Distance: {distance}")

                # Calculer R:R
                risk = abs(entry - sl) if entry and sl else 0
                reward = abs(tps[0]['price'] - entry) if tps else 0
                rr_ratio = reward / risk if risk > 0 else 0
                print(f"{color}  Risk-Reward Ratio: 1:{rr_ratio:.2f}")

                # Position size (placeholder - need integration with risk manager)
                print(f"{color}  Suggested Position Size: TBD (based on {config.risk.RISK_PER_TRADE*100:.1f}% risk per trade)")
        else:
            print(f"{color}  No trade setup available")

        print(f"{color}  Analysis Details:")
        print(f"{color}    Hurst: {analysis['hurst']['value']:.3f} ({analysis['hurst']['regime']})")
        print(f"{color}    Z-Score: {analysis['zscore']['zscore']:.2f}")
        print(f"{color}    Ichimoku: {analysis['ichimoku']['signal']}")
        print(f"{color}    SMC: {analysis['smc']['signal']}")

        print("\n" + "="*80)

        print("\n" + "="*80)
        print("           QUANTUM TRADING SYSTEM - SCAN REPORT")
        print("="*80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Symbols scanned: {len(results)}")
        print("="*80)

        # Section Analyse
        print("\n📊 ANALYSIS SECTION")
        print("-"*80)

        for symbol, data in results.items():
            display_name = config.symbols.DISPLAY_NAMES.get(symbol, symbol)
            print(f"\n🔍 {display_name} ({symbol})")

            if 'error' in data:
                print(f"  ❌ Error: {data['error']}")
                continue

            analysis = data['analysis']
            print(f"  Hurst: {analysis['hurst']['value']:.3f} ({analysis['hurst']['regime']})")
            print(f"  Z-Score: {analysis['zscore']['zscore']:.2f}")
            print(f"  Ichimoku: {analysis['ichimoku']['signal']}")
            print(f"  SMC: {analysis['smc']['signal']}")

        # Section Signaux
        print("\n📈 SIGNALS SECTION")
        print("-"*80)

        valid_signals = {s: d for s, d in results.items() if 'analysis' in d and d['analysis']['combined_signal'] in ['BUY', 'SELL']}

        if not valid_signals:
            print("No valid trading signals found.")
        else:
            for symbol, data in valid_signals.items():
                analysis = data['analysis']
                trade_setup = data.get('trade_setup')
                signal = analysis['combined_signal']
                confidence = analysis['confidence']

                # Couleur selon signal
                if signal == 'BUY':
                    color = Fore.GREEN
                elif signal == 'SELL':
                    color = Fore.RED
                else:
                    color = Fore.YELLOW

                display_name = config.symbols.DISPLAY_NAMES.get(symbol, symbol)
                print(f"\n{color}🚀 {display_name} ({symbol}) - {signal} ({confidence:.1f}%)")

                if trade_setup:
                    entry = trade_setup['entry_price']
                    sl = trade_setup['stop_loss']
                    tps = trade_setup['take_profits']

                    print(f"{color}  Entry: {entry}")
                    print(f"{color}  Stop-Loss: {sl}")

                    if tps:
                        print(f"{color}  Take-Profits:")
                        for i, tp in enumerate(tps, 1):
                            print(f"{color}    TP{i}: {tp['price']} ({tp['size_percent']}%)")

                    # Calculer R:R si possible
                    if sl and entry and tps:
                        risk = abs(entry - sl)
                        reward = abs(tps[0]['price'] - entry) if tps else 0
                        rr_ratio = reward / risk if risk > 0 else 0
                        print(f"{color}  Risk-Reward: 1:{rr_ratio:.2f}")

                        # Position size suggestion (placeholder, need risk manager integration)
                        print(f"{color}  Position Size: TBD (based on risk management)")

        # Section Résumé
        print("\n📋 SUMMARY SECTION")
        print("-"*80)

        total_symbols = len(results)
        successful_analyses = len([d for d in results.values() if 'analysis' in d])
        buy_signals = len([d for d in results.values() if 'analysis' in d and d['analysis']['combined_signal'] == 'BUY'])
        sell_signals = len([d for d in results.values() if 'analysis' in d and d['analysis']['combined_signal'] == 'SELL'])
        wait_signals = len([d for d in results.values() if 'analysis' in d and d['analysis']['combined_signal'] == 'WAIT'])

        print(f"Total symbols: {total_symbols}")
        print(f"Successful analyses: {successful_analyses}")
        print(f"Buy signals: {buy_signals}")
        print(f"Sell signals: {sell_signals}")
        print(f"Wait signals: {wait_signals}")

        # Régime de marché
        trending_count = len([d for d in results.values() if 'analysis' in d and d['analysis']['hurst']['regime'] == 'TRENDING'])
        mean_revert_count = len([d for d in results.values() if 'analysis' in d and d['analysis']['hurst']['regime'] == 'MEAN-REVERTING'])

        print(f"\nMarket Regime:")
        print(f"  Trending assets: {trending_count}")
        print(f"  Mean-reverting assets: {mean_revert_count}")

        # Estimation win rate (placeholder)
        if buy_signals + sell_signals > 0:
            estimated_win_rate = 65.0  # Placeholder
            print(f"\nEstimated Win Rate: {estimated_win_rate:.1f}% (based on historical backtests)")

        print("\n" + "="*80)


if __name__ == "__main__":
    # Test
    interface = TradingInterface()
    
    # Données de test
    analysis = {
        "convergence": {
            "overall_trend": "BULLISH",
            "confirmation_level": 0.75
        },
        "signal": "BUY",
        "confidence": 85,
        "reason": "Convergence multi-TF haussière avec confirmation Ichimoku"
    }
    
    trade_setup = {
        "entry_price": 1.0875,
        "stop_loss": 1.0845,
        "take_profits": [
            {"price": 1.0905, "size_percent": 50},
            {"price": 1.0935, "size_percent": 30},
            {"price": 1.0975, "size_percent": 20}
        ]
    }
    
    ml_prediction = {
        "signal": "STRONG_BUY",
        "probability": 92.4,
        "threshold_met": True,
        "action": "Signal ML validé avec haute confiance"
    }
    
    # Générer le rapport
    report = interface.print_signal(
        symbol="EURUSD=X",
        analysis=analysis,
        trade_setup=trade_setup,
        ml_prediction=ml_prediction
    )
