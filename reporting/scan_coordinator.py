"""
ScanCoordinator - Coordonne le scan multi-actifs pour le système de trading.
"""

from typing import Dict
from config.settings import config


class ScanCoordinator:
    """
    Coordonne le scan et l'analyse de tous les symboles actifs.
    """

    def __init__(self, system):
        """
        Initialise le coordinateur de scan.

        Args:
            system: Instance de QuantumTradingSystem
        """
        self.system = system

    def scan_all_symbols(self) -> Dict[str, Dict]:
        """
        Analyse tous les symboles configurés et retourne les résultats complets.

        Returns:
            Dict avec les résultats pour chaque symbole
        """
        from tqdm import tqdm

        symbols = config.symbols.ACTIVE_SYMBOLS
        results = {}

        print(f"\n🔍 Analyse de {len(symbols)} symboles actifs...")

        for symbol in tqdm(symbols, desc="Analyse en cours"):
            try:
                # Charger les données si nécessaire
                if symbol not in self.system.data:
                    df = self.system.load_data(symbol)
                else:
                    df = self.system.data[symbol]

                if df.empty:
                    results[symbol] = {"error": "Pas de données"}
                    continue

                # Analyser le symbole
                analysis = self.system.analyze_symbol(symbol)

                if 'error' in analysis:
                    results[symbol] = analysis
                    continue

                # Générer le setup de trade si signal valide
                trade_setup = None
                signal = analysis['combined_signal']

                if signal in ['BUY', 'SELL']:
                    trade_setup = self.system.risk_manager.create_trade_setup(
                        self.system.data[symbol],
                        symbol,
                        signal
                    )
                    trade_setup = {
                        'entry_price': trade_setup.entry_price,
                        'stop_loss': trade_setup.stop_loss,
                        'take_profits': trade_setup.take_profits
                    }

                results[symbol] = {
                    'analysis': analysis,
                    'trade_setup': trade_setup
                }

            except Exception as e:
                print(f"❌ Erreur lors de l'analyse de {symbol}: {e}")
                results[symbol] = {"error": str(e)}
                continue

        return results

    def generate_summary_report(self, results: Dict[str, Dict]) -> str:
        """
        Génère un rapport de résumé des résultats du scan.

        Args:
            results: Résultats du scan

        Returns:
            Rapport de résumé en string
        """
        total_symbols = len(results)
        successful_analyses = len([d for d in results.values() if 'analysis' in d])
        buy_signals = len([d for d in results.values() if 'analysis' in d and d['analysis']['combined_signal'] == 'BUY'])
        sell_signals = len([d for d in results.values() if 'analysis' in d and d['analysis']['combined_signal'] == 'SELL'])

        report = []
        report.append("=== RÉSUMÉ DU SCAN ===")
        report.append(f"Total symboles : {total_symbols}")
        report.append(f"Analyses réussies : {successful_analyses}")
        report.append(f"Signaux d'achat (BUY) : {buy_signals}")
        report.append(f"Signaux de vente (SELL) : {sell_signals}")

        return "\n".join(report)