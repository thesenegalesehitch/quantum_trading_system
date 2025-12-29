# Gestionnaire de données temps réel pour le système de trading quantique
# Fournit des connexions WebSocket pour les données de marché en direct

import asyncio
import websockets
import json
import logging
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
import aiohttp
import time
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DataProvider(Enum):
    """Fournisseurs de données supportés."""
    POLYGON = "polygon"
    FINNHUB = "finnhub"
    YAHOO = "yahoo"  # Fallback

@dataclass
class RealTimeSubscription:
    """Représente une souscription temps réel."""
    symbol: str
    provider: DataProvider
    callback: Callable[[Dict], None]
    subscribed_at: datetime
    last_update: Optional[datetime] = None

class RealTimeDataManager:
    """
    Gestionnaire de données temps réel avec connexions WebSocket multiples.
    Supporte Polygon.io, Finnhub et Yahoo Finance comme fallback.
    """

    def __init__(self, polygon_key: Optional[str] = None, finnhub_key: Optional[str] = None):
        """
        Initialise le gestionnaire temps réel.

        Args:
            polygon_key: Clé API Polygon.io
            finnhub_key: Clé API Finnhub
        """
        self.polygon_key = polygon_key
        self.finnhub_key = finnhub_key

        # Souscriptions actives
        self.subscriptions: Dict[str, RealTimeSubscription] = {}

        # Connexions WebSocket
        self.websocket_connections: Dict[DataProvider, Any] = {}
        self.connection_tasks: Dict[DataProvider, asyncio.Task] = {}

        # Files d'attente pour les messages
        self.message_queues: Dict[str, asyncio.Queue] = {}

        # Statistiques
        self.stats = {
            'messages_received': 0,
            'messages_processed': 0,
            'connection_attempts': 0,
            'connection_failures': 0,
            'last_heartbeat': None
        }

        # Callbacks globaux
        self.global_callbacks: List[Callable[[Dict], None]] = []

    async def subscribe_symbol(self, symbol: str, provider: DataProvider = DataProvider.POLYGON,
                              callback: Optional[Callable[[Dict], None]] = None) -> bool:
        """
        Souscrit aux données temps réel d'un symbole.

        Args:
            symbol: Symbole à suivre
            provider: Fournisseur de données
            callback: Fonction de callback pour les nouvelles données

        Returns:
            True si souscription réussie, False sinon
        """
        try:
            if symbol in self.subscriptions:
                logger.warning(f"Déjà souscrit à {symbol}")
                return True

            # Créer la souscription
            subscription = RealTimeSubscription(
                symbol=symbol,
                provider=provider,
                callback=callback,
                subscribed_at=datetime.utcnow()
            )

            self.subscriptions[symbol] = subscription
            self.message_queues[symbol] = asyncio.Queue()

            # Démarrer la connexion si nécessaire
            if provider not in self.websocket_connections:
                await self._start_connection(provider)

            # Envoyer la commande de souscription
            await self._send_subscription_command(provider, symbol, subscribe=True)

            logger.info(f"Souscription réussie pour {symbol} via {provider.value}")
            return True

        except Exception as e:
            logger.error(f"Erreur lors de la souscription à {symbol}: {e}")
            return False

    async def unsubscribe_symbol(self, symbol: str) -> bool:
        """
        Se désabonne des données temps réel d'un symbole.

        Args:
            symbol: Symbole à arrêter de suivre

        Returns:
            True si désabonnement réussi, False sinon
        """
        try:
            if symbol not in self.subscriptions:
                return True

            provider = self.subscriptions[symbol].provider

            # Envoyer la commande de désabonnement
            await self._send_subscription_command(provider, symbol, subscribe=False)

            # Nettoyer
            del self.subscriptions[symbol]
            if symbol in self.message_queues:
                del self.message_queues[symbol]

            logger.info(f"Désabonnement réussi pour {symbol}")
            return True

        except Exception as e:
            logger.error(f"Erreur lors du désabonnement de {symbol}: {e}")
            return False

    async def _start_connection(self, provider: DataProvider):
        """Démarre une connexion WebSocket pour un fournisseur."""
        try:
            if provider in self.connection_tasks:
                return

            self.stats['connection_attempts'] += 1

            if provider == DataProvider.POLYGON:
                task = asyncio.create_task(self._polygon_connection())
            elif provider == DataProvider.FINNHUB:
                task = asyncio.create_task(self._finnhub_connection())
            else:
                logger.error(f"Fournisseur non supporté: {provider}")
                return

            self.connection_tasks[provider] = task

        except Exception as e:
            logger.error(f"Erreur démarrage connexion {provider}: {e}")
            self.stats['connection_failures'] += 1

    async def _polygon_connection(self):
        """Gère la connexion WebSocket Polygon.io."""
        if not self.polygon_key:
            logger.error("Clé API Polygon manquante")
            return

        uri = f"wss://socket.polygon.io/stocks"

        try:
            async with websockets.connect(uri) as websocket:
                self.websocket_connections[DataProvider.POLYGON] = websocket

                # Authentification
                auth_message = {
                    "action": "auth",
                    "params": self.polygon_key
                }
                await websocket.send(json.dumps(auth_message))

                # Attendre confirmation d'authentification
                response = await websocket.recv()
                auth_response = json.loads(response)

                if auth_response.get("status") != "auth_success":
                    logger.error("Échec authentification Polygon")
                    return

                logger.info("Connecté à Polygon.io")

                # Boucle principale
                async for message in websocket:
                    try:
                        await self._process_polygon_message(message)
                    except Exception as e:
                        logger.error(f"Erreur traitement message Polygon: {e}")

        except Exception as e:
            logger.error(f"Erreur connexion Polygon: {e}")
            self.stats['connection_failures'] += 1
        finally:
            if DataProvider.POLYGON in self.websocket_connections:
                del self.websocket_connections[DataProvider.POLYGON]

    async def _finnhub_connection(self):
        """Gère la connexion WebSocket Finnhub."""
        if not self.finnhub_key:
            logger.error("Clé API Finnhub manquante")
            return

        uri = f"wss://ws.finnhub.io?token={self.finnhub_key}"

        try:
            async with websockets.connect(uri) as websocket:
                self.websocket_connections[DataProvider.FINNHUB] = websocket

                logger.info("Connecté à Finnhub")

                # Boucle principale
                async for message in websocket:
                    try:
                        await self._process_finnhub_message(message)
                    except Exception as e:
                        logger.error(f"Erreur traitement message Finnhub: {e}")

        except Exception as e:
            logger.error(f"Erreur connexion Finnhub: {e}")
            self.stats['connection_failures'] += 1
        finally:
            if DataProvider.FINNHUB in self.websocket_connections:
                del self.websocket_connections[DataProvider.FINNHUB]

    async def _send_subscription_command(self, provider: DataProvider, symbol: str, subscribe: bool = True):
        """Envoie une commande de souscription/désabonnement."""
        try:
            if provider not in self.websocket_connections:
                return

            websocket = self.websocket_connections[provider]
            action = "subscribe" if subscribe else "unsubscribe"

            if provider == DataProvider.POLYGON:
                message = {
                    "action": action,
                    "params": f"T.{symbol}"  # Trades en temps réel
                }
            elif provider == DataProvider.FINNHUB:
                message = {
                    "type": action,
                    "symbol": symbol
                }
            else:
                return

            await websocket.send(json.dumps(message))
            logger.debug(f"Commande {action} envoyée pour {symbol} via {provider.value}")

        except Exception as e:
            logger.error(f"Erreur envoi commande {action} pour {symbol}: {e}")

    async def _process_polygon_message(self, message: str):
        """Traite un message reçu de Polygon.io."""
        try:
            data = json.loads(message)

            if data.get("ev") == "T":  # Trade event
                symbol = data.get("sym")
                if symbol in self.subscriptions:
                    # Convertir en format standard
                    processed_data = {
                        'symbol': symbol,
                        'price': data.get('p'),
                        'volume': data.get('s'),
                        'timestamp': data.get('t'),
                        'provider': 'polygon',
                        'type': 'trade'
                    }

                    await self._handle_new_data(symbol, processed_data)

            elif data.get("ev") == "status":
                logger.debug(f"Status Polygon: {data}")

        except Exception as e:
            logger.error(f"Erreur traitement message Polygon: {e}")

    async def _process_finnhub_message(self, message: str):
        """Traite un message reçu de Finnhub."""
        try:
            data = json.loads(message)

            if "type" in data and data["type"] == "trade":
                for trade in data.get("data", []):
                    symbol = trade.get("s")
                    if symbol in self.subscriptions:
                        # Convertir en format standard
                        processed_data = {
                            'symbol': symbol,
                            'price': trade.get('p'),
                            'volume': trade.get('v'),
                            'timestamp': trade.get('t'),
                            'provider': 'finnhub',
                            'type': 'trade'
                        }

                        await self._handle_new_data(symbol, processed_data)

        except Exception as e:
            logger.error(f"Erreur traitement message Finnhub: {e}")

    async def _handle_new_data(self, symbol: str, data: Dict):
        """Gère les nouvelles données reçues."""
        self.stats['messages_received'] += 1

        # Mettre à jour le timestamp de dernière mise à jour
        if symbol in self.subscriptions:
            self.subscriptions[symbol].last_update = datetime.utcnow()

        # Ajouter à la file d'attente
        if symbol in self.message_queues:
            await self.message_queues[symbol].put(data)

        # Appeler le callback spécifique
        if symbol in self.subscriptions and self.subscriptions[symbol].callback:
            try:
                await self.subscriptions[symbol].callback(data)
            except Exception as e:
                logger.error(f"Erreur callback pour {symbol}: {e}")

        # Appeler les callbacks globaux
        for callback in self.global_callbacks:
            try:
                await callback(data)
            except Exception as e:
                logger.error(f"Erreur callback global: {e}")

        self.stats['messages_processed'] += 1

    def add_global_callback(self, callback: Callable[[Dict], None]):
        """Ajoute un callback global pour toutes les nouvelles données."""
        self.global_callbacks.append(callback)

    def remove_global_callback(self, callback: Callable[[Dict], None]):
        """Supprime un callback global."""
        if callback in self.global_callbacks:
            self.global_callbacks.remove(callback)

    async def get_latest_data(self, symbol: str, timeout: float = 1.0) -> Optional[Dict]:
        """
        Récupère les dernières données pour un symbole.

        Args:
            symbol: Symbole
            timeout: Timeout en secondes

        Returns:
            Dernières données ou None
        """
        if symbol not in self.message_queues:
            return None

        try:
            return await asyncio.wait_for(
                self.message_queues[symbol].get(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            return None

    def get_subscription_status(self) -> Dict:
        """Retourne le statut des souscriptions actives."""
        status = {
            'active_subscriptions': len(self.subscriptions),
            'subscriptions': {},
            'connections': {},
            'stats': self.stats.copy()
        }

        # Détails des souscriptions
        for symbol, sub in self.subscriptions.items():
            status['subscriptions'][symbol] = {
                'provider': sub.provider.value,
                'subscribed_at': sub.subscribed_at.isoformat(),
                'last_update': sub.last_update.isoformat() if sub.last_update else None,
                'queue_size': self.message_queues[symbol].qsize() if symbol in self.message_queues else 0
            }

        # Statut des connexions
        for provider in DataProvider:
            status['connections'][provider.value] = {
                'connected': provider in self.websocket_connections,
                'task_running': provider in self.connection_tasks and not self.connection_tasks[provider].done()
            }

        return status

    async def shutdown(self):
        """Arrête proprement toutes les connexions."""
        logger.info("Arrêt du gestionnaire temps réel...")

        # Annuler les tâches de connexion
        for provider, task in self.connection_tasks.items():
            if not task.done():
                task.cancel()

        # Fermer les connexions WebSocket
        for provider, websocket in self.websocket_connections.items():
            try:
                await websocket.close()
            except Exception as e:
                logger.error(f"Erreur fermeture connexion {provider}: {e}")

        # Vider les files d'attente
        self.subscriptions.clear()
        self.message_queues.clear()
        self.websocket_connections.clear()
        self.connection_tasks.clear()

        logger.info("Gestionnaire temps réel arrêté")

# Instance globale
realtime_manager = RealTimeDataManager()

def get_realtime_manager() -> RealTimeDataManager:
    """Retourne l'instance globale du gestionnaire temps réel."""
    return realtime_manager