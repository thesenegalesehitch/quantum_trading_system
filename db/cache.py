# Système de mise en cache Redis pour le système de trading quantique
# Fournit une mise en cache intelligente avec invalidation automatique

import redis
import json
import pickle
from typing import Any, Optional, Dict, List
import logging
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)

class RedisCache:
    """
    Gestionnaire de cache Redis avec invalidation intelligente.
    Implémente une stratégie de cache à plusieurs niveaux pour optimiser les performances.
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0,
                 password: Optional[str] = None, decode_responses: bool = False):
        """
        Initialise la connexion Redis.

        Args:
            host: Hôte Redis
            port: Port Redis
            db: Numéro de base de données
            password: Mot de passe Redis (optionnel)
            decode_responses: Décoder automatiquement les réponses en chaînes
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.decode_responses = decode_responses

        self.client = None
        self._connect()

    def _connect(self):
        """Établit la connexion Redis."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=self.decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            # Test de connexion
            self.client.ping()
            logger.info("Connexion Redis établie avec succès")
        except redis.ConnectionError as e:
            logger.error(f"Impossible de se connecter à Redis: {e}")
            self.client = None

    def is_connected(self) -> bool:
        """Vérifie si la connexion Redis est active."""
        if not self.client:
            return False
        try:
            self.client.ping()
            return True
        except:
            return False

    def _get_cache_key(self, prefix: str, key: str, **kwargs) -> str:
        """
        Génère une clé de cache normalisée.

        Args:
            prefix: Préfixe de la clé (ex: 'market_data')
            key: Clé principale
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            Clé de cache formatée
        """
        if kwargs:
            # Trier les kwargs pour une cohérence
            sorted_kwargs = '&'.join(f"{k}={v}" for k, v in sorted(kwargs.items()))
            full_key = f"{prefix}:{key}:{sorted_kwargs}"
        else:
            full_key = f"{prefix}:{key}"

        # Hash si la clé est trop longue
        if len(full_key) > 250:
            full_key = f"{prefix}:{hashlib.md5(full_key.encode()).hexdigest()}"

        return full_key

    def set(self, prefix: str, key: str, value: Any, ttl: Optional[int] = None, **kwargs) -> bool:
        """
        Stocke une valeur dans le cache.

        Args:
            prefix: Préfixe de la clé
            key: Clé principale
            value: Valeur à stocker
            ttl: Durée de vie en secondes (optionnel)
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            True si succès, False sinon
        """
        if not self.is_connected():
            return False

        try:
            cache_key = self._get_cache_key(prefix, key, **kwargs)

            # Sérialiser selon le type
            if isinstance(value, (dict, list)):
                serialized = json.dumps(value)
                self.client.set(cache_key, serialized, ex=ttl)
            elif isinstance(value, (int, float, str, bool)):
                self.client.set(cache_key, str(value), ex=ttl)
            else:
                # Objets complexes avec pickle
                serialized = pickle.dumps(value)
                self.client.set(cache_key + ':pickle', serialized, ex=ttl)

            return True
        except Exception as e:
            logger.error(f"Erreur lors de la mise en cache: {e}")
            return False

    def get(self, prefix: str, key: str, **kwargs) -> Optional[Any]:
        """
        Récupère une valeur du cache.

        Args:
            prefix: Préfixe de la clé
            key: Clé principale
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            Valeur récupérée ou None si non trouvée
        """
        if not self.is_connected():
            return None

        try:
            cache_key = self._get_cache_key(prefix, key, **kwargs)

            # Essayer d'abord JSON
            value = self.client.get(cache_key)
            if value is not None:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value

            # Essayer pickle
            pickle_value = self.client.get(cache_key + ':pickle')
            if pickle_value is not None:
                return pickle.loads(pickle_value)

            return None
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du cache: {e}")
            return None

    def delete(self, prefix: str, key: str, **kwargs) -> bool:
        """
        Supprime une valeur du cache.

        Args:
            prefix: Préfixe de la clé
            key: Clé principale
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            True si supprimé, False sinon
        """
        if not self.is_connected():
            return False

        try:
            cache_key = self._get_cache_key(prefix, key, **kwargs)
            result = self.client.delete(cache_key)
            # Supprimer aussi la version pickle si elle existe
            self.client.delete(cache_key + ':pickle')
            return result > 0
        except Exception as e:
            logger.error(f"Erreur lors de la suppression du cache: {e}")
            return False

    def exists(self, prefix: str, key: str, **kwargs) -> bool:
        """
        Vérifie si une clé existe dans le cache.

        Args:
            prefix: Préfixe de la clé
            key: Clé principale
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            True si la clé existe, False sinon
        """
        if not self.is_connected():
            return False

        try:
            cache_key = self._get_cache_key(prefix, key, **kwargs)
            return self.client.exists(cache_key) > 0
        except Exception as e:
            logger.error(f"Erreur lors de la vérification du cache: {e}")
            return False

    def clear_prefix(self, prefix: str) -> int:
        """
        Supprime toutes les clés avec un préfixe donné.

        Args:
            prefix: Préfixe à supprimer

        Returns:
            Nombre de clés supprimées
        """
        if not self.is_connected():
            return 0

        try:
            # Utiliser SCAN pour trouver toutes les clés avec le préfixe
            keys_to_delete = []
            cursor = 0
            while True:
                cursor, keys = self.client.scan(cursor, f"{prefix}:*")
                keys_to_delete.extend(keys)
                if cursor == 0:
                    break

            if keys_to_delete:
                return self.client.delete(*keys_to_delete)
            return 0
        except Exception as e:
            logger.error(f"Erreur lors de la suppression du préfixe: {e}")
            return 0

    def get_ttl(self, prefix: str, key: str, **kwargs) -> int:
        """
        Récupère le TTL restant d'une clé.

        Args:
            prefix: Préfixe de la clé
            key: Clé principale
            **kwargs: Paramètres supplémentaires pour la clé

        Returns:
            TTL en secondes, -2 si clé inexistante, -1 si pas d'expiration
        """
        if not self.is_connected():
            return -2

        try:
            cache_key = self._get_cache_key(prefix, key, **kwargs)
            return self.client.ttl(cache_key)
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du TTL: {e}")
            return -2

    # Méthodes spécialisées pour le trading

    def cache_market_data(self, symbol: str, interval: str, data: Dict, ttl: int = 300) -> bool:
        """
        Met en cache les données de marché.

        Args:
            symbol: Symbole
            interval: Intervalle ('1d', '1h', etc.)
            data: Données OHLCV
            ttl: TTL en secondes (défaut 5 minutes)

        Returns:
            True si mis en cache, False sinon
        """
        return self.set('market_data', symbol, data, ttl=ttl, interval=interval)

    def get_market_data(self, symbol: str, interval: str) -> Optional[Dict]:
        """
        Récupère les données de marché du cache.

        Args:
            symbol: Symbole
            interval: Intervalle

        Returns:
            Données de marché ou None
        """
        return self.get('market_data', symbol, interval=interval)

    def cache_signals(self, symbol: str, signals: List[Dict], ttl: int = 60) -> bool:
        """
        Met en cache les signaux de trading.

        Args:
            symbol: Symbole
            signals: Liste des signaux
            ttl: TTL en secondes (défaut 1 minute)

        Returns:
            True si mis en cache, False sinon
        """
        return self.set('signals', symbol, signals, ttl=ttl)

    def get_signals(self, symbol: str) -> Optional[List[Dict]]:
        """
        Récupère les signaux du cache.

        Args:
            symbol: Symbole

        Returns:
            Signaux ou None
        """
        return self.get('signals', symbol)

    def cache_correlations(self, correlations: Dict, ttl: int = 3600) -> bool:
        """
        Met en cache la matrice de corrélations.

        Args:
            correlations: Matrice de corrélations
            ttl: TTL en secondes (défaut 1 heure)

        Returns:
            True si mis en cache, False sinon
        """
        return self.set('correlations', 'matrix', correlations, ttl=ttl)

    def get_correlations(self) -> Optional[Dict]:
        """
        Récupère la matrice de corrélations du cache.

        Returns:
            Corrélations ou None
        """
        return self.get('correlations', 'matrix')

    def cache_risk_metrics(self, portfolio_id: str, metrics: Dict, ttl: int = 1800) -> bool:
        """
        Met en cache les métriques de risque.

        Args:
            portfolio_id: ID du portefeuille
            metrics: Métriques de risque
            ttl: TTL en secondes (défaut 30 minutes)

        Returns:
            True si mis en cache, False sinon
        """
        return self.set('risk', portfolio_id, metrics, ttl=ttl)

    def get_risk_metrics(self, portfolio_id: str) -> Optional[Dict]:
        """
        Récupère les métriques de risque du cache.

        Args:
            portfolio_id: ID du portefeuille

        Returns:
            Métriques ou None
        """
        return self.get('risk', portfolio_id)

    def invalidate_symbol_cache(self, symbol: str) -> int:
        """
        Invalide tout le cache pour un symbole.

        Args:
            symbol: Symbole à invalider

        Returns:
            Nombre d'entrées supprimées
        """
        deleted = 0
        deleted += self.clear_prefix(f"market_data:{symbol}")
        deleted += self.clear_prefix(f"signals:{symbol}")
        deleted += self.clear_prefix(f"ml:{symbol}")
        deleted += self.clear_prefix(f"sentiment:{symbol}")
        return deleted

    def get_cache_stats(self) -> Dict:
        """
        Récupère les statistiques du cache.

        Returns:
            Dictionnaire avec les stats
        """
        if not self.is_connected():
            return {'connected': False}

        try:
            info = self.client.info()
            return {
                'connected': True,
                'used_memory': info.get('used_memory_human', 'N/A'),
                'total_connections_received': info.get('total_connections_received', 0),
                'connected_clients': info.get('connected_clients', 0),
                'uptime_days': info.get('uptime_in_days', 0)
            }
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des stats: {e}")
            return {'connected': False, 'error': str(e)}

# Instance globale du cache
cache = RedisCache()

def get_cache() -> RedisCache:
    """Retourne l'instance globale du cache."""
    return cache