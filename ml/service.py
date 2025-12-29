# Service d'apprentissage automatique pour le système de trading quantique
# Fournit des prédictions de signaux avec ensemble de modèles

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import xgboost as xgb
import lightgbm as lgb
import catboost as cb
from sklearn.ensemble import VotingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import joblib
import os
import logging

logger = logging.getLogger(__name__)

class MLService:
    """
    Service d'apprentissage automatique pour la confirmation de signaux de trading.
    Utilise un ensemble de modèles XGBoost, LightGBM et CatBoost pour améliorer la fiabilité.
    """

    def __init__(self, model_path: str = "models"):
        """
        Initialise le service ML.

        Args:
            model_path: Chemin vers le répertoire des modèles
        """
        self.model_path = model_path
        self.models = {}
        self.ensemble_model = None
        self.feature_columns = [
            'rsi', 'macd', 'macd_signal', 'macd_hist', 'bb_upper', 'bb_middle', 'bb_lower',
            'stoch_k', 'stoch_d', 'williams_r', 'cci', 'mfi', 'volume_ratio',
            'price_change', 'volatility', 'trend_strength'
        ]

        # Créer le répertoire des modèles s'il n'existe pas
        os.makedirs(model_path, exist_ok=True)

        # Charger ou entraîner les modèles
        self._load_or_train_models()

    def _load_or_train_models(self):
        """Charge les modèles existants ou les entraîne si nécessaire."""
        try:
            # Essayer de charger les modèles sauvegardés
            self.models = self._load_models()
            self.ensemble_model = self._create_ensemble()
            logger.info("Modèles ML chargés avec succès")
        except Exception as e:
            logger.warning(f"Impossible de charger les modèles: {e}. Entraînement en cours...")
            # Si pas de modèles, entraîner avec des données d'exemple
            self._train_initial_models()

    def _load_models(self) -> Dict:
        """Charge les modèles depuis le disque."""
        models = {}
        for model_name in ['xgboost', 'lightgbm', 'catboost']:
            model_file = os.path.join(self.model_path, f'{model_name}_model.pkl')
            if os.path.exists(model_file):
                models[model_name] = joblib.load(model_file)
        return models

    def _create_ensemble(self) -> VotingClassifier:
        """Crée un modèle d'ensemble à partir des modèles individuels."""
        if len(self.models) < 3:
            raise ValueError("Au moins 3 modèles requis pour l'ensemble")

        estimators = [
            ('xgboost', self.models['xgboost']),
            ('lightgbm', self.models['lightgbm']),
            ('catboost', self.models['catboost'])
        ]

        return VotingClassifier(estimators=estimators, voting='soft')

    def _train_initial_models(self):
        """Entraîne les modèles initiaux avec des données synthétiques."""
        # Générer des données d'entraînement synthétiques
        np.random.seed(42)
        n_samples = 10000

        # Caractéristiques techniques simulées
        features = {}
        for col in self.feature_columns:
            features[col] = np.random.randn(n_samples)

        X = pd.DataFrame(features)

        # Cibles simulées (signaux: 0=vente, 1=achat, 2=neutre)
        # Simuler une logique simple pour créer des patterns
        y = np.random.choice([0, 1, 2], n_samples, p=[0.3, 0.4, 0.3])

        # Ajouter du bruit réaliste
        for i in range(len(y)):
            if X.loc[i, 'rsi'] > 70 and X.loc[i, 'macd'] > X.loc[i, 'macd_signal']:
                y[i] = 1  # Signal d'achat
            elif X.loc[i, 'rsi'] < 30 and X.loc[i, 'macd'] < X.loc[i, 'macd_signal']:
                y[i] = 0  # Signal de vente

        # Entraîner chaque modèle
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # XGBoost
        xgb_model = xgb.XGBClassifier(
            objective='multi:softprob',
            num_class=3,
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100,
            random_state=42
        )
        xgb_model.fit(X_train, y_train)
        self.models['xgboost'] = xgb_model

        # LightGBM
        lgb_model = lgb.LGBMClassifier(
            objective='multiclass',
            num_class=3,
            max_depth=6,
            learning_rate=0.1,
            n_estimators=100,
            random_state=42,
            verbose=-1
        )
        lgb_model.fit(X_train, y_train)
        self.models['lightgbm'] = lgb_model

        # CatBoost
        cb_model = cb.CatBoostClassifier(
            iterations=100,
            depth=6,
            learning_rate=0.1,
            loss_function='MultiClass',
            random_state=42,
            verbose=False
        )
        cb_model.fit(X_train, y_train)
        self.models['catboost'] = cb_model

        # Créer l'ensemble
        self.ensemble_model = self._create_ensemble()

        # Sauvegarder les modèles
        self._save_models()

        # Évaluer les performances
        y_pred = self.ensemble_model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Précision du modèle d'ensemble initial: {accuracy:.3f}")

    def _save_models(self):
        """Sauvegarde les modèles sur le disque."""
        for model_name, model in self.models.items():
            model_file = os.path.join(self.model_path, f'{model_name}_model.pkl')
            joblib.dump(model, model_file)

    def predict_signal(self, features: Dict) -> Dict:
        """
        Prédit le signal de trading avec score de confiance.

        Args:
            features: Dictionnaire des caractéristiques techniques

        Returns:
            Dictionnaire avec prédiction, confiance et probabilités
        """
        try:
            # Préparer les données d'entrée
            X_input = pd.DataFrame([features])[self.feature_columns]

            # Prédiction avec l'ensemble
            probabilities = self.ensemble_model.predict_proba(X_input)[0]
            prediction = int(self.ensemble_model.predict(X_input)[0])

            # Calculer la confiance (probabilité maximale)
            confidence = float(max(probabilities))

            # Mapper les prédictions
            signal_map = {0: 'VENTE', 1: 'ACHAT', 2: 'NEUTRE'}
            signal = signal_map.get(prediction, 'NEUTRE')

            return {
                'signal': signal,
                'confidence': confidence,
                'probabilities': {
                    'vente': float(probabilities[0]),
                    'achat': float(probabilities[1]),
                    'neutre': float(probabilities[2])
                },
                'prediction_time': pd.Timestamp.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Erreur lors de la prédiction: {e}")
            return {
                'signal': 'NEUTRE',
                'confidence': 0.0,
                'probabilities': {'vente': 0.0, 'achat': 0.0, 'neutre': 1.0},
                'error': str(e)
            }

    def update_models(self, new_data: pd.DataFrame):
        """
        Réentraîne les modèles avec de nouvelles données.

        Args:
            new_data: DataFrame avec nouvelles données d'entraînement
        """
        try:
            if len(new_data) < 100:
                logger.warning("Données insuffisantes pour le réentraînement")
                return

            # Utiliser les colonnes de caractéristiques
            X = new_data[self.feature_columns]
            y = new_data.get('target', new_data.get('signal'))

            if y is None:
                logger.error("Colonne cible manquante dans les données")
                return

            # Réentraîner chaque modèle
            for model_name, model in self.models.items():
                logger.info(f"Réentraînement du modèle {model_name}")
                model.fit(X, y)

            # Mettre à jour l'ensemble
            self.ensemble_model = self._create_ensemble()

            # Sauvegarder
            self._save_models()

            logger.info("Modèles mis à jour avec succès")

        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour des modèles: {e}")

    def get_feature_importance(self) -> Dict:
        """
        Retourne l'importance des caractéristiques pour chaque modèle.

        Returns:
            Dictionnaire avec importances par modèle
        """
        importance = {}

        try:
            # XGBoost
            if hasattr(self.models.get('xgboost'), 'feature_importances_'):
                importance['xgboost'] = dict(zip(
                    self.feature_columns,
                    self.models['xgboost'].feature_importances_
                ))

            # LightGBM
            if hasattr(self.models.get('lightgbm'), 'feature_importances_'):
                importance['lightgbm'] = dict(zip(
                    self.feature_columns,
                    self.models['lightgbm'].feature_importances_
                ))

            # CatBoost
            if hasattr(self.models.get('catboost'), 'feature_importances_'):
                importance['catboost'] = dict(zip(
                    self.feature_columns,
                    self.models['catboost'].feature_importances_
                ))

        except Exception as e:
            logger.error(f"Erreur lors de la récupération de l'importance: {e}")

        return importance