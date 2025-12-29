"""
Pipeline d'entraînement avancé du modèle ML.
Gère l'entraînement, la validation croisée, l'optimisation et les ensembles.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, log_loss, brier_score_loss
)
import sys
import os
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config
from .features import MLFeaturesPreparer
from .model import SignalClassifier

# Import des modèles avancés
try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

try:
    import lightgbm as lgb
    LIGHTGBM_AVAILABLE = True
except ImportError:
    LIGHTGBM_AVAILABLE = False

try:
    import catboost as cb
    CATBOOST_AVAILABLE = True
except ImportError:
    CATBOOST_AVAILABLE = False


class EnsembleTrainer:
    """
    Entraîneur avancé utilisant un ensemble de modèles avec optimisation.

    Features:
    - Ensemble de modèles (XGBoost, LightGBM, CatBoost, RandomForest)
    - Optimisation Optuna des hyperparamètres
    - Validation croisée walk-forward
    - Stacking et voting
    """

    def __init__(self):
        self.preparer = MLFeaturesPreparer()
        self.models = {}
        self.best_params = {}
        self.ensemble_model = None
        self.is_trained = False

    def _create_base_models(self) -> Dict:
        """Crée les modèles de base pour l'ensemble."""
        models = {}

        # XGBoost
        try:
            from xgboost import XGBClassifier
            models['xgboost'] = XGBClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42,
                eval_metric='auc'
            )
        except ImportError:
            pass

        # LightGBM
        if LIGHTGBM_AVAILABLE:
            models['lightgbm'] = lgb.LGBMClassifier(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42,
                verbose=-1
            )

        # CatBoost (désactivé temporairement pour compatibilité sklearn)
        # if CATBOOST_AVAILABLE:
        #     try:
        #         models['catboost'] = cb.CatBoostClassifier(
        #             iterations=100,
        #             depth=6,
        #             learning_rate=0.1,
        #             random_state=42,
        #             verbose=False
        #         )
        #     except:
        #         print("⚠️ CatBoost incompatible avec cette version de sklearn")
        #         CATBOOST_AVAILABLE = False

        # RandomForest (fallback)
        from sklearn.ensemble import RandomForestClassifier
        models['random_forest'] = RandomForestClassifier(
            n_estimators=100,
            max_depth=6,
            random_state=42
        )

        return models

    def optimize_hyperparameters(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        model_name: str = 'xgboost',
        n_trials: int = 50
    ) -> Dict:
        """
        Optimise les hyperparamètres avec Optuna.

        Args:
            X: Features d'entraînement
            y: Target
            model_name: Nom du modèle à optimiser
            n_trials: Nombre d'essais Optuna

        Returns:
            Meilleurs paramètres trouvés
        """
        if not OPTUNA_AVAILABLE:
            print("⚠️ Optuna non disponible, utilisation des paramètres par défaut")
            return self._get_default_params(model_name)

        def objective(trial):
            if model_name == 'xgboost':
                params = {
                    'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                    'max_depth': trial.suggest_int('max_depth', 3, 10),
                    'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                    'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                    'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                    'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
                    'gamma': trial.suggest_float('gamma', 0, 1),
                    'random_state': 42,
                    'eval_metric': 'auc'
                }
                model = XGBClassifier(**params)

            elif model_name == 'lightgbm':
                params = {
                    'n_estimators': trial.suggest_int('n_estimators', 50, 300),
                    'max_depth': trial.suggest_int('max_depth', 3, 10),
                    'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
                    'subsample': trial.suggest_float('subsample', 0.6, 1.0),
                    'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
                    'min_child_samples': trial.suggest_int('min_child_samples', 5, 50),
                    'random_state': 42,
                    'verbose': -1
                }
                model = lgb.LGBMClassifier(**params)

            # elif model_name == 'catboost':
            #     params = {
            #         'iterations': trial.suggest_int('iterations', 50, 300),
            #         'depth': trial.suggest_int('depth', 3, 10),
            #         'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
            #         'random_state': 42,
            #         'verbose': False
            #     }
            #     model = cb.CatBoostClassifier(**params)

            else:
                return 0.5  # Score neutre

            # Validation croisée rapide
            tscv = TimeSeriesSplit(n_splits=3)
            scores = []

            for train_idx, val_idx in tscv.split(X):
                X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
                y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

                model.fit(X_train, y_train)
                proba = model.predict_proba(X_val)[:, 1]
                auc = roc_auc_score(y_val, proba)
                scores.append(auc)

            return np.mean(scores)

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials)

        best_params = study.best_params
        best_params['random_state'] = 42
        if model_name == 'xgboost':
            best_params['eval_metric'] = 'auc'
        elif model_name == 'lightgbm':
            best_params['verbose'] = -1
        # elif model_name == 'catboost':
        #     best_params['verbose'] = False

        self.best_params[model_name] = best_params
        print(f"✅ Meilleurs paramètres pour {model_name}: AUC = {study.best_value:.4f}")

        return best_params

    def _get_default_params(self, model_name: str) -> Dict:
        """Paramètres par défaut si Optuna n'est pas disponible."""
        defaults = {
            'xgboost': {
                'n_estimators': 100, 'max_depth': 6, 'learning_rate': 0.1,
                'subsample': 0.8, 'colsample_bytree': 0.8, 'random_state': 42,
                'eval_metric': 'auc'
            },
            'lightgbm': {
                'n_estimators': 100, 'max_depth': 6, 'learning_rate': 0.1,
                'subsample': 0.8, 'colsample_bytree': 0.8, 'random_state': 42,
                'verbose': -1
            },
            # 'catboost': {
            #     'iterations': 100, 'depth': 6, 'learning_rate': 0.1,
            #     'random_state': 42, 'verbose': False
            # },
            'random_forest': {
                'n_estimators': 100, 'max_depth': 6, 'random_state': 42
            }
        }
        return defaults.get(model_name, {})

    def train_ensemble(
        self,
        df: pd.DataFrame,
        optimize: bool = True,
        n_trials: int = 30
    ) -> Dict:
        """
        Entraîne un ensemble de modèles avec optimisation.

        Args:
            df: DataFrame avec features
            optimize: Si True, optimise les hyperparamètres
            n_trials: Nombre d'essais Optuna par modèle

        Returns:
            Résultats d'entraînement
        """
        print("🚀 Entraînement de l'ensemble de modèles...")

        # Préparer les données
        X, y = self.preparer.prepare_train_data(df)

        if len(X) < 100:
            raise ValueError("Données insuffisantes pour l'entraînement")

        # Créer les modèles
        base_models = self._create_base_models()
        trained_models = []

        # Optimiser et entraîner chaque modèle
        for name, model in base_models.items():
            print(f"📊 Entraînement de {name}...")

            if optimize and OPTUNA_AVAILABLE:
                best_params = self.optimize_hyperparameters(X, y, name, n_trials)
                model.set_params(**best_params)

            # Entraîner le modèle
            model.fit(X, y)
            self.models[name] = model
            trained_models.append((name, model))

        # Créer l'ensemble avec voting
        self.ensemble_model = VotingClassifier(
            estimators=trained_models,
            voting='soft'  # Utilise les probabilités
        )

        # Entraîner l'ensemble
        self.ensemble_model.fit(X, y)
        self.is_trained = True

        # Évaluation
        train_proba = self.ensemble_model.predict_proba(X)[:, 1]
        train_pred = self.ensemble_model.predict(X)

        metrics = {
            'train_accuracy': accuracy_score(y, train_pred),
            'train_precision': precision_score(y, train_pred, zero_division=0),
            'train_recall': recall_score(y, train_pred, zero_division=0),
            'train_f1': f1_score(y, train_pred, zero_division=0),
            'train_auc': roc_auc_score(y, train_proba),
            'train_log_loss': log_loss(y, train_proba),
            'brier_score': brier_score_loss(y, train_proba),
            'models_used': list(self.models.keys()),
            'n_samples': len(X),
            'n_features': X.shape[1]
        }

        print("✅ Ensemble entraîné avec succès!")
        print(f"  Accuracy: {metrics['train_accuracy']:.4f}")
        print(f"  AUC: {metrics['train_auc']:.4f}")
        return metrics

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Prédit les probabilités avec l'ensemble."""
        if not self.is_trained:
            raise ValueError("Ensemble non entraîné")
        return self.ensemble_model.predict_proba(X)[:, 1]

    def predict_signal(self, X: pd.DataFrame) -> Dict:
        """Génère un signal avec l'ensemble."""
        proba = self.predict_proba(X)

        if len(proba) == 1:
            proba = proba[0]
        else:
            proba = proba[-1]

        min_threshold = config.ml.MIN_PROBABILITY_THRESHOLD
        strong_threshold = config.ml.STRONG_SIGNAL_THRESHOLD

        if proba >= strong_threshold:
            signal = "STRONG_BUY"
            action = "Signal fort de l'ensemble - entrer en position"
        elif proba >= min_threshold:
            signal = "BUY"
            action = "Signal valide de l'ensemble"
        elif proba <= 1 - strong_threshold:
            signal = "STRONG_AVOID"
            action = "Éviter - probabilité d'échec élevée"
        elif proba <= 1 - min_threshold:
            signal = "AVOID"
            action = "Probabilité insuffisante"
        else:
            signal = "WAIT"
            action = "Zone d'incertitude"

        return {
            "signal": signal,
            "probability": proba * 100,
            "action": action,
            "threshold_met": proba >= min_threshold,
            "is_strong": proba >= strong_threshold,
            "model_type": "ensemble"
        }

    def walk_forward_validation(
        self,
        df: pd.DataFrame,
        n_splits: int = 5,
        train_window: int = 252  # 1 an
    ) -> Dict:
        """
        Validation walk-forward pour évaluer la stabilité temporelle.

        Args:
            df: DataFrame complet
            n_splits: Nombre de fenêtres
            train_window: Taille de la fenêtre d'entraînement (en jours)

        Returns:
            Résultats de validation
        """
        print("🔄 Validation walk-forward...")

        X, y = self.preparer.prepare_train_data(df)
        results = []

        # Trier par date (assumé que l'index est datetime)
        X = X.sort_index()
        y = y.sort_index()

        total_samples = len(X)
        step_size = max(1, (total_samples - train_window) // n_splits)

        for i in range(n_splits):
            train_end = train_window + (i * step_size)
            if train_end >= total_samples:
                break

            # Fenêtre d'entraînement
            X_train = X.iloc[:train_end]
            y_train = y.iloc[:train_end]

            # Fenêtre de test (prochaine période)
            test_start = train_end
            test_end = min(total_samples, train_end + step_size)
            X_test = X.iloc[test_start:test_end]
            y_test = y.iloc[test_start:test_end]

            if len(X_test) == 0:
                continue

            # Entraîner temporairement
            temp_ensemble = EnsembleTrainer()
            temp_ensemble.train_ensemble(
                pd.concat([X_train, y_train], axis=1),
                optimize=False
            )

            # Tester
            test_proba = temp_ensemble.predict_proba(X_test)
            test_pred = (test_proba >= 0.5).astype(int)

            fold_metrics = {
                'fold': i,
                'train_size': len(X_train),
                'test_size': len(X_test),
                'accuracy': accuracy_score(y_test, test_pred),
                'precision': precision_score(y_test, test_pred, zero_division=0),
                'recall': recall_score(y_test, test_pred, zero_division=0),
                'auc': roc_auc_score(y_test, test_proba) if len(np.unique(y_test)) > 1 else 0.5
            }

            results.append(fold_metrics)
            print(f"  Fold {i}: AUC = {fold_metrics['auc']:.4f}")

        # Résumé
        auc_scores = [r['auc'] for r in results]
        summary = {
            'mean_auc': np.mean(auc_scores),
            'std_auc': np.std(auc_scores),
            'min_auc': np.min(auc_scores),
            'max_auc': np.max(auc_scores),
            'fold_results': results
        }

        print("✅ Validation walk-forward terminée")
        print(f"  AUC moyen: {summary['mean_auc']:.4f} ± {summary['std_auc']:.4f}")
        return summary


class ModelTrainer:
    """
    Gère l'entraînement complet du modèle (version legacy + avancée).

    Étapes:
    1. Préparation des features
    2. Validation croisée temporelle
    3. Entraînement final
    4. Évaluation
    """

    def __init__(self, use_ensemble: bool = None):
        self.preparer = MLFeaturesPreparer()
        self.classifier = SignalClassifier()
        self.use_ensemble = use_ensemble if use_ensemble is not None else config.ml.USE_ENSEMBLE
        self.ensemble_trainer = EnsembleTrainer() if self.use_ensemble else None
        self.cv_results = []
    
    def train_with_cross_validation(
        self,
        df: pd.DataFrame,
        n_splits: int = None,
        use_ensemble: bool = None
    ) -> Dict:
        """
        Entraîne avec validation croisée temporelle.

        Args:
            df: DataFrame complet avec features
            n_splits: Nombre de splits CV
            use_ensemble: Utiliser l'ensemble avancé (auto si None)

        Returns:
            Résultats de la validation croisée
        """
        use_ensemble = use_ensemble if use_ensemble is not None else self.use_ensemble

        if use_ensemble and self.ensemble_trainer:
            print("🚀 Utilisation de l'ensemble avancé avec optimisation...")
            return self._train_ensemble_advanced(df)

        # Version classique
        n_splits = n_splits or config.ml.CV_SPLITS

        # Préparer données
        X, y = self.preparer.prepare_train_data(df)

        if len(X) < n_splits * 50:
            print("⚠️ Données insuffisantes pour CV complète")
            n_splits = max(2, len(X) // 100)

        # Time Series Split
        tscv = TimeSeriesSplit(n_splits=n_splits)

        fold_results = []

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

            # Nouveau classificateur pour ce fold
            fold_clf = SignalClassifier()

            # Entraîner
            temp_metrics = self._train_fold(fold_clf, X_train, y_train, X_val, y_val)
            temp_metrics['fold'] = fold
            fold_results.append(temp_metrics)

        self.cv_results = fold_results

        # Moyennes
        avg_metrics = {
            'mean_accuracy': np.mean([r['accuracy'] for r in fold_results]),
            'std_accuracy': np.std([r['accuracy'] for r in fold_results]),
            'mean_auc': np.mean([r.get('auc', 0.5) for r in fold_results]),
            'n_folds': n_splits
        }

        # Entraîner modèle final sur toutes les données
        final_metrics = self.classifier.train(X, y)

        return {
            'cv_results': fold_results,
            'cv_summary': avg_metrics,
            'final_model': final_metrics,
            'model_type': 'single_xgboost'
        }

    def _train_ensemble_advanced(self, df: pd.DataFrame) -> Dict:
        """Entraîne avec l'ensemble avancé."""
        # Validation croisée walk-forward pour l'ensemble
        wf_results = self.ensemble_trainer.walk_forward_validation(df, n_splits=5)

        # Entraînement final
        final_metrics = self.ensemble_trainer.train_ensemble(df, optimize=True, n_trials=20)

        return {
            'cv_results': wf_results['fold_results'],
            'cv_summary': {
                'mean_accuracy': np.mean([r['accuracy'] for r in wf_results['fold_results']]),
                'std_accuracy': np.std([r['accuracy'] for r in wf_results['fold_results']]),
                'mean_auc': wf_results['mean_auc'],
                'n_folds': len(wf_results['fold_results'])
            },
            'final_model': final_metrics,
            'model_type': 'ensemble',
            'walk_forward': wf_results
        }
    
    def _train_fold(
        self,
        clf: SignalClassifier,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series
    ) -> Dict:
        """Entraîne un fold de CV."""
        # Fit temporaire
        clf.model.fit(X_train, y_train)
        clf.is_trained = True
        
        # Évaluer
        train_acc = clf.model.score(X_train, y_train)
        val_acc = clf.model.score(X_val, y_val)
        
        # AUC
        from sklearn.metrics import roc_auc_score
        try:
            val_proba = clf.model.predict_proba(X_val)[:, 1]
            auc = roc_auc_score(y_val, val_proba)
        except:
            auc = 0.5
        
        return {
            'accuracy': val_acc,
            'train_accuracy': train_acc,
            'auc': auc,
            'train_size': len(X_train),
            'val_size': len(X_val)
        }
    
    def evaluate_on_test(
        self,
        test_df: pd.DataFrame
    ) -> Dict:
        """
        Évalue sur données de test.
        
        Args:
            test_df: DataFrame de test
        
        Returns:
            Métriques de test
        """
        if not self.classifier.is_trained:
            raise ValueError("Modèle non entraîné")
        
        X_test, y_test = self.preparer.prepare_train_data(test_df)
        
        # Prédictions
        y_pred = self.classifier.model.predict(X_test)
        y_proba = self.classifier.predict_proba(X_test)
        
        from sklearn.metrics import (
            accuracy_score, precision_score, recall_score,
            f1_score, roc_auc_score, confusion_matrix
        )
        
        return {
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred, zero_division=0),
            'recall': recall_score(y_test, y_pred, zero_division=0),
            'f1': f1_score(y_test, y_pred, zero_division=0),
            'auc': roc_auc_score(y_test, y_proba) if len(np.unique(y_test)) > 1 else 0.5,
            'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
            'n_samples': len(y_test)
        }
    
    def get_trading_statistics(
        self,
        df: pd.DataFrame,
        min_proba: float = None
    ) -> Dict:
        """
        Calcule les statistiques de trading basées sur les prédictions.
        
        Simule les trades qui auraient été pris avec le seuil de probabilité.
        """
        min_proba = min_proba or config.ml.MIN_PROBABILITY_THRESHOLD
        
        X, y = self.preparer.prepare_train_data(df)
        
        if not self.classifier.is_trained:
            return {"error": "Modèle non entraîné"}
        
        probas = self.classifier.predict_proba(X)
        
        # Trades au-dessus du seuil
        above_threshold = probas >= min_proba
        trades_taken = above_threshold.sum()
        
        if trades_taken == 0:
            return {
                "trades_taken": 0,
                "message": "Aucun trade au-dessus du seuil"
            }
        
        # Win rate sur les trades pris
        wins = (y[above_threshold] == 1).sum()
        win_rate = wins / trades_taken
        
        # Comparaison avec random
        overall_win_rate = y.mean()
        
        return {
            "trades_taken": int(trades_taken),
            "total_opportunities": len(y),
            "selectivity": trades_taken / len(y) * 100,
            "wins": int(wins),
            "losses": int(trades_taken - wins),
            "win_rate": win_rate * 100,
            "baseline_win_rate": overall_win_rate * 100,
            "edge": (win_rate - overall_win_rate) * 100,
            "threshold_used": min_proba * 100
        }
    
    def save_model(self, path: str):
        """Sauvegarde modèle et preparer."""
        self.classifier.save(path)
    
    def load_model(self, path: str):
        """Charge modèle."""
        self.classifier.load(path)


if __name__ == "__main__":
    # Test
    np.random.seed(42)
    n = 1000
    
    # Données synthétiques
    df = pd.DataFrame({
        'Close': 100 + np.cumsum(np.random.randn(n) * 0.5),
        'zscore': np.random.randn(n),
        'hurst': np.random.uniform(0.3, 0.7, n),
        'rsi': np.random.uniform(20, 80, n),
        'macd_signal': np.random.randn(n) * 0.1,
        'atr_normalized': np.random.uniform(0.01, 0.03, n)
    })
    
    # Entraîner
    trainer = ModelTrainer()
    results = trainer.train_with_cross_validation(df, n_splits=3)
    
    print("=== Résultats CV ===")
    for fold_res in results['cv_results']:
        print(f"  Fold {fold_res['fold']}: acc={fold_res['accuracy']:.3f}, auc={fold_res['auc']:.3f}")
    
    print("\n=== Résumé CV ===")
    summary = results['cv_summary']
    print(f"  Accuracy: {summary['mean_accuracy']:.3f} ± {summary['std_accuracy']:.3f}")
    print(f"  AUC: {summary['mean_auc']:.3f}")
    
    print("\n=== Statistiques Trading ===")
    stats = trainer.get_trading_statistics(df)
    for k, v in stats.items():
        print(f"  {k}: {v}")
