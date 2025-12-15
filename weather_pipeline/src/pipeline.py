"""
Pipeline principal d'orchestration.

Ce module coordonne toutes les étapes :
1. Extraction (via Extractor)
2. Transformation (via Transformer)
3. Chargement (sauvegarde des résultats)
"""

import os
import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from config import settings
from src.extractor import WeatherExtractor
from src.transformer import WeatherTransformer
from src.api_client import WeatherAPIClient

logger = logging.getLogger(__name__)


class WeatherPipeline:
    """
    Pipeline ETL pour les données météo.
    
    RESPONSABILITÉ : Orchestrer les étapes Extract, Transform, Load
    sans connaître les détails de chaque étape.
    """
    
    def __init__(self):
        """Initialise les composants du pipeline."""
        self.client = WeatherAPIClient()
        self.extractor = WeatherExtractor(self.client)
        self.transformer = WeatherTransformer()
        
        logger.info("Pipeline initialisé")
    
    def run(self) -> Optional[pd.DataFrame]:
        """
        Exécute le pipeline complet.
        
        Returns:
            DataFrame avec les résultats, ou None si échec
        """
        start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("DÉMARRAGE DU PIPELINE MÉTÉO")
        logger.info("=" * 60)
        
        try:
            # ÉTAPE 1 : EXTRACTION
            logger.info("ÉTAPE 1 : Extraction des données...")
            raw_data = self.extractor.extract_cities()
            
            if not raw_data:
                logger.error("Aucune donnée extraite")
                return None
            
            # ÉTAPE 2 : TRANSFORMATION
            logger.info("ÉTAPE 2 : Transformation des données...")
            df = self.transformer.transform(raw_data)
            
            if df.empty:
                logger.error("DataFrame vide après transformation")
                return None
            
            # ÉTAPE 3 : CHARGEMENT (Sauvegarde)
            logger.info("ÉTAPE 3 : Sauvegarde des résultats...")
            output_path = self._save_results(df)
            
            # RÉSUMÉ
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("PIPELINE TERMINÉ AVEC SUCCÈS")
            logger.info(f"  - Villes traitées : {len(df)}")
            logger.info(f"  - Fichier généré : {output_path}")
            logger.info(f"  - Durée : {duration:.2f} secondes")
            logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur fatale du pipeline : {e}")
            raise
        
        finally:
            # Libérer les ressources quoi qu'il arrive
            self._cleanup()
    
    def _save_results(self, df: pd.DataFrame) -> str:
        """
        Sauvegarde le DataFrame en CSV.
        
        Args:
            df: DataFrame à sauvegarder
            
        Returns:
            Chemin du fichier créé
        """
        # Créer le dossier de sortie si nécessaire
        os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
        
        # Chemin complet du fichier
        output_path = os.path.join(settings.OUTPUT_DIR, settings.OUTPUT_FILE)
        
        # Sauvegarder en CSV
        df.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info(f"Résultats sauvegardés : {output_path}")
        return output_path
    
    def _cleanup(self):
        """Libère toutes les ressources."""
        self.extractor.close()
        logger.debug("Ressources libérées")