"""
Pipeline principal d'orchestration.

Ce module coordonne toutes les etapes :
1. Extraction (via Extractor)
2. Transformation (via Transformer)
3. Chargement (sauvegarde des resultats)

C'est le pattern ETL (Extract, Transform, Load).
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd

from config import settings
from src.extractor import WeatherExtractor
from src.transformer import WeatherTransformer
from src.api_client import WeatherAPIClient


logger = logging.getLogger(__name__)


class WeatherPipeline:
    """
    Pipeline ETL pour les donnees meteo.
    
    RESPONSABILITE : Orchestrer les etapes Extract, Transform, Load
    sans connaitre les details de chaque etape.
    
    Attributes:
        client: Client API
        extractor: Extracteur de donnees
        transformer: Transformateur de donnees
    """
    
    def __init__(self):
        """Initialise les composants du pipeline."""
        self.client = WeatherAPIClient()
        self.extractor = WeatherExtractor(self.client)
        self.transformer = WeatherTransformer()
        
        logger.info("Pipeline initialise")
    
    def run(self) -> Optional[pd.DataFrame]:
        """
        Execute le pipeline complet.
        
        Returns:
            DataFrame avec les resultats, ou None si echec
            
        Raises:
            Exception: Si une erreur fatale se produit
        """
        start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("DEMARRAGE DU PIPELINE METEO")
        logger.info("=" * 60)
        
        try:
            # =========================================================
            # ETAPE 1 : EXTRACTION
            # =========================================================
            logger.info("ETAPE 1 : Extraction des donnees...")
            raw_data = self.extractor.extract_cities()
            
            if not raw_data:
                logger.error("Aucune donnee extraite - arret du pipeline")
                return None
            
            logger.info(f"  -> {len(raw_data)} villes extraites")
            
            # =========================================================
            # ETAPE 2 : TRANSFORMATION
            # =========================================================
            logger.info("ETAPE 2 : Transformation des donnees...")
            df = self.transformer.transform(raw_data)
            
            if df.empty:
                logger.error("DataFrame vide apres transformation")
                return None
            
            logger.info(f"  -> {len(df)} lignes dans le DataFrame")
            
            # =========================================================
            # ETAPE 3 : CHARGEMENT (Sauvegarde)
            # =========================================================
            logger.info("ETAPE 3 : Sauvegarde des resultats...")
            output_path = self._save_results(df)
            
            # =========================================================
            # RESUME
            # =========================================================
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("PIPELINE TERMINE AVEC SUCCES")
            logger.info(f"  - Villes traitees : {len(df)}")
            logger.info(f"  - Fichier genere  : {output_path}")
            logger.info(f"  - Duree           : {duration:.2f} secondes")
            logger.info("=" * 60)
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur fatale du pipeline : {e}")
            raise
        
        finally:
            self._cleanup()
    
    def _save_results(self, df: pd.DataFrame) -> str:
        """
        Sauvegarde le DataFrame en CSV.
        
        Args:
            df: DataFrame a sauvegarder
            
        Returns:
            Chemin du fichier cree
        """
        # Creer le dossier de sortie si necessaire
        os.makedirs(settings.OUTPUT_DIR, exist_ok=True)
        
        # Chemin complet du fichier
        output_path = os.path.join(settings.OUTPUT_DIR, settings.OUTPUT_FILE)
        
        # Sauvegarder en CSV
        df.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info(f"Resultats sauvegardes : {output_path}")
        return output_path
    
    def _save_json(self, df: pd.DataFrame, path: str) -> str:
        """
        Sauvegarde le DataFrame en JSON (optionnel).
        
        Args:
            df: DataFrame a sauvegarder
            path: Chemin du fichier
            
        Returns:
            Chemin du fichier cree
        """
        df.to_json(path, orient="records", indent=2, date_format="iso")
        logger.info(f"JSON sauvegarde : {path}")
        return path
    
    def _cleanup(self):
        """Libere toutes les ressources."""
        self.extractor.close()
        logger.debug("Ressources liberees")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Retourne des statistiques sur le dernier run.
        
        Returns:
            Dictionnaire avec les stats
        """
        return {
            "cities_configured": len(settings.CITIES),
            "api_url": settings.BASE_URL,
            "output_dir": settings.OUTPUT_DIR
        }
