"""
Module d'extraction des donnees meteo.

RESPONSABILITE : Extraire les donnees brutes de l'API
pour une liste de villes.
"""

import time
import logging
from typing import List, Dict, Any, Optional

from config import settings
from src.api_client import WeatherAPIClient


logger = logging.getLogger(__name__)


class WeatherExtractor:
    """
    Extracteur de donnees meteo.
    
    Utilise le client API pour recuperer les donnees
    de plusieurs villes.
    
    Attributes:
        client: Client API pour les requetes
    """
    
    def __init__(self, client: WeatherAPIClient = None):
        """
        Initialise l'extracteur.
        
        Args:
            client: Client API a utiliser.
                    Si None, en cree un nouveau.
                    
        Note:
            L'injection du client facilite les tests unitaires
            (on peut injecter un mock).
        """
        self.client = client or WeatherAPIClient()
        self._owns_client = client is None
    
    def extract_single(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Extrait la meteo d'une seule ville.
        
        Args:
            city: Nom de la ville
            
        Returns:
            Donnees meteo ou None si echec
        """
        logger.debug(f"Extraction pour {city}")
        return self.client.get_weather(city)
    
    def extract_cities(self, cities: List[str] = None) -> List[Dict[str, Any]]:
        """
        Extrait la meteo de plusieurs villes.
        
        Args:
            cities: Liste des villes. Si None, utilise la config.
            
        Returns:
            Liste des donnees meteo (une par ville reussie)
        """
        cities = cities or settings.CITIES
        results = []
        successful = 0
        failed = 0
        
        logger.info(f"Debut extraction pour {len(cities)} villes")
        
        for i, city in enumerate(cities, 1):
            logger.debug(f"Traitement ville {i}/{len(cities)}: {city}")
            
            data = self.client.get_weather(city)
            
            if data:
                results.append(data)
                successful += 1
            else:
                failed += 1
                logger.warning(f"Echec extraction pour {city}")
            
            # Pause entre les requetes (respect du rate limit)
            if i < len(cities):
                time.sleep(settings.REQUEST_DELAY)
        
        logger.info(
            f"Extraction terminee : {successful} succes, {failed} echecs "
            f"sur {len(cities)} villes"
        )
        
        return results
    
    def close(self):
        """Libere les ressources."""
        if self._owns_client:
            self.client.close()
        logger.debug("Extracteur ferme")
    
    def __enter__(self):
        """Support du context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique."""
        self.close()
        return False
