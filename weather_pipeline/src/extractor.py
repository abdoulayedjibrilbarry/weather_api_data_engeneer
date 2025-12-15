"""
Module d'extraction des données météo.

RESPONSABILITÉ : Extraire les données brutes de l'API
pour une liste de villes.
"""

import time
import logging
from typing import List, Dict, Any

from config import settings
from src.api_client import WeatherAPIClient

logger = logging.getLogger(__name__)


class WeatherExtractor:
    """
    Extracteur de données météo.
    
    Utilise le client API pour récupérer les données
    de plusieurs villes.
    """
    
    def __init__(self, client: WeatherAPIClient = None):
        """
        Initialise l'extracteur.
        
        Args:
            client: Client API à utiliser.
                    Si None, en crée un nouveau.
                    
        POURQUOI INJECTER LE CLIENT ?
        C'est le pattern "Injection de Dépendances".
        Avantages :
        - Facilite les tests (on peut injecter un faux client)
        - Flexibilité (on peut changer le client sans modifier l'extracteur)
        """
        self.client = client or WeatherAPIClient()
    
    def extract_cities(self, cities: List[str] = None) -> List[Dict[str, Any]]:
        """
        Extrait la météo de plusieurs villes.
        
        Args:
            cities: Liste des villes. Si None, utilise la config.
            
        Returns:
            Liste des données météo (une par ville)
        """
        cities = cities or settings.CITIES
        results = []
        successful = 0
        failed = 0
        
        logger.info(f"Début extraction pour {len(cities)} villes")
        
        for city in cities:
            # Récupérer les données de la ville
            data = self.client.get_weather(city)
            
            if data:
                results.append(data)
                successful += 1
            else:
                failed += 1
            
            # Pause entre les requêtes (respect du rate limit)
            # IMPORTANT : Ne pas surcharger l'API
            time.sleep(settings.REQUEST_DELAY)
        
        # Résumé de l'extraction
        logger.info(
            f"Extraction terminée : {successful} succès, {failed} échecs"
        )
        
        return results
    
    def close(self):
        """Libère les ressources."""
        self.client.close()