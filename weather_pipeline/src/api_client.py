
"""
Client API pour OpenWeatherMap.

Ce module gère :
- La connexion à l'API
- Les timeouts (API lente)
- Les retries (API qui échoue temporairement)
- La gestion des erreurs HTTP
"""

import time
import logging
import requests
from typing import Dict, Any, Optional

# Import de la configuration
from config import settings

# Création du logger pour ce module
# Chaque module a son propre logger pour filtrer les messages
logger = logging.getLogger(__name__)


class APIError(Exception):
    """
    Exception personnalisée pour les erreurs API.
    
    POURQUOI UNE EXCEPTION PERSONNALISÉE ?
    - Permet de distinguer les erreurs API des autres erreurs
    - Facilite le traitement spécifique dans le code appelant
    """
    pass


class WeatherAPIClient:
    """
    Client pour l'API OpenWeatherMap.
    
    RESPONSABILITÉ UNIQUE : Communiquer avec l'API.
    Ce client ne sait pas ce qu'on fait des données,
    il sait juste comment les récupérer.
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialise le client API.
        
        Args:
            api_key: Clé API OpenWeatherMap. 
                     Si None, utilise la clé de settings.
        """
        # On utilise la clé fournie ou celle de la config
        self.api_key = api_key or settings.API_KEY
        self.base_url = settings.BASE_URL
        
        # Création d'une session requests
        # POURQUOI UNE SESSION ?
        # - Réutilise les connexions (plus rapide)
        # - Conserve les cookies si nécessaire
        # - Permet de configurer des headers par défaut
        self.session = requests.Session()
        
        logger.info("Client API initialisé")
    
    def get_weather(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Récupère la météo d'une ville.
        
        Args:
            city: Nom de la ville (ex: "Paris")
            
        Returns:
            Dictionnaire avec les données météo, ou None si échec
            
        PATTERN UTILISÉ : Retry avec backoff exponentiel
        """
        # Paramètres de la requête
        params = {
            "q": city,           # Nom de la ville
            "appid": self.api_key,  # Clé API
            "units": "metric"    # Température en Celsius
        }
        
        # Tentatives avec retry
        for attempt in range(1, settings.MAX_RETRIES + 1):
            try:
                logger.debug(f"Tentative {attempt}/{settings.MAX_RETRIES} pour {city}")
                
                # Effectuer la requête avec timeout
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=settings.REQUEST_TIMEOUT
                )
                
                # Vérifier le code de réponse HTTP
                # raise_for_status() lève une exception si code >= 400
                response.raise_for_status()
                
                # Succès ! On retourne les données JSON
                logger.info(f"Météo récupérée pour {city}")
                return response.json()
                
            except requests.exceptions.Timeout:
                # L'API n'a pas répondu à temps
                logger.warning(
                    f"Timeout pour {city} (tentative {attempt}/{settings.MAX_RETRIES})"
                )
                
            except requests.exceptions.HTTPError as e:
                # Erreur HTTP (401, 404, 500, etc.)
                status_code = e.response.status_code
                
                if status_code == 401:
                    # Clé API invalide - pas la peine de réessayer
                    logger.error("Clé API invalide")
                    raise APIError("Clé API invalide")
                    
                elif status_code == 404:
                    # Ville non trouvée - pas la peine de réessayer
                    logger.warning(f"Ville non trouvée : {city}")
                    return None
                    
                elif status_code == 429:
                    # Rate limit - on attend plus longtemps
                    logger.warning(f"Rate limit atteint, attente prolongée...")
                    time.sleep(settings.RETRY_DELAY * 5)
                    
                else:
                    logger.warning(f"Erreur HTTP {status_code} pour {city}")
                    
            except requests.exceptions.RequestException as e:
                # Autres erreurs réseau
                logger.warning(f"Erreur réseau pour {city}: {e}")
            
            # Attendre avant de réessayer (backoff exponentiel)
            if attempt < settings.MAX_RETRIES:
                wait_time = settings.RETRY_DELAY * (2 ** (attempt - 1))
                logger.debug(f"Attente de {wait_time}s avant nouvelle tentative")
                time.sleep(wait_time)
        
        # Toutes les tentatives ont échoué
        logger.error(f"Échec définitif pour {city} après {settings.MAX_RETRIES} tentatives")
        return None
    
    def close(self):
        """Ferme la session HTTP proprement."""
        self.session.close()
        logger.debug("Session HTTP fermée")