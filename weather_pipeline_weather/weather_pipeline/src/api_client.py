"""
Client API pour OpenWeatherMap.

Ce module gere :
- La connexion a l'API
- Les timeouts (API lente)
- Les retries (API qui echoue temporairement)
- La gestion des erreurs HTTP
"""

import time
import logging
import requests
from typing import Dict, Any, Optional

from config import settings


# Chaque module a son propre logger
logger = logging.getLogger(__name__)


class APIError(Exception):
    """
    Exception personnalisee pour les erreurs API.
    
    Permet de distinguer les erreurs API des autres erreurs Python.
    """
    pass


class WeatherAPIClient:
    """
    Client pour l'API OpenWeatherMap.
    
    RESPONSABILITE UNIQUE : Communiquer avec l'API.
    Ce client ne sait pas ce qu'on fait des donnees,
    il sait juste comment les recuperer.
    
    Attributes:
        api_key: Cle API OpenWeatherMap
        base_url: URL de base de l'API
        session: Session HTTP reutilisable
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialise le client API.
        
        Args:
            api_key: Cle API OpenWeatherMap. 
                     Si None, utilise la cle de settings.
        """
        self.api_key = api_key or settings.API_KEY
        self.base_url = settings.BASE_URL
        
        # Session requests pour reutiliser les connexions
        self.session = requests.Session()
        
        logger.info("Client API initialise")
    
    def get_weather(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Recupere la meteo d'une ville.
        
        Args:
            city: Nom de la ville (ex: "Paris")
            
        Returns:
            Dictionnaire avec les donnees meteo, ou None si echec
            
        Raises:
            APIError: Si la cle API est invalide
        """
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "metric"
        }
        
        # Tentatives avec retry et backoff exponentiel
        for attempt in range(1, settings.MAX_RETRIES + 1):
            try:
                logger.debug(f"Tentative {attempt}/{settings.MAX_RETRIES} pour {city}")
                
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=settings.REQUEST_TIMEOUT
                )
                
                response.raise_for_status()
                
                logger.info(f"Meteo recuperee pour {city}")
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(
                    f"Timeout pour {city} (tentative {attempt}/{settings.MAX_RETRIES})"
                )
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                if status_code == 401:
                    logger.error("Cle API invalide")
                    raise APIError("Cle API invalide - verifiez votre configuration")
                    
                elif status_code == 404:
                    logger.warning(f"Ville non trouvee : {city}")
                    return None
                    
                elif status_code == 429:
                    logger.warning("Rate limit atteint, attente prolongee...")
                    time.sleep(settings.RETRY_DELAY * 5)
                    
                else:
                    logger.warning(f"Erreur HTTP {status_code} pour {city}")
                    
            except requests.exceptions.ConnectionError:
                logger.warning(f"Erreur de connexion pour {city}")
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Erreur reseau pour {city}: {e}")
            
            # Attendre avant de reessayer (backoff exponentiel)
            if attempt < settings.MAX_RETRIES:
                wait_time = settings.RETRY_DELAY * (2 ** (attempt - 1))
                logger.debug(f"Attente de {wait_time}s avant nouvelle tentative")
                time.sleep(wait_time)
        
        logger.error(f"Echec definitif pour {city} apres {settings.MAX_RETRIES} tentatives")
        return None
    
    def close(self):
        """Ferme la session HTTP proprement."""
        self.session.close()
        logger.debug("Session HTTP fermee")
    
    def __enter__(self):
        """Support du context manager (with statement)."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fermeture automatique avec context manager."""
        self.close()
        return False
