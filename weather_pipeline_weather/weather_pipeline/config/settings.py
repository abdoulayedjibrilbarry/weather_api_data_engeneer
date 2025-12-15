"""
Configuration centralisee du projet Weather Pipeline.

REGLE D'OR : Ne jamais ecrire de valeurs en dur dans le code.
Tout doit etre ici ou dans des variables d'environnement.
"""

import os
from typing import List


# =============================================================================
# CONFIGURATION API
# =============================================================================

# Cle API OpenWeatherMap
# En production : API_KEY = os.environ.get("OPENWEATHER_API_KEY")
API_KEY: str = os.environ.get("OPENWEATHER_API_KEY", "demo_key_replace_me")

# URL de base de l'API
BASE_URL: str = "https://api.openweathermap.org/data/2.5/weather"

# Liste des villes a interroger
CITIES: List[str] = [
    "Paris",
    "London",
    "New York",
    "Tokyo",
    "Sydney"
]


# =============================================================================
# CONFIGURATION RESEAU
# =============================================================================

# Timeout pour les requetes API (en secondes)
REQUEST_TIMEOUT: int = 10

# Nombre de tentatives en cas d'echec
MAX_RETRIES: int = 3

# Delai initial entre les tentatives (en secondes)
RETRY_DELAY: int = 2

# Delai entre chaque requete (en secondes)
REQUEST_DELAY: float = 0.5


# =============================================================================
# CONFIGURATION LOGGING
# =============================================================================

# Niveau de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL: str = "INFO"

# Chemin du fichier de log
LOG_FILE: str = "logs/pipeline.log"

# Format des messages de log
LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


# =============================================================================
# CONFIGURATION SORTIE
# =============================================================================

# Dossier de sortie des resultats
OUTPUT_DIR: str = "output"

# Nom du fichier de sortie
OUTPUT_FILE: str = "weather_data.csv"
