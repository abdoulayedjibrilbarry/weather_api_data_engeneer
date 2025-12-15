"""
Configuration centralisée du projet.

RÈGLE D'OR : Ne jamais écrire de valeurs en dur dans le code.
Tout doit être ici ou dans des variables d'environnement.
"""

import os
from typing import List

# =============================================================================
# CONFIGURATION API
# =============================================================================

# Clé API - En production, on utiliserait :
# API_KEY = os.environ.get("OPENWEATHER_API_KEY")
API_KEY: str = "f81837dc402124853d067b23fcc50460"

# URL de base de l'API OpenWeatherMap
BASE_URL: str = "https://api.openweathermap.org/data/2.5/weather"

# Villes à interroger
CITIES: List[str] = [
    "Paris",
    "London", 
    "New York",
    "Tokyo",
    "Sydney"
]

# =============================================================================
# CONFIGURATION RÉSEAU
# =============================================================================

# Timeout : temps max d'attente pour une réponse (en secondes)
# Si l'API ne répond pas en 10s, on abandonne
REQUEST_TIMEOUT: int = 10

# Nombre de tentatives en cas d'échec
MAX_RETRIES: int = 3

# Délai initial entre les tentatives (en secondes)
RETRY_DELAY: int = 2

# Délai entre chaque requête pour éviter le rate limiting
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

# Dossier de sortie des résultats
OUTPUT_DIR: str = "output"

# Nom du fichier de sortie
OUTPUT_FILE: str = "weather_data.csv"