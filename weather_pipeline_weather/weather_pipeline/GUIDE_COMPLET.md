# GUIDE COMPLET : Weather Pipeline
## Projet Data Engineering - Extraction API

---

# TABLE DES MATIÈRES

1. [Introduction et Objectifs](#1-introduction-et-objectifs)
2. [Architecture du Projet](#2-architecture-du-projet)
3. [Concepts Clés à Maîtriser](#3-concepts-clés-à-maîtriser)
4. [Fichier par Fichier : Explications Détaillées](#4-fichier-par-fichier--explications-détaillées)
5. [Flux d'Exécution du Pipeline](#5-flux-dexécution-du-pipeline)
6. [Gestion des Erreurs en Entreprise](#6-gestion-des-erreurs-en-entreprise)
7. [Tests Unitaires](#7-tests-unitaires)
8. [Bonnes Pratiques Professionnelles](#8-bonnes-pratiques-professionnelles)

---

# 1. INTRODUCTION ET OBJECTIFS

## 1.1 Contexte Métier

En entreprise, les data engineers doivent souvent :
- **Extraire** des données depuis des sources externes (APIs, bases de données)
- **Transformer** ces données en format exploitable
- **Charger** les résultats pour les équipes (dashboards, rapports)

C'est le pattern **ETL** (Extract, Transform, Load).

## 1.2 Notre Mission

```
Extraire la météo de 5 villes via l'API OpenWeatherMap
         ↓
Transformer en DataFrame propre
         ↓
Sauvegarder pour un dashboard interne
```

## 1.3 Critères de Qualité Professionnelle

| Critère | Pourquoi c'est important |
|---------|--------------------------|
| **Robustesse** | Le code ne doit pas planter si l'API est lente ou down |
| **Logging** | On doit savoir ce qui s'est passé (succès, erreurs) |
| **Tests** | Le code doit être vérifié automatiquement |
| **Maintenabilité** | Un autre développeur doit comprendre le code |

---

# 2. ARCHITECTURE DU PROJET

## 2.1 Structure des Dossiers

```
weather_pipeline/
│
├── main.py                 # Point d'entrée unique
│
├── config/
│   ├── __init__.py
│   └── settings.py         # Toute la configuration
│
├── src/
│   ├── __init__.py         # Expose les classes publiques
│   ├── api_client.py       # Connexion à l'API
│   ├── extractor.py        # Logique d'extraction
│   ├── transformer.py      # Transformation des données
│   └── pipeline.py         # Orchestration
│
├── tests/
│   ├── __init__.py
│   └── test_transformer.py # Tests unitaires
│
├── logs/                   # Fichiers de logs
│   └── pipeline.log
│
├── output/                 # Résultats
│   └── weather_data.csv
│
├── requirements.txt        # Dépendances Python
└── README.md              # Documentation
```

## 2.2 Principe de Séparation des Responsabilités

**Règle d'or en entreprise : 1 fichier = 1 responsabilité**

| Fichier | Responsabilité unique |
|---------|----------------------|
| `settings.py` | Gérer la configuration |
| `api_client.py` | Communiquer avec l'API |
| `extractor.py` | Extraire les données |
| `transformer.py` | Nettoyer/transformer les données |
| `pipeline.py` | Orchestrer les étapes |

**Pourquoi ?**
- Plus facile à **tester** (on teste chaque partie isolément)
- Plus facile à **maintenir** (on sait où chercher)
- Plus facile à **modifier** (on change une partie sans casser les autres)

## 2.3 Flux de Données

```
┌─────────────────────────────────────────────────────────────────┐
│                         main.py                                  │
│                    (Point d'entrée)                             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      pipeline.py                                 │
│              (Orchestre toutes les étapes)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │ api_client  │    │  extractor  │    │ transformer │
   │             │◄───│             │───►│             │
   │ (Connexion) │    │(Extraction) │    │(Nettoyage)  │
   └─────────────┘    └─────────────┘    └─────────────┘
          │
          ▼
   ┌─────────────┐
   │ OpenWeather │
   │     API     │
   └─────────────┘
```

---

# 3. CONCEPTS CLÉS À MAÎTRISER

## 3.1 Les APIs REST

### Qu'est-ce qu'une API ?

Une **API** (Application Programming Interface) est comme un serveur de restaurant :
- Tu fais une **demande** (requête HTTP)
- Le serveur va chercher en cuisine (serveur distant)
- Il te ramène le **résultat** (réponse JSON)

### Anatomie d'une Requête API

```
GET https://api.openweathermap.org/data/2.5/weather?q=Paris&appid=ABC123
│   │                                              │        │
│   │                                              │        └── Paramètre : clé API
│   │                                              └── Paramètre : ville
│   └── URL de base de l'API
└── Méthode HTTP (GET = récupérer des données)
```

### Codes de Réponse HTTP

| Code | Signification | Action à prendre |
|------|---------------|------------------|
| 200 | Succès | Traiter les données |
| 401 | Non autorisé | Vérifier la clé API |
| 404 | Non trouvé | Vérifier la ville |
| 429 | Trop de requêtes | Attendre (rate limit) |
| 500 | Erreur serveur | Réessayer plus tard |

## 3.2 Le Logging (Journalisation)

### Pourquoi logger ?

En production, tu ne peux pas utiliser `print()`. Pourquoi ?
- Les `print()` disparaissent quand le programme s'arrête
- Impossible de filtrer par importance
- Pas de timestamp (quand ça s'est passé)

### Les Niveaux de Log

```python
import logging

logging.debug("Détail technique")      # Pour débugger
logging.info("Le pipeline démarre")    # Information normale
logging.warning("API lente")           # Attention, mais pas bloquant
logging.error("Échec de connexion")    # Erreur, mais on continue
logging.critical("Base de données down") # Erreur fatale
```

### Hiérarchie des Niveaux

```
DEBUG < INFO < WARNING < ERROR < CRITICAL
  │      │       │        │        │
  │      │       │        │        └── Le plus grave
  │      │       │        └── Erreur importante
  │      │       └── Avertissement
  │      └── Information normale
  └── Le plus détaillé (pour débugger)
```

**En production**, on configure souvent le niveau `INFO` ou `WARNING` pour ne pas être noyé de messages.

## 3.3 Le Pattern Retry (Réessai)

### Le Problème

Les APIs peuvent être :
- **Lentes** (réseau surchargé)
- **Temporairement indisponibles** (maintenance)
- **Limitées** (rate limiting)

### La Solution : Retry avec Backoff

```
1ère tentative : Échec
    ↓ (attendre 2 secondes)
2ème tentative : Échec
    ↓ (attendre 4 secondes) ← Backoff exponentiel
3ème tentative : Succès !
```

### Pourquoi le "Backoff Exponentiel" ?

On attend de plus en plus longtemps entre chaque tentative :
- 1ère attente : 2 secondes
- 2ème attente : 4 secondes
- 3ème attente : 8 secondes

**Avantage** : On laisse le temps à l'API de se remettre sans la surcharger.

## 3.4 Les Type Hints (Annotations de Types)

### Qu'est-ce que c'est ?

```python
# Sans type hints (ambigu)
def get_weather(city):
    pass

# Avec type hints (clair)
def get_weather(city: str) -> dict:
    pass
```

### Pourquoi c'est important en entreprise ?

1. **Documentation automatique** : On sait ce que la fonction attend/retourne
2. **Détection d'erreurs** : Les IDE détectent les erreurs avant l'exécution
3. **Maintenabilité** : Un nouveau développeur comprend vite le code

### Types Courants

```python
from typing import List, Dict, Optional, Any

# Types simples
nom: str = "Paris"
temperature: float = 20.5
actif: bool = True

# Types composés
villes: List[str] = ["Paris", "London"]
meteo: Dict[str, Any] = {"temp": 20, "ville": "Paris"}

# Type optionnel (peut être None)
resultat: Optional[str] = None
```

## 3.5 Les Dataclasses

### Le Problème avec les Dictionnaires

```python
# Avec un dictionnaire (risque d'erreurs)
meteo = {"temp": 20, "ville": "Paris", "humidity": 80}
print(meteo["temperatue"])  # Erreur de typo non détectée !
```

### La Solution : Dataclass

```python
from dataclasses import dataclass

@dataclass
class WeatherData:
    city: str
    temperature: float
    humidity: int

# Utilisation (erreurs détectées par l'IDE)
meteo = WeatherData(city="Paris", temperature=20.0, humidity=80)
print(meteo.temperatue)  # L'IDE signale l'erreur !
```

### Avantages des Dataclasses

| Avantage | Explication |
|----------|-------------|
| **Typage** | L'IDE détecte les erreurs |
| **Lisibilité** | On sait exactement ce que contient l'objet |
| **Immutabilité** | On peut rendre l'objet non modifiable |
| **Comparaison** | Deux objets identiques sont égaux automatiquement |

---

# 4. FICHIER PAR FICHIER : EXPLICATIONS DÉTAILLÉES

## 4.1 `config/settings.py` - Configuration Centralisée

### Objectif
Centraliser TOUS les paramètres du projet en un seul endroit.

### Code Commenté

```python
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
API_KEY: str = "votre_cle_api_ici"

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
```

### Points Clés à Retenir

1. **Variables d'environnement** : En production, les secrets (clés API) ne sont JAMAIS dans le code
2. **Types explicites** : Chaque variable a son type annoté
3. **Commentaires** : Chaque section est expliquée
4. **Valeurs par défaut** : Tout a une valeur sensée

---

## 4.2 `src/api_client.py` - Client API Robuste

### Objectif
Gérer toute la communication avec l'API OpenWeatherMap de manière robuste.

### Code Commenté

```python
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
```

### Points Clés à Retenir

1. **Session requests** : Plus performant que des requêtes individuelles
2. **Retry avec backoff** : On réessaie avec des délais croissants
3. **Gestion fine des erreurs** : Chaque type d'erreur a son traitement
4. **Logging contextuel** : On sait exactement ce qui se passe

---

## 4.3 `src/extractor.py` - Extraction des Données

### Objectif
Orchestrer l'extraction des données pour plusieurs villes.

### Code Commenté

```python
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
```

### Points Clés à Retenir

1. **Injection de dépendances** : Le client est fourni de l'extérieur
2. **Délai entre requêtes** : Respecter les limites de l'API
3. **Comptage des résultats** : Savoir combien ont réussi/échoué

---

## 4.4 `src/transformer.py` - Transformation des Données

### Objectif
Transformer les données brutes JSON en DataFrame propre et exploitable.

### Code Commenté

```python
"""
Module de transformation des données météo.

RESPONSABILITÉ : Transformer les données brutes de l'API
en format propre et exploitable (DataFrame pandas).
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class WeatherRecord:
    """
    Structure de données pour une observation météo.
    
    POURQUOI UNE DATACLASS ?
    - Typage fort (erreurs détectées à l'avance)
    - Code plus lisible
    - Validation automatique des types
    """
    city: str
    country: str
    temperature: float
    feels_like: float
    humidity: int
    pressure: int
    wind_speed: float
    description: str
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire pour pandas."""
        return {
            "city": self.city,
            "country": self.country,
            "temperature": self.temperature,
            "feels_like": self.feels_like,
            "humidity": self.humidity,
            "pressure": self.pressure,
            "wind_speed": self.wind_speed,
            "description": self.description,
            "timestamp": self.timestamp
        }


class WeatherTransformer:
    """
    Transformateur de données météo.
    
    Convertit les données brutes de l'API en DataFrame propre.
    """
    
    def parse_single(self, raw_data: Dict[str, Any]) -> Optional[WeatherRecord]:
        """
        Parse une réponse API en WeatherRecord.
        
        Args:
            raw_data: Données brutes de l'API
            
        Returns:
            WeatherRecord ou None si parsing échoue
            
        STRUCTURE DE L'API OpenWeatherMap :
        {
            "name": "Paris",
            "sys": {"country": "FR"},
            "main": {
                "temp": 20.5,
                "feels_like": 19.8,
                "humidity": 65,
                "pressure": 1015
            },
            "wind": {"speed": 3.5},
            "weather": [{"description": "clear sky"}],
            "dt": 1234567890  # Timestamp Unix
        }
        """
        try:
            # Extraction des données avec gestion des clés manquantes
            record = WeatherRecord(
                city=raw_data.get("name", "Unknown"),
                country=raw_data.get("sys", {}).get("country", "??"),
                temperature=raw_data.get("main", {}).get("temp", 0.0),
                feels_like=raw_data.get("main", {}).get("feels_like", 0.0),
                humidity=raw_data.get("main", {}).get("humidity", 0),
                pressure=raw_data.get("main", {}).get("pressure", 0),
                wind_speed=raw_data.get("wind", {}).get("speed", 0.0),
                description=raw_data.get("weather", [{}])[0].get("description", ""),
                timestamp=datetime.fromtimestamp(raw_data.get("dt", 0))
            )
            
            logger.debug(f"Parsing réussi pour {record.city}")
            return record
            
        except (KeyError, IndexError, TypeError) as e:
            # Erreur de structure des données
            logger.error(f"Erreur de parsing : {e}")
            return None
    
    def transform(self, raw_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transforme une liste de données brutes en DataFrame.
        
        Args:
            raw_data_list: Liste des réponses API
            
        Returns:
            DataFrame pandas avec les données nettoyées
        """
        if not raw_data_list:
            logger.warning("Aucune donnée à transformer")
            return pd.DataFrame()
        
        # Parser chaque élément
        records = []
        for raw_data in raw_data_list:
            record = self.parse_single(raw_data)
            if record:
                records.append(record.to_dict())
        
        # Créer le DataFrame
        df = pd.DataFrame(records)
        
        if df.empty:
            logger.warning("DataFrame vide après transformation")
            return df
        
        # Nettoyage et enrichissement
        df = self._clean_dataframe(df)
        
        logger.info(f"Transformation terminée : {len(df)} lignes")
        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie et enrichit le DataFrame.
        
        OPÉRATIONS :
        1. Arrondir les valeurs numériques
        2. Ajouter des colonnes calculées
        3. Trier les données
        """
        # Arrondir les températures à 1 décimale
        df["temperature"] = df["temperature"].round(1)
        df["feels_like"] = df["feels_like"].round(1)
        df["wind_speed"] = df["wind_speed"].round(1)
        
        # Ajouter une colonne "date d'extraction"
        df["extracted_at"] = datetime.now()
        
        # Trier par ville
        df = df.sort_values("city").reset_index(drop=True)
        
        return df
```

### Points Clés à Retenir

1. **Dataclass** : Structure claire pour les données
2. **Gestion des clés manquantes** : `.get()` avec valeurs par défaut
3. **Méthodes privées** : `_clean_dataframe` est interne (convention `_`)
4. **Enrichissement** : Ajout de colonnes calculées

---

## 4.5 `src/pipeline.py` - Orchestration

### Objectif
Orchestrer toutes les étapes du pipeline (ETL).

### Code Commenté

```python
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
```

### Points Clés à Retenir

1. **Orchestration** : Le pipeline coordonne sans implémenter
2. **Try/Finally** : Les ressources sont libérées même en cas d'erreur
3. **Mesure du temps** : Utile pour le monitoring
4. **Logging structuré** : On sait exactement où on en est

---

## 4.6 `main.py` - Point d'Entrée

### Objectif
Point d'entrée unique pour exécuter le pipeline.

### Code Commenté

```python
"""
Point d'entrée du pipeline météo.

POURQUOI UN main.py SÉPARÉ ?
- Séparation entre le code métier (src/) et l'exécution
- Facilite les tests (on peut importer sans exécuter)
- Point d'entrée clair pour les opérations
"""

import os
import sys
import logging

# Ajouter le dossier racine au path Python
# Nécessaire pour que les imports fonctionnent
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from src.pipeline import WeatherPipeline


def setup_logging():
    """
    Configure le système de logging.
    
    DEUX HANDLERS :
    1. Console : pour voir en temps réel
    2. Fichier : pour garder l'historique
    """
    # Créer le dossier logs si nécessaire
    log_dir = os.path.dirname(settings.LOG_FILE)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # Configuration du logging
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format=settings.LOG_FORMAT,
        handlers=[
            # Handler console
            logging.StreamHandler(sys.stdout),
            # Handler fichier
            logging.FileHandler(settings.LOG_FILE, encoding="utf-8")
        ]
    )
    
    # Réduire le bruit des bibliothèques externes
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    """Fonction principale."""
    # Configurer le logging en premier
    setup_logging()
    
    logger = logging.getLogger(__name__)
    logger.info("Démarrage de l'application")
    
    try:
        # Créer et exécuter le pipeline
        pipeline = WeatherPipeline()
        result = pipeline.run()
        
        if result is not None:
            # Afficher un aperçu des résultats
            print("\n" + "=" * 60)
            print("APERÇU DES RÉSULTATS")
            print("=" * 60)
            print(result.to_string(index=False))
            
            return 0  # Code de sortie : succès
        else:
            return 1  # Code de sortie : échec
            
    except Exception as e:
        logger.critical(f"Erreur critique : {e}")
        return 1


# Point d'entrée standard Python
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
```

### Points Clés à Retenir

1. **`if __name__ == "__main__"`** : Le code ne s'exécute que si on lance directement ce fichier
2. **Codes de sortie** : 0 = succès, 1 = erreur (convention Unix)
3. **Setup logging** : Toujours configurer les logs en premier
4. **Gestion des exceptions** : Capturer les erreurs fatales

---

# 5. FLUX D'EXÉCUTION DU PIPELINE

## 5.1 Schéma Complet

```
[Utilisateur]
     │
     │  python main.py
     ▼
┌─────────────────────────────────────────────────────────────────┐
│                           main.py                                │
│  1. Configure le logging                                        │
│  2. Crée le WeatherPipeline                                     │
│  3. Appelle pipeline.run()                                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      WeatherPipeline.run()                       │
└─────────────────────────────────────────────────────────────────┘
     │                        │                        │
     │ EXTRACT                │ TRANSFORM              │ LOAD
     ▼                        ▼                        ▼
┌───────────┐          ┌───────────┐          ┌───────────┐
│ Extractor │          │Transformer│          │  to_csv() │
│           │          │           │          │           │
│ Pour chaque ville :  │ Pour chaque│          │ Sauvegarde│
│ - Appelle API        │ réponse :  │          │ le CSV    │
│ - Gère les erreurs   │ - Parse    │          │           │
│ - Retry si échec     │ - Nettoie  │          │           │
└───────────┘          └───────────┘          └───────────┘
     │
     ▼
┌───────────┐
│ APIClient │
│           │
│ - Session │
│ - Timeout │
│ - Retry   │
└───────────┘
     │
     ▼
[API OpenWeatherMap]
```

## 5.2 Exemple de Log d'Exécution

```
2024-01-15 10:30:00 | INFO     | __main__ | Démarrage de l'application
2024-01-15 10:30:00 | INFO     | src.pipeline | Pipeline initialisé
2024-01-15 10:30:00 | INFO     | src.api_client | Client API initialisé
2024-01-15 10:30:00 | INFO     | src.pipeline | ============================================================
2024-01-15 10:30:00 | INFO     | src.pipeline | DÉMARRAGE DU PIPELINE MÉTÉO
2024-01-15 10:30:00 | INFO     | src.pipeline | ============================================================
2024-01-15 10:30:00 | INFO     | src.pipeline | ÉTAPE 1 : Extraction des données...
2024-01-15 10:30:00 | INFO     | src.extractor | Début extraction pour 5 villes
2024-01-15 10:30:01 | INFO     | src.api_client | Météo récupérée pour Paris
2024-01-15 10:30:02 | INFO     | src.api_client | Météo récupérée pour London
2024-01-15 10:30:03 | INFO     | src.api_client | Météo récupérée pour New York
2024-01-15 10:30:04 | INFO     | src.api_client | Météo récupérée pour Tokyo
2024-01-15 10:30:05 | INFO     | src.api_client | Météo récupérée pour Sydney
2024-01-15 10:30:05 | INFO     | src.extractor | Extraction terminée : 5 succès, 0 échecs
2024-01-15 10:30:05 | INFO     | src.pipeline | ÉTAPE 2 : Transformation des données...
2024-01-15 10:30:05 | INFO     | src.transformer | Transformation terminée : 5 lignes
2024-01-15 10:30:05 | INFO     | src.pipeline | ÉTAPE 3 : Sauvegarde des résultats...
2024-01-15 10:30:05 | INFO     | src.pipeline | Résultats sauvegardés : output/weather_data.csv
2024-01-15 10:30:05 | INFO     | src.pipeline | ============================================================
2024-01-15 10:30:05 | INFO     | src.pipeline | PIPELINE TERMINÉ AVEC SUCCÈS
2024-01-15 10:30:05 | INFO     | src.pipeline |   - Villes traitées : 5
2024-01-15 10:30:05 | INFO     | src.pipeline |   - Fichier généré : output/weather_data.csv
2024-01-15 10:30:05 | INFO     | src.pipeline |   - Durée : 5.23 secondes
2024-01-15 10:30:05 | INFO     | src.pipeline | ============================================================
```

---

# 6. GESTION DES ERREURS EN ENTREPRISE

## 6.1 Principe de Base

```
JAMAIS : try: ... except: pass  (avaler l'erreur silencieusement)
TOUJOURS : Logger l'erreur + Décider quoi faire
```

## 6.2 Types d'Erreurs et Traitements

| Type d'erreur | Exemple | Traitement |
|---------------|---------|------------|
| **Récupérable** | Timeout API | Retry avec backoff |
| **Non récupérable** | Clé API invalide | Arrêter et alerter |
| **Partielle** | Une ville non trouvée | Continuer les autres |
| **Fatale** | Plus de mémoire | Arrêter proprement |

## 6.3 Pattern Try/Except Professionnel

```python
# MAUVAIS
try:
    result = api.call()
except:
    pass  # On ne sait pas ce qui s'est passé !

# BON
try:
    result = api.call()
except requests.Timeout:
    logger.warning("Timeout, nouvelle tentative...")
    # Retry logic
except requests.HTTPError as e:
    if e.response.status_code == 401:
        logger.error("Clé API invalide")
        raise  # Erreur fatale, on remonte
    else:
        logger.warning(f"Erreur HTTP {e.response.status_code}")
except Exception as e:
    logger.error(f"Erreur inattendue: {e}")
    raise  # Erreur inconnue, on remonte pour investigation
```

---

# 7. TESTS UNITAIRES

## 7.1 Pourquoi Tester ?

| Raison | Explication |
|--------|-------------|
| **Confiance** | Savoir que le code fonctionne |
| **Régression** | Détecter si une modification casse quelque chose |
| **Documentation** | Les tests montrent comment utiliser le code |
| **Refactoring** | Modifier le code en sécurité |

## 7.2 Structure d'un Test

```python
def test_nom_explicite():
    """Description de ce qu'on teste."""
    # ARRANGE : Préparer les données
    input_data = {...}
    
    # ACT : Exécuter le code
    result = function_to_test(input_data)
    
    # ASSERT : Vérifier le résultat
    assert result == expected_value
```

## 7.3 Exemple de Test

```python
"""tests/test_transformer.py"""

import pytest
from src.transformer import WeatherTransformer


class TestWeatherTransformer:
    """Tests pour le transformateur de données météo."""
    
    def test_parse_single_valid_data(self):
        """Test avec des données valides."""
        # ARRANGE
        transformer = WeatherTransformer()
        raw_data = {
            "name": "Paris",
            "sys": {"country": "FR"},
            "main": {
                "temp": 20.5,
                "feels_like": 19.8,
                "humidity": 65,
                "pressure": 1015
            },
            "wind": {"speed": 3.5},
            "weather": [{"description": "clear sky"}],
            "dt": 1700000000
        }
        
        # ACT
        result = transformer.parse_single(raw_data)
        
        # ASSERT
        assert result is not None
        assert result.city == "Paris"
        assert result.temperature == 20.5
        assert result.country == "FR"
    
    def test_parse_single_missing_data(self):
        """Test avec des données manquantes."""
        # ARRANGE
        transformer = WeatherTransformer()
        raw_data = {}  # Données vides
        
        # ACT
        result = transformer.parse_single(raw_data)
        
        # ASSERT
        assert result is not None  # Doit gérer gracieusement
        assert result.city == "Unknown"
    
    def test_transform_empty_list(self):
        """Test avec une liste vide."""
        # ARRANGE
        transformer = WeatherTransformer()
        
        # ACT
        df = transformer.transform([])
        
        # ASSERT
        assert df.empty
```

## 7.4 Lancer les Tests

```bash
# Installer pytest
pip install pytest

# Lancer tous les tests
pytest tests/ -v

# Lancer avec couverture de code
pip install pytest-cov
pytest tests/ --cov=src --cov-report=html
```

---

# 8. BONNES PRATIQUES PROFESSIONNELLES

## 8.1 Conventions de Nommage

| Élément | Convention | Exemple |
|---------|------------|---------|
| Variables | snake_case | `user_name` |
| Fonctions | snake_case | `get_weather()` |
| Classes | PascalCase | `WeatherClient` |
| Constantes | UPPER_SNAKE | `MAX_RETRIES` |
| Fichiers | snake_case | `api_client.py` |

## 8.2 Structure d'un Module

```python
"""
Docstring du module : Description générale.
"""

# 1. Imports de la bibliothèque standard
import os
import sys
from datetime import datetime

# 2. Imports de bibliothèques tierces
import requests
import pandas as pd

# 3. Imports locaux
from config import settings
from src.utils import helper

# 4. Constantes
DEFAULT_TIMEOUT = 30

# 5. Classes et fonctions
class MyClass:
    pass

# 6. Point d'entrée (si applicable)
if __name__ == "__main__":
    main()
```

## 8.3 Docstrings

```python
def get_weather(city: str, units: str = "metric") -> dict:
    """
    Récupère les données météo d'une ville.
    
    Args:
        city: Nom de la ville (ex: "Paris", "London")
        units: Unités de mesure ("metric" ou "imperial")
        
    Returns:
        Dictionnaire contenant les données météo :
        - temperature: float
        - humidity: int
        - description: str
        
    Raises:
        APIError: Si la connexion à l'API échoue
        ValueError: Si la ville n'est pas trouvée
        
    Example:
        >>> data = get_weather("Paris")
        >>> print(data["temperature"])
        20.5
    """
    pass
```

## 8.4 Checklist Avant de Commiter

- [ ] Le code fonctionne (`python main.py`)
- [ ] Les tests passent (`pytest tests/`)
- [ ] Pas de secrets dans le code (clés API, mots de passe)
- [ ] Les imports sont propres
- [ ] Les fonctions ont des docstrings
- [ ] Les logs sont appropriés (pas de `print()`)
- [ ] Les erreurs sont gérées

---

# CONCLUSION

Ce projet t'a appris les bases d'un pipeline data professionnel :

1. **Architecture** : Séparer les responsabilités
2. **Robustesse** : Gérer les erreurs, retry, timeouts
3. **Logging** : Tracer ce qui se passe
4. **Tests** : Vérifier que le code fonctionne
5. **Configuration** : Centraliser les paramètres

Ces compétences sont essentielles pour tout data engineer en entreprise !
