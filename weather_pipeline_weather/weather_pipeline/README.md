# Weather Pipeline

Pipeline ETL pour extraire les donnees meteo depuis OpenWeatherMap.

## Installation

```bash
# 1. Creer un environnement virtuel
python -m venv venv

# 2. Activer l'environnement
# Windows :
venv\Scripts\activate
# Linux/Mac :
source venv/bin/activate

# 3. Installer les dependances
pip install -r requirements.txt
```

## Configuration

1. Creez un compte sur [OpenWeatherMap](https://openweathermap.org/)
2. Recuperez votre cle API gratuite
3. Configurez la cle dans `config/settings.py` :

```python
API_KEY = "votre_cle_api_ici"
```

Ou via variable d'environnement :

```bash
# Windows
set OPENWEATHER_API_KEY=votre_cle_api

# Linux/Mac
export OPENWEATHER_API_KEY=votre_cle_api
```

## Utilisation

```bash
python main.py
```

## Structure du Projet

```
weather_pipeline/
├── main.py              # Point d'entree
├── config/
│   └── settings.py      # Configuration
├── src/
│   ├── api_client.py    # Client API
│   ├── extractor.py     # Extraction
│   ├── transformer.py   # Transformation
│   └── pipeline.py      # Orchestration
├── tests/
│   └── test_transformer.py
├── logs/                # Fichiers de log
└── output/              # Resultats CSV
```

## Tests

```bash
# Lancer les tests
pytest tests/ -v

# Avec couverture
pytest tests/ --cov=src --cov-report=html
```

## Resultats

Les donnees sont sauvegardees dans `output/weather_data.csv` avec les colonnes :
- city : Nom de la ville
- country : Code pays
- temperature : Temperature en Celsius
- feels_like : Temperature ressentie
- humidity : Humidite (%)
- pressure : Pression atmospherique (hPa)
- wind_speed : Vitesse du vent (m/s)
- description : Description meteo
- timestamp : Date/heure de la mesure
- extracted_at : Date/heure d'extraction
