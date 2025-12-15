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