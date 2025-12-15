"""
Module de transformation des donnees meteo.

RESPONSABILITE : Transformer les donnees brutes de l'API
en format propre et exploitable (DataFrame pandas).
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class WeatherRecord:
    """
    Structure de donnees pour une observation meteo.
    
    Utilise une dataclass pour :
    - Typage fort (erreurs detectees par l'IDE)
    - Code plus lisible et maintenable
    - Conversion facile en dictionnaire
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
        return asdict(self)


class WeatherTransformer:
    """
    Transformateur de donnees meteo.
    
    Convertit les donnees brutes de l'API en DataFrame propre.
    """
    
    def parse_single(self, raw_data: Dict[str, Any]) -> Optional[WeatherRecord]:
        """
        Parse une reponse API en WeatherRecord.
        
        Args:
            raw_data: Donnees brutes de l'API OpenWeatherMap
            
        Returns:
            WeatherRecord ou None si parsing echoue
            
        Note:
            Structure attendue de l'API :
            {
                "name": "Paris",
                "sys": {"country": "FR"},
                "main": {"temp": 20.5, "feels_like": 19.8, ...},
                "wind": {"speed": 3.5},
                "weather": [{"description": "clear sky"}],
                "dt": 1234567890
            }
        """
        try:
            # Extraction securisee avec valeurs par defaut
            record = WeatherRecord(
                city=raw_data.get("name", "Unknown"),
                country=raw_data.get("sys", {}).get("country", "??"),
                temperature=float(raw_data.get("main", {}).get("temp", 0.0)),
                feels_like=float(raw_data.get("main", {}).get("feels_like", 0.0)),
                humidity=int(raw_data.get("main", {}).get("humidity", 0)),
                pressure=int(raw_data.get("main", {}).get("pressure", 0)),
                wind_speed=float(raw_data.get("wind", {}).get("speed", 0.0)),
                description=raw_data.get("weather", [{}])[0].get("description", ""),
                timestamp=datetime.fromtimestamp(raw_data.get("dt", 0))
            )
            
            logger.debug(f"Parsing reussi pour {record.city}")
            return record
            
        except (KeyError, IndexError, TypeError, ValueError) as e:
            logger.error(f"Erreur de parsing : {e}")
            logger.debug(f"Donnees problematiques : {raw_data}")
            return None
    
    def transform(self, raw_data_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Transforme une liste de donnees brutes en DataFrame.
        
        Args:
            raw_data_list: Liste des reponses API
            
        Returns:
            DataFrame pandas avec les donnees nettoyees
        """
        if not raw_data_list:
            logger.warning("Aucune donnee a transformer")
            return pd.DataFrame()
        
        logger.info(f"Transformation de {len(raw_data_list)} enregistrements")
        
        # Parser chaque element
        records = []
        for raw_data in raw_data_list:
            record = self.parse_single(raw_data)
            if record:
                records.append(record.to_dict())
        
        if not records:
            logger.warning("Aucun enregistrement valide apres parsing")
            return pd.DataFrame()
        
        # Creer le DataFrame
        df = pd.DataFrame(records)
        
        # Nettoyage et enrichissement
        df = self._clean_dataframe(df)
        
        logger.info(f"Transformation terminee : {len(df)} lignes")
        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie et enrichit le DataFrame.
        
        Operations :
        1. Arrondir les valeurs numeriques
        2. Ajouter des colonnes calculees
        3. Trier les donnees
        
        Args:
            df: DataFrame brut
            
        Returns:
            DataFrame nettoye
        """
        # Copie pour eviter les warnings pandas
        df = df.copy()
        
        # Arrondir les temperatures a 1 decimale
        df["temperature"] = df["temperature"].round(1)
        df["feels_like"] = df["feels_like"].round(1)
        df["wind_speed"] = df["wind_speed"].round(1)
        
        # Ajouter une colonne "date d'extraction"
        df["extracted_at"] = datetime.now()
        
        # Trier par ville alphabetiquement
        df = df.sort_values("city").reset_index(drop=True)
        
        logger.debug("DataFrame nettoye et enrichi")
        return df
