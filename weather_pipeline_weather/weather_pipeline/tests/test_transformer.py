"""
Tests unitaires pour le module transformer.

Pour executer les tests :
    pytest tests/ -v

Pour executer avec couverture :
    pytest tests/ --cov=src --cov-report=html
"""

import pytest
from datetime import datetime

import pandas as pd

# Import du module a tester
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transformer import WeatherTransformer, WeatherRecord


class TestWeatherRecord:
    """Tests pour la dataclass WeatherRecord."""
    
    def test_create_record(self):
        """Test de creation d'un enregistrement."""
        record = WeatherRecord(
            city="Paris",
            country="FR",
            temperature=20.5,
            feels_like=19.8,
            humidity=65,
            pressure=1015,
            wind_speed=3.5,
            description="clear sky",
            timestamp=datetime.now()
        )
        
        assert record.city == "Paris"
        assert record.temperature == 20.5
        assert record.country == "FR"
    
    def test_to_dict(self):
        """Test de conversion en dictionnaire."""
        record = WeatherRecord(
            city="London",
            country="GB",
            temperature=15.0,
            feels_like=14.0,
            humidity=70,
            pressure=1020,
            wind_speed=5.0,
            description="cloudy",
            timestamp=datetime(2024, 1, 15, 10, 30)
        )
        
        result = record.to_dict()
        
        assert isinstance(result, dict)
        assert result["city"] == "London"
        assert result["temperature"] == 15.0


class TestWeatherTransformer:
    """Tests pour le transformateur de donnees meteo."""
    
    @pytest.fixture
    def transformer(self):
        """Fixture : cree un transformer pour chaque test."""
        return WeatherTransformer()
    
    @pytest.fixture
    def valid_api_response(self):
        """Fixture : reponse API valide."""
        return {
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
    
    def test_parse_single_valid_data(self, transformer, valid_api_response):
        """Test avec des donnees valides."""
        result = transformer.parse_single(valid_api_response)
        
        assert result is not None
        assert result.city == "Paris"
        assert result.temperature == 20.5
        assert result.country == "FR"
        assert result.humidity == 65
        assert result.description == "clear sky"
    
    def test_parse_single_empty_data(self, transformer):
        """Test avec des donnees vides."""
        result = transformer.parse_single({})
        
        # Doit gerer gracieusement avec valeurs par defaut
        assert result is not None
        assert result.city == "Unknown"
        assert result.country == "??"
    
    def test_parse_single_partial_data(self, transformer):
        """Test avec des donnees partielles."""
        partial_data = {
            "name": "Tokyo",
            "sys": {},
            "main": {"temp": 25.0}
        }
        
        result = transformer.parse_single(partial_data)
        
        assert result is not None
        assert result.city == "Tokyo"
        assert result.temperature == 25.0
        assert result.country == "??"  # Valeur par defaut
    
    def test_transform_empty_list(self, transformer):
        """Test avec une liste vide."""
        df = transformer.transform([])
        
        assert df.empty
        assert isinstance(df, pd.DataFrame)
    
    def test_transform_valid_list(self, transformer, valid_api_response):
        """Test avec une liste valide."""
        data_list = [valid_api_response]
        
        df = transformer.transform(data_list)
        
        assert not df.empty
        assert len(df) == 1
        assert "city" in df.columns
        assert "temperature" in df.columns
        assert "extracted_at" in df.columns
        assert df.iloc[0]["city"] == "Paris"
    
    def test_transform_multiple_cities(self, transformer):
        """Test avec plusieurs villes."""
        data_list = [
            {
                "name": "Paris",
                "sys": {"country": "FR"},
                "main": {"temp": 20.0, "feels_like": 19.0, "humidity": 60, "pressure": 1015},
                "wind": {"speed": 3.0},
                "weather": [{"description": "sunny"}],
                "dt": 1700000000
            },
            {
                "name": "London",
                "sys": {"country": "GB"},
                "main": {"temp": 15.0, "feels_like": 14.0, "humidity": 70, "pressure": 1020},
                "wind": {"speed": 5.0},
                "weather": [{"description": "cloudy"}],
                "dt": 1700000000
            }
        ]
        
        df = transformer.transform(data_list)
        
        assert len(df) == 2
        # Verifie le tri alphabetique
        assert df.iloc[0]["city"] == "London"
        assert df.iloc[1]["city"] == "Paris"
    
    def test_transform_rounds_temperatures(self, transformer):
        """Test que les temperatures sont arrondies."""
        data = {
            "name": "Test",
            "sys": {"country": "XX"},
            "main": {"temp": 20.567, "feels_like": 19.123, "humidity": 60, "pressure": 1015},
            "wind": {"speed": 3.789},
            "weather": [{"description": "test"}],
            "dt": 1700000000
        }
        
        df = transformer.transform([data])
        
        assert df.iloc[0]["temperature"] == 20.6
        assert df.iloc[0]["feels_like"] == 19.1
        assert df.iloc[0]["wind_speed"] == 3.8


class TestEdgeCases:
    """Tests pour les cas limites."""
    
    def test_none_values_in_response(self):
        """Test avec des valeurs None dans la reponse."""
        transformer = WeatherTransformer()
        data = {
            "name": None,
            "sys": None,
            "main": None,
            "wind": None,
            "weather": None,
            "dt": None
        }
        
        result = transformer.parse_single(data)
        # Le parsing doit echouer gracieusement
        # car .get() sur None leve une exception
        assert result is None or result.city == "Unknown"
    
    def test_weather_list_empty(self):
        """Test avec une liste weather vide."""
        transformer = WeatherTransformer()
        data = {
            "name": "Test",
            "sys": {"country": "XX"},
            "main": {"temp": 20.0, "feels_like": 19.0, "humidity": 60, "pressure": 1015},
            "wind": {"speed": 3.0},
            "weather": [],  # Liste vide
            "dt": 1700000000
        }
        
        result = transformer.parse_single(data)
        
        # Doit gerer la liste vide
        assert result is not None
        assert result.description == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
