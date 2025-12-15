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