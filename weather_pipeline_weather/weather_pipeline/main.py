"""
Point d'entree du pipeline meteo.

POURQUOI UN main.py SEPARE ?
- Separation entre le code metier (src/) et l'execution
- Facilite les tests (on peut importer sans executer)
- Point d'entree clair pour les operations

Usage:
    python main.py
"""

import os
import sys
import logging

# Ajouter le dossier racine au path Python
# Necessaire pour que les imports fonctionnent
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from src.pipeline import WeatherPipeline


def setup_logging():
    """
    Configure le systeme de logging.
    
    Deux handlers :
    1. Console : pour voir en temps reel
    2. Fichier : pour garder l'historique
    """
    # Creer le dossier logs si necessaire
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
    
    # Reduire le bruit des bibliotheques externes
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    """
    Fonction principale.
    
    Returns:
        int: Code de sortie (0 = succes, 1 = erreur)
    """
    # Configurer le logging en premier
    setup_logging()
    
    logger = logging.getLogger(__name__)
    logger.info("Demarrage de l'application")
    
    # Verifier la cle API
    if settings.API_KEY == "demo_key_replace_me":
        logger.warning("=" * 60)
        logger.warning("ATTENTION : Cle API non configuree !")
        logger.warning("Configurez votre cle API OpenWeatherMap :")
        logger.warning("  1. Creez un compte sur openweathermap.org")
        logger.warning("  2. Modifiez config/settings.py")
        logger.warning("  3. Ou definissez OPENWEATHER_API_KEY")
        logger.warning("=" * 60)
        logger.warning("Le pipeline va continuer mais echouera...")
    
    try:
        # Creer et executer le pipeline
        pipeline = WeatherPipeline()
        result = pipeline.run()
        
        if result is not None:
            # Afficher un apercu des resultats
            print("\n" + "=" * 60)
            print("APERCU DES RESULTATS")
            print("=" * 60)
            
            # Colonnes a afficher
            display_cols = ["city", "country", "temperature", "humidity", "description"]
            available_cols = [c for c in display_cols if c in result.columns]
            
            print(result[available_cols].to_string(index=False))
            print("=" * 60)
            
            return 0  # Code de sortie : succes
        else:
            logger.error("Le pipeline n'a produit aucun resultat")
            return 1  # Code de sortie : echec
            
    except KeyboardInterrupt:
        logger.info("Arret demande par l'utilisateur")
        return 130
        
    except Exception as e:
        logger.critical(f"Erreur critique : {e}")
        logger.exception("Details de l'erreur :")
        return 1


# Point d'entree standard Python
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
