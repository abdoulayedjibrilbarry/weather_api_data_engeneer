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
    
