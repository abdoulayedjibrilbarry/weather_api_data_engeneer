
import os 
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from src.pipeline import WeatherPipeline


def setup_logging():
    # création du dossier log
    
    log_dir = os.path.dirname(settings.LOG_FILE)

    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # Configuration du loggin
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL),
        format=settings.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(settings.LOG_FILE , encoding="utf-8")
        ]
    )
    
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

def main():
    
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Demarrage de l'application")

    
    try:
        pipeline = WeatherPipeline()
        result = pipeline.run()

        if result is not None:
            print("\n" + "=" * 60) 
            print("Appersu ")
            
            print(result.to_string(index=False))
            
            return 0
        else:
            return 1
    except Exception as e:
        logger.critical(f"Erreur critique : {e}")
        
        
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)