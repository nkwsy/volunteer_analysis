import os
import sys
import subprocess
import logging
from dotenv import load_dotenv

# Add the project root to Python path to make imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

def main():
    """
    Launch the Volunteer Analysis Dashboard application.
    
    This script checks for required dependencies and launches the Streamlit app.
    """
    try:
        # Check if we're in a virtual environment
        in_venv = sys.prefix != sys.base_prefix
        if not in_venv:
            logging.warning("Not running in a virtual environment. It's recommended to use a virtual environment.")
        
        # Launch the Streamlit app
        logging.info("Starting Volunteer Analysis Dashboard...")
        subprocess.run(["streamlit", "run", "src/app.py"])
        
    except Exception as e:
        logging.error(f"Error launching application: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 