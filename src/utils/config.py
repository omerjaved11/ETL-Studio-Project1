import yaml
import os
from pathlib import Path

# Get project root: .../Revature-Cognizant-Project1
PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

def load_config():
    config_path = os.path.join(CONFIG_PATH)
    with open(config_path,"r") as file:
        return yaml.safe_load(file)
    
config = load_config()
