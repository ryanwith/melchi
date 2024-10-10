import yaml
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    def __init__(self, config_path):
        self.config_data = self.load_config(config_path)
        
        self.source_type = self.config_data['source']['type']
        self.target_type = self.config_data['target']['type']
        
        # Remove 'type' from source_config
        self.source_config = {k: v for k, v in self.config_data['source'].items() if k != 'type'}
        self.target_config = {k: v for k, v in self.config_data['target'].items() if k != 'type'}

    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        
        # Process the config to replace environment variables
        return Config._process_config(config)

    @staticmethod
    def _process_config(config):
        if isinstance(config, dict):
            return {k: Config._process_config(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [Config._process_config(v) for v in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            value = os.environ.get(env_var)
            if value is None:
                raise ValueError(f"Environment variable {env_var} is not set")
            return value
        else:
            return config

# Usage example
if __name__ == "__main__":
    config = Config("path/to/config.yaml")
    print(f"Source type: {config.source_type}")
    print(f"Target type: {config.target_type}")
    print(f"Source config: {config.source_config}")
    print(f"Target config: {config.target_config}")