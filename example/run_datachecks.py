from datachecks import Inspect, load_configuration
from datachecks import Configuration
import requests


if __name__ == "__main__":

    #Reding Config File
    configuration: Configuration = load_configuration('config.yaml')

    # Generating Metrics
    metrics = Inspect(configuration).start()

    # Sending to ELK
    for metric in metrics:
        print(metric)
        requests.post('https://localhost:9201/example_indices/_doc', json=metric, auth=("admin", "admin"), verify=False)
