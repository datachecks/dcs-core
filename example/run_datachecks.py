from datachecks import Inspect, load_configuration
from datachecks import Configuration
import requests


if __name__ == "__main__":
    configuration: Configuration = load_configuration('config_v1.yaml')

    metrics = Inspect(configuration).start()
    for m in metrics:
        print(m)
        requests.post('https://localhost:9201/example_indices/_doc', json=m, auth=("admin", "admin"), verify=False)
