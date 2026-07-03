from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import json

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def ollama_test():

    url = "http://localhost:11434/api/chat"

    payload = {
        "model": "gemma4",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant."
            }
        ]
    }

    response = requests.post(url, json=payload, stream=True)

    if response.status_code == 200:
        print("Streaming response:")
        for line in response.iter_lines(decode_unicode=True):
            if line:
                try:
                    json_data = json.loads(line)
                    if "message" in json_data and "content" in json_data["message"]:
                        print(json_data["message"]["content"], end='', flush=True)
                except json.JSONDecodeError:
                    print(f"Failed to decode JSON: {line}")
        print()
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)

dag = ollama_test()