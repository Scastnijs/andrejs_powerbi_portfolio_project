from datetime import datetime
import json

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook


def chat_with_ollama():
    hook = HttpHook(
        method="POST",
        http_conn_id="Ollama"
    )

    payload = {
        "model": "gemma4",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant."
            }
        ]
    }

    response = hook.run(
        endpoint="/api/chat",
        json=payload,
        extra_options={"stream": True}
    )

    if response.status_code != 200:
        raise Exception(
            f"Request failed: {response.status_code}\n{response.text}"
        )

    print("Streaming response:")

    for line in response.iter_lines(decode_unicode=True):
        if line:
            try:
                json_data = json.loads(line)

                if (
                    "message" in json_data
                    and "content" in json_data["message"]
                ):
                    print(
                        json_data["message"]["content"],
                        end="",
                        flush=True
                    )

            except json.JSONDecodeError:
                print(f"Failed to decode JSON: {line}")

    print()


with DAG(
    dag_id="ollama_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["http", "ollama"],
) as dag:

    ollama_task = PythonOperator(
        task_id="chat_with_ollama",
        python_callable=chat_with_ollama,
    )