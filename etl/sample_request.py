import requests
import json

url = "http://localhost:11434/api/chat"

payload = {
    "model": "llama3",
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
