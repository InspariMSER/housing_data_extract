import requests
import pandas as pd

# Define token

token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb21haW51c2VyIjoiYW5vbnltb3VzIiwidG9rZW5pZCI6ImQ5OTQ5MGFlLTBhN2UtNDQzNS05MmYwLTMxYjcyNzgxMjZjNyIsInVzZXJpZCI6IjlkNjRjNWE4LTZjYzUtNDgyZS1hZjg1LWQ0M2FlMGRiNDcyMCIsImV4cCI6MTgzMjkyOTE5NiwiaXNzIjoiaHR0cHM6Ly9kb3RuZXRkZXRhaWwubmV0IiwiYXVkIjoiaHR0cHM6Ly9kb3RuZXRkZXRhaWwubmV0In0.U__S8TCiZFehmF6yEZNJaLaVzjzl7zeaF8jE8dIBV7I'

# Define the API endpoint and headers
url = "https://api.uddannelsesstatistik.dk/Api/v1/statistik"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}"
}

# Define the request body
body = {
    "område": "GS",
    "emne": "OVER",
    "underemne": "OVERSKO",
    "nøgletal": [
        "Karaktergennemsnit",
        "Samlet elevfravær",
        "Andel med højest trivsel"
    ],
    "detaljering": [
        "[Institution].[Afdeling]",
        "[Institution].[Afdelingstype]",
        "[Institution].[Beliggenhedskommune]",
        "[Tid].[Skoleår]"
    ],
    "indlejret": False,
    "tomme_rækker": False,
    "formattering": "json",
    "side": 1
}

# Make the POST request
response = requests.post(url, headers=headers, json=body)

# Check if the request was successful
if response.status_code == 200:
    print("Request successful!")
    data = response.json()
    
    # Convert JSON response to a Pandas DataFrame
    df = pd.DataFrame(data)
    
    # Define the path to write the data to    
    print(df)
else:
    print(f"Request failed with status code {response.status_code}")
