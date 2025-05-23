import requests

# Extrai valores das views (DataFrames)
email_value = id_email.select("email").collect()[0]["email"]
id_value = id_anexo.select("id").collect()[0]["id"]

API_ENDPOINT = "xxxx"

DATA = {
    "request": "bbbb",
    "password": "aaaa",
    "id": id_xxxx,
    "email": email_xxxx
}

response = requests.post(url=API_ENDPOINT, json=DATA)
o
print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
