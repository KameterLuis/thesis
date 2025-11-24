import requests
import json

with open('eth-validators.json', 'r') as file:
    validators_data = json.load(file)

print(len(validators_data["validators"]))

'''
validator_ids = ["0", "1", "29305"]

url = "http://10.105.50.169:5052/eth/v1/beacon/rewards/attestations/401577"
headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, data=json.dumps(validator_ids))

print(json.dumps(response.json(), indent=2))
'''