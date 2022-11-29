from flask import Flask, jsonify
from flask_cors import CORS
from flask_caching import Cache
import requests
import os

config = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 240
}

headers = {
    'x-api-key': os.environ.get("TRANSPOSEKEY"),
    'Content-Type': 'application/json',
}

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)
CORS(app)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,true')
    response.headers.add('Access-Control-Allow-Methods', 'GET,POST,PATCH,OPTIONS')
    return response

@app.route('/ethereum/<time>')
@cache.cached()
def get_ethereum(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data = {
        'sql': 'WITH current AS ( SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now()   AND t.timestamp > now() - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5     )  , preceding AS ( SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now()  - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5  )  SELECT c.contract, c.active_accounts, 100*(c.active_accounts - p.active_accounts)/p.active_accounts as accounts_percentage_growth, c.txns, 100*(c.txns - p.txns)/p.txns as txns_percentage_growth, c.gas_spend, 100*(c.gas_spend - p.gas_spend)/p.gas_spend as gas_spend_percentage_growth FROM current c INNER JOIN preceding p ON c.contract = p.contract WHERE p.active_accounts > 2 AND (c.active_accounts - p.active_accounts) > 0 ORDER BY 3 DESC',
        "options": {
        "timeout": 600
        }
        }
        response = requests.post('https://sql.transpose.io', headers=headers, json=json_data)
        return response.json()

@app.route('/polygon/<time>')
@cache.cached()
def get_polygon(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data = {
        'sql': 'WITH current AS ( SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now()   AND t.timestamp > now() - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5     )  , preceding AS ( SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now()  - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5  )  SELECT c.contract, c.active_accounts, 100*(c.active_accounts - p.active_accounts)/p.active_accounts as accounts_percentage_growth, c.txns, 100*(c.txns - p.txns)/p.txns as txns_percentage_growth, c.gas_spend, 100*(c.gas_spend - p.gas_spend)/p.gas_spend as gas_spend_percentage_growth FROM current c INNER JOIN preceding p ON c.contract = p.contract WHERE p.active_accounts > 2 AND (c.active_accounts - p.active_accounts) > 0 ORDER BY 3 DESC',
        }
        response = requests.post('https://sql.transpose.io', headers=headers, json=json_data)
        return response.json()

if __name__ == '__main__':
    app.run()
