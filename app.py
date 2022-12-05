from flask import Flask, jsonify
from flask_cors import CORS
from flask_caching import Cache
from flask import make_response
import requests
import os
import pandas as pd
import json

config = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300
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
        json_data1 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true  AND t.timestamp > now() - \''+str(time)+' minutes\'::interval   AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 300
            }
        }
        json_data2 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns_previous,   COUNT(DISTINCT from_address) AS active_accounts_previous,   SUM(gas_used*gas_price/1e18) AS gas_spend_previous,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now() - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 300
            }
        }
        response1 = requests.post('https://sql.transpose.io', headers=headers, json=json_data1)
        response2 = requests.post('https://sql.transpose.io', headers=headers, json=json_data2)
        df1 = pd.DataFrame(response1.json()['results'])
        df2 = pd.DataFrame(response2.json()['results'])
        df2 = df2[df2['active_accounts_previous']>5]
        df1 = df1[df1['active_accounts']>5]
        df = pd.merge(df1,df2,how='inner',on='contract',validate='1:1')
        df['accounts_percentage_growth'] = df.apply(lambda x: 100*(x['active_accounts'] - x['active_accounts_previous'])/x['txns_previous'], axis =1)
        df['txns_percentage_growth'] = df.apply(lambda x: 100*(x['txns'] - x['txns_previous'])/x['txns_previous'], axis =1)
        df['gas_spend_percentage_growth'] = df.apply(lambda x: 100*(x['gas_spend'] - x['gas_spend_previous'])/x['txns_previous'], axis =1)
        df = df[df['accounts_percentage_growth']>0]
        df = df.sort_values(by=['accounts_percentage_growth'], ascending=False)
        return df.to_json(orient='records')

@app.route('/polygon/<time>')
@cache.cached()
def get_polygon(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data1 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true  AND t.timestamp > now() - \''+str(time)+' minutes\'::interval   AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 300
            }
        }
        json_data2 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns_previous,   COUNT(DISTINCT from_address) AS active_accounts_previous,   SUM(gas_used*gas_price/1e18) AS gas_spend_previous,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now() - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 300
            }
        }
        response1 = requests.post('https://sql.transpose.io', headers=headers, json=json_data1)
        response2 = requests.post('https://sql.transpose.io', headers=headers, json=json_data2)
        df1 = pd.DataFrame(response1.json()['results'])
        df2 = pd.DataFrame(response2.json()['results'])
        df2 = df2[df2['active_accounts_previous']>5]
        df1 = df1[df1['active_accounts']>5]
        df = pd.merge(df1,df2,how='inner',on='contract',validate='1:1')
        df['accounts_percentage_growth'] = df.apply(lambda x: 100*(x['active_accounts'] - x['active_accounts_previous'])/x['txns_previous'], axis =1)
        df['txns_percentage_growth'] = df.apply(lambda x: 100*(x['txns'] - x['txns_previous'])/x['txns_previous'], axis =1)
        df['gas_spend_percentage_growth'] = df.apply(lambda x: 100*(x['gas_spend'] - x['gas_spend_previous'])/x['txns_previous'], axis =1)
        df = df[df['accounts_percentage_growth']>0]
        df = df.sort_values(by=['accounts_percentage_growth'], ascending=False)
        return df.to_json(orient='records')

if __name__ == '__main__':
    app.run()
