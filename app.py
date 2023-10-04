from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from flask import make_response
import requests
import os
import pandas as pd
import json
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# config = {
#     "CACHE_TYPE": "filesystem",
#     "CACHE_DIR": '/tmp',
#     "CACHE_DEFAULT_TIMEOUT": 600
# }

REDIS_LINK = os.environ['REDIS_URL']

config = {
    "CACHE_TYPE": "redis",
    "CACHE_DEFAULT_TIMEOUT": 82800,
    "CACHE_REDIS_URL": REDIS_LINK
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

def make_cache_key(*args, **kwargs):
    path = request.path
    args = str(hash(frozenset(request.args.items())))
    return (path + args).encode('utf-8')

@app.route('/ethereum/<time>')
@cache.memoize(make_name=make_cache_key)
def get_ethereum(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data1 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true  AND t.timestamp > now() - \''+str(time)+' minutes\'::interval   AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        json_data2 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns_previous,   COUNT(DISTINCT from_address) AS active_accounts_previous,   SUM(gas_used*gas_price/1e18) AS gas_spend_previous,   a.created_timestamp AS created_at FROM  ethereum.transactions t   LEFT JOIN  ethereum.accounts a ON t.to_address = a.address   LEFT JOIN ethereum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN ethereum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now() - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        response1 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data1)
        response2 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data2)
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
@cache.memoize(make_name=make_cache_key)
def get_polygon(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data1 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true  AND t.timestamp > now() - \''+str(time)+' minutes\'::interval   AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        json_data2 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns_previous,   COUNT(DISTINCT from_address) AS active_accounts_previous,   SUM(gas_used*gas_price/1e18) AS gas_spend_previous,   a.created_timestamp AS created_at FROM  polygon.transactions t   LEFT JOIN  polygon.accounts a ON t.to_address = a.address   LEFT JOIN polygon.tokens tok ON t.to_address = tok.contract_address LEFT JOIN polygon.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now() - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        response1 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data1)
        response2 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data2)
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

@app.route('/arbitrum/<time>')
@cache.memoize(make_name=make_cache_key)
def get_arbitrum(time):
    if int(time) >= 120:
        return 'Use a shorter timeslot'
    else:
        json_data1 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns,   COUNT(DISTINCT from_address) AS active_accounts,   SUM(gas_used*gas_price/1e18) AS gas_spend,   a.created_timestamp AS created_at FROM  arbitrum.transactions t   LEFT JOIN  arbitrum.accounts a ON t.to_address = a.address   LEFT JOIN arbitrum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN arbitrum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true  AND t.timestamp > now() - \''+str(time)+' minutes\'::interval   AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        json_data2 = {
            'sql': ' SELECT  to_address AS contract,   COUNT(DISTINCT transaction_hash) AS txns_previous,   COUNT(DISTINCT from_address) AS active_accounts_previous,   SUM(gas_used*gas_price/1e18) AS gas_spend_previous,   a.created_timestamp AS created_at FROM  arbitrum.transactions t   LEFT JOIN  arbitrum.accounts a ON t.to_address = a.address   LEFT JOIN arbitrum.tokens tok ON t.to_address = tok.contract_address LEFT JOIN arbitrum.collections col ON t.to_address = col.contract_address WHERE __confirmed = true   AND t.timestamp < now() - \''+str(time)+' minutes\'::interval AND t.timestamp > now() - \''+str(time)+' minutes\'::interval  - \''+str(time)+' minutes\'::interval AND a.type = \'contract\'   AND tok.contract_address IS NULL AND col.contract_address IS NULL GROUP BY 1,5    ',
            "options": {
                "timeout": 200
            }
        }
        response1 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data1)
        response2 = requests.post('https://api.transpose.io/sql', headers=headers, json=json_data2)
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
    
@app.route('/ethereum_tc/<time>')
@cache.memoize(make_name=make_cache_key)
def get_ethereum_tc(time):
    if time == 'day':
        param = "'1' day"
    elif time == 'week':
        param = "'7' day"
    elif time == 'month':
        param = "'1' month"
    query = QueryBase(
        name="tc_new_eth",
        query_id=3027612,
        params=[
        QueryParameter.text_type(name="time", value=param),
        ],
    )

    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh_into_dataframe(query)
    return results.to_json(orient='records')

@app.route('/arbitrum_tc/<time>')
@cache.memoize(make_name=make_cache_key)
def get_arbitrum_tc(time):
    if time == 'day':
        param = "'1' day"
    elif time == 'week':
        param = "'7' day"
    elif time == 'month':
        param = "'1' month"    
    query = QueryBase(
        name="tc_new_arb",
        query_id=3029500,
        params=[
        QueryParameter.text_type(name="time", value=param),
        ],
    )

    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh_into_dataframe(query)
    return results.to_json(orient='records')

@app.route('/optimism_tc/<time>')
@cache.memoize(make_name=make_cache_key)
def get_optimism_tc(time):
    if time == 'day':
        param = "'1' day"
    elif time == 'week':
        param = "'7' day"
    elif time == 'month':
        param = "'1' month"     
    query = QueryBase(
        name="tc_new_op",
        query_id=3029431,
        params=[
        QueryParameter.text_type(name="time", value=param),
        ],
    )

    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh_into_dataframe(query)
    return results.to_json(orient='records')

@app.route('/base_tc/<time>')
@cache.memoize(make_name=make_cache_key)
def get_base_tc(time):
    if time == 'day':
        param = "'1' day"
    elif time == 'week':
        param = "'7' day"
    elif time == 'month':
        param = "'1' month"  
    query = QueryBase(
        name="tc_new_base",
        query_id=3029480,
        params=[
        QueryParameter.text_type(name="time", value=param),
        ],
    )

    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh_into_dataframe(query)
    return results.to_json(orient='records')

@app.route('/polygon_tc/<time>')
@cache.memoize(make_name=make_cache_key)
def get_polygon_tc(time):
    if time == 'day':
        param = "'1' day"
    elif time == 'week':
        param = "'7' day"
    elif time == 'month':
        param = "'1' month"  
    query = QueryBase(
        name="tc_new_poly",
        query_id=3029509,
        params=[
        QueryParameter.text_type(name="time", value=param),
        ],
    )

    dune = DuneClient(os.environ["DUNE_API_KEY"])
    results = dune.refresh_into_dataframe(query)
    return results.to_json(orient='records')

if __name__ == '__main__':
    app.run()
