import json
import requests
import datetime
import time
from stellar_sdk import (
    Asset,
    Keypair,
    Network,
    Server,
    TransactionBuilder
            ) 
import pandas as pd

config = {
    "testnet" : True,
    "memo_message" : "NUNA Staking Rewards",
    "company_name" : "NUNAproject",
    "contract_address" : "GCW7AGZBQK2CSJWMQRZ3HWIF3MV222DWTMOQNX3M3XBG5CID25HT6VG3",
    "horizon" : "https://horizon-testnet.stellar.org/",
    "secret_key" : "SDM6W6QM7YWN25HFD2ZLBHRL4JGBA2YF23Q7MGXUCAJJ6T574XJZ4RRH",
    "asset_disperse_daily" : 100,
    "asset_royalty_daily" : 24,
    "disallow" : [],
    "lockup_seconds" : 120,
    "asset_disperse" : {
        "code" : "FakeNUNA",
        "issuer" : "GARSR6GI5FBNDUPK7JTW2T3HAZYGEU4CNHK45564T2LKPWC5CFKPPYCP"
    },
    "asset_royalty" : {
        "code" : "FakeNUNA",
        "issuer" : "GARSR6GI5FBNDUPK7JTW2T3HAZYGEU4CNHK45564T2LKPWC5CFKPPYCP",
        "send_to" : "GDSBTSOOTZJOTYOL2MHDLTEIMS4LNV7267DXMGJNJQVNQMDH3U5QTTTN",
        "pay_from_secret" : "SDM6W6QM7YWN25HFD2ZLBHRL4JGBA2YF23Q7MGXUCAJJ6T574XJZ4RRH"
        },    
    }



def get_pool_index(balances,pool_id):
    i = 0
    while(i < len(balances)):
        if balances[i]['asset_type'] == "liquidity_pool_shares":
            if balances[i]['liquidity_pool_id'] == pool_id:
                return i
        i+=1
                
def get_stakers(appConfig):
    claimables_url = f"{config['horizon']}claimable_balances?&asset={config['asset_disperse']['code']}:{config['asset_disperse']['issuer']}&claimant={config['contract_address']}&limit=200"
    claimables = requests.get(claimables_url).json()
    claimables_list = []
    total = 0
    while len(claimables['_embedded']['records']) > 0:
        for i in claimables['_embedded']['records']:

            # Validating the transaction includes the following steps:
            # - Must be timelocked properly: 
            #   - The 'not abs_before' must be atleast 'lockup_seconds' after the ledger submission time.
            #   - 2 Claimants
            #   - One is the contract address: Unconditional
            #   - The other is the self, which must be timelocked.

            # 2022-11-09T20:39:38Z
            
            valid = False
            try:
                time_expire = i['claimants'][1]['predicate']['not']['abs_before']
                time_create = i['last_modified_time']
                a2 = (datetime.datetime.strptime(time_expire, "%Y-%m-%dT%H:%M:%SZ"))
                a1 = (datetime.datetime.strptime(time_create, "%Y-%m-%dT%H:%M:%SZ"))
                print(a2)
                print(a1)
                print((a2-a1).total_seconds())
                a = (a2-a1).total_seconds() >= config['lockup_seconds']
                print(a)
                print()
                valid = (True if (
                    len(i['claimants']) == 2 
                    and i['claimants'][0]['predicate']['unconditional']
                    and i['claimants'][0]['destination'] == config['contract_address']
                    and a
                )
                    else False
                )
            except:
                valid = False
            if valid:
                amt = (float(i['amount']))
                claimables_list.append({
                        "amount" : amt,
                        "id" : i['sponsor'],
                    })
                total += amt
        claimables_url = claimables['_links']['next']['href']
        claimables = requests.get(claimables_url).json()
    for i in claimables_list:
        i['reward'] = round((i['amount']/total) * (config['asset_disperse_daily']/24),7)
    if len(claimables_list) > 1:
        return {"claimables_list" : list(pd.DataFrame(claimables_list).groupby("id").sum().reset_index().T.to_dict().values())}
    elif len(claimables_list) == 1:
        return {"claimables_list" : claimables_list}

    elif len(claimables_list) == 0:
        return {"claimables_list" : []}
 
    

    
def script(appConfig):
    reward_list = get_stakers(appConfig)
    if len(reward_list['claimables_list']) > 0:
        print(reward_list)
        keypair = Keypair.from_secret(appConfig['secret_key'])
        server = Server(appConfig['horizon'])
        account = server.load_account(keypair.public_key)
        batch = 0
        hashes = []
        z = 0
        # for i in reward_list['providers']:
        #     z += i['reward']
        # return z
        fee_stat = requests.get(appConfig['horizon'] + "fee_stats").json()
        fee = int(fee_stat['max_fee']['p90'])
        tx = TransactionBuilder(
            source_account=account,
            network_passphrase=(Network.PUBLIC_NETWORK_PASSPHRASE if appConfig['testnet'] == False else Network.TESTNET_NETWORK_PASSPHRASE),
            base_fee=500000
            )
        tx.add_text_memo(appConfig['memo_message'])
        for i in reward_list['claimables_list']:
            if batch == 100:
            # if batch == 100:
                batch = 0
                completed = tx.set_timeout(100).build()
                completed.sign(keypair)
                sub = server.submit_transaction(completed)
                hashes.append(sub['hash'])
                print("submitted transaction")
                time.sleep(5)
                tx = TransactionBuilder(
                source_account=account,
                network_passphrase=(Network.PUBLIC_NETWORK_PASSPHRASE if appConfig['testnet'] == False else Network.TESTNET_NETWORK_PASSPHRASE),
                base_fee=500000
                )    
                tx.add_text_memo(appConfig['memo_message'])
            else:
                batch += 1
                tx.append_payment_op(
                    i['id'],
                    asset = Asset(
                        appConfig['asset_disperse']['code'],
                        appConfig['asset_disperse']['issuer'],
                        ),
                    amount=str(round(i['reward'],7))
                    )
        if batch!=0:
            completed = tx.set_timeout(100).build()
            completed.sign(keypair)
            sub = server.submit_transaction(completed)
            hashes.append(sub['hash'])
        else:
            print("done")
        if appConfig['asset_royalty_daily'] != 0:
            royalty_keypair = Keypair.from_secret(appConfig['asset_royalty']['pay_from_secret'])
            royalty_source = server.load_account(royalty_keypair.public_key)
            royalty_tx = TransactionBuilder(
                source_account = royalty_source,
                network_passphrase=(Network.PUBLIC_NETWORK_PASSPHRASE if appConfig['testnet'] == False else Network.TESTNET_NETWORK_PASSPHRASE),
                base_fee = 500000,
                ).append_payment_op(
                    appConfig['asset_royalty']['send_to'],
                    Asset(
                        appConfig['asset_royalty']['code'],
                        appConfig['asset_royalty']['issuer'],
                        ),
                    str(round(appConfig['asset_royalty_daily']/24,7))
                    ).add_text_memo("maintainence").set_timeout(100).build()
            royalty_tx.sign(royalty_keypair)
            response = server.submit_transaction(royalty_tx)
            return (response['hash'])

def main(e):
    script(config)