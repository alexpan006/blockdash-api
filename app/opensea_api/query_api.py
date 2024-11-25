import requests
from dotenv import load_dotenv
import os
import json
from web3 import Web3
import datetime

#Query Transfer, Sale, and Offer event using Opensea Api
def query_nft_trx_data(collection_contract,identifier,after,till,cursor=None):
    """
    Using Opensea API to fetch transaction record given NFT identifier and collection contract
    Parameters:
    - collection_contract: str
        contract address of NFT collection.
    - identifier: str
        which NFT to be queried.
    - after: str
        specified blocktime to start.
    - till: str
        specified blocktime to stop.
    """

    status=""
    if(cursor==None):
        status=f"First time query on identifier = {identifier}"
    else:
        status=f"Querying on identifier = {identifier}, with cursor:{cursor}....."
        
    if cursor != None:
        url = f"https://api.opensea.io/api/v2/events/chain/ethereum/contract/{collection_contract}/nfts/{identifier}?event_type=sale&event_type=transfer&limit=50&after={after}&before={till}&next={cursor}"
    else:
        url = f"https://api.opensea.io/api/v2/events/chain/ethereum/contract/{collection_contract}/nfts/{identifier}?event_type=sale&event_type=transfer&limit=50&after={after}&before={till}"
    headers = {
        "accept": "application/json",
        "x-api-key": os.getenv("OPENSEA_API_KEY")
    }
    response = requests.get(url, headers=headers)
    status_co = response.status_code
    response = response.json()

    return response,status,status_co
def get_current_blocktime():
    """
    Get current blocktime of Ethereum using free cloudflare endpoint.
    """

    # Connect to a public Ethereum node provided by Cloudflare
    web3 = Web3(Web3.HTTPProvider('https://cloudflare-eth.com'))

    # Check if the connection is successful
    if not web3.is_connected():
        raise Exception("Failed to connect to Ethereum node")

    # Get the latest block
    latest_block = web3.eth.get_block('latest')

    # Extract the timestamp from the latest block
    block_timestamp = latest_block['timestamp']
    return block_timestamp
def convert_unix_to_blockNumber(unix):
    ETHERSCAN_API= os.getenv("ETHERSCAN_API")
    url = f"https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={unix}&closest=before&apikey={ETHERSCAN_API}"

    response = requests.get(url)

    # Parse the response JSON
    data = response.json()
    if(data['status']=="1"):
        # print(data['result'])
        return (data['result'])
    else:
        return False
    
def get_current_blocktime_v2():

    url = f"https://api.blockcypher.com/v1/eth/main"

    response = requests.get(url)

    # Parse the response JSON
    data = response.json()
    return (data['height'])



