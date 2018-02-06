import time
import json
import subprocess
import os
import blocksci
import blocksci.cluster_python

# bitcoind --datadir=/home/ubuntu/.bitcoin/ --daemon
# scp -i ~/.ssh/merkle-nv.pem cluster.py ubuntu@ec2-54-82-214-182.compute-1.amazonaws.com:/home/ubuntu/BlockSci/Notebooks/
#  ./clusterer /home/ubuntu/bitcoin/ && mv *.dat /home/ubuntu/bitcoin/clusters/

def get_current_block():
    # get recent block tip from bitcoind
    username = 'user'
    password = 'password'
    block_info = subprocess.getoutput("bitcoin-cli -rpcuser='%s' -rpcpassword='%s' getblockchaininfo" % (username, password))
    block_info = json.loads(block_info)
    block_tip = block_info["blocks"]
    print("Most recent block is %s" %(block_tip))
    return block_tip

def run_blocksci_parser(block_tip):
    # update blockSci to recent - offset
    offset = 10
    result = os.system("blocksci_parser --output-directory /home/ubuntu/bitcoin update --max-block %i disk --coin-directory /home/ubuntu/.bitcoin" %(block_tip - offset))
    # error code for core dump
    if result == 34304:
        os.system("rm /home/ubuntu/bitcoin/parser/blockList.dat ")
        run_blocksci_parser(block_tip)

def run_clusterer():
    # run blockSci clustering & move cluster data into the correct directory
    os.system("clusterer /home/ubuntu/bitcoin")
    os.system("mv *.dat /home/ubuntu/bitcoin/clusters/")

def cluster_num(known_addr):
    chain = blocksci.Blockchain("/home/ubuntu/bitcoin/")
    cm = blocksci.cluster_python.ClusterManager("/home/ubuntu/bitcoin/clusters/")
    addr_object = blocksci.Address.from_string(known_addr)
    c = cm.cluster_with_address(addr_object)
    print(c.cluster_num)
    print(len(c.scripts))

def dump_clusters():
    # dump all cluster address
    chain = blocksci.Blockchain("/home/ubuntu/bitcoin/")
    cm = blocksci.cluster_python.ClusterManager("/home/ubuntu/bitcoin/clusters/")
    # 'gdax':["12h1fc3HpRi8HxwmLeAVEQ8bk5LZrj4uhT","3BCecHMNH8wFQ4KivDnMUo8EbZgjtkE1aM"]
    exchanges = {
        'bittrex':['1N52wHoVR79PMDishab2XmRHsbekCdGquK'],
        'poloniex':["17A16QmavnUfCW11DAApiJxp7ARnxN5pGX"],
        'kraken':["14eQD1QQb8QFVG8YFwGz7skyzsvBLWLwJS","3CyQZNawjoLXqfuS8ELBt7xNv7mJVSefZJ"],
        'bitfinex':["1DKxBfaSJX9YmuKgxsxqV36Ngh8pETaQjp", "19VNw8EQWKTmN7u1X15hqyWHrymrCCVWdK"],
        'bitstamp':["385Z1kPRKYP8XCekjyebc4cYL6speKtYv7"],
        'bithump': ["15c7QVBG4QFyBo1VLZAbiHZyWY4rUybxcM"],
        'binance': ["154CkCxtBM3e7fwdfQkXRmWQyissb5VeyK"],
        'bitmex': ["3BMEXjsXGepw8ypD3TeUJNfVzjzc1ip9Fi", "3BMEXTGRqBcwexGDu3Pq7JcaN4U5sBNYar"]
    }
    for name, known_addrs in exchanges.items():
        print("Start dumping addresses for %s" % (name))
        start_time = time.time()
        with open("/home/ubuntu/exchanges/%s.csv" % (name), 'w') as f:
            for known_addr in known_addrs:
                addr_object = blocksci.Address.from_string(known_addr)
                c = cm.cluster_with_address(addr_object)
                for s in c.scripts:
                    if s.type == blocksci.script_type.pubkey:
                        addr = s.script.address_string
                        f.write(addr+"\n")
                    elif s.type == blocksci.script_type.scripthash:
                        addr = s.script.address
                        f.write(addr+"\n")
                    elif s.type == blocksci.script_type.multisig:
                        addrs = s.script.addresses
                        for addr in addrs:
                            f.write(addr.script.address_string+"\n")
        end_time = time.time()
        print("Finished dumping addresses for %s | %i s" % (name, end_time - start_time))

def move_clusters():
    # move all cluster file to the parser server
    os.system("scp -i /home/ubuntu/.ssh/merkle-nv.pem /home/ubuntu/exchanges/* ubuntu@ec2-52-91-7-72.compute-1.amazonaws.com:/merkle-bitcoin-parser/data/exchanges")

last_block = 0
while True:
    print(time.ctime())
    current_block = get_current_block()
    if (current_block > last_block):
        run_blocksci_parser(current_block)
        run_clusterer()
        dump_clusters()
        move_clusters()
        last_block = current_block
    time.sleep(1)
