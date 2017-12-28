import time
import json
import subprocess
import os
import blocksci
import blocksci.cluster_python


while true:
    run_blocksci_parser()
    run_clusterer()
    dump_clusters()
    move_clusters()

def run_blocksci_parser():
    # get recent block tip from bitcoind
    config_path = '/home/ubuntu/.bitcoin/.cookie'
    f = open(config_path, 'r')
    username = '__cookie__'
    password = f.readline().split(":")[1]
    block_info = subprocess.getoutput("bitcoin-cli -rpcuser='%s' -rpcpassword='%s' getblockchaininfo" % (username, password))
    block_info = json.loads(block_info)
    block_tip = block_info["blocks"]
    print("Most recent block is %s" %(block_tip))
    # update blockSci to recent - 10
    os.system("blocksci_parser --output-directory /home/ubuntu/bitcoin update disk --coin-directory /home/ubuntu/.bitcoin --max-block %i" %(block_tip - 10))

def run_clusterer():
    # run blockSci clustering & move cluster data into the correct directory
    os.system("clusterer /home/ubuntu/bitcoin")
    os.system("mv *.dat /home/ubuntu/bitcoin/clusters/")

def dump_clusters():
    # dump all cluster address
    chain = blocksci.Blockchain("/home/ubuntu/bitcoin/")
    cm = blocksci.cluster_python.ClusterManager("/home/ubuntu/bitcoin/clusters/")
    exchanges = {
        'bittrex':['1N52wHoVR79PMDishab2XmRHsbekCdGquK'],
        'poloniex':["17A16QmavnUfCW11DAApiJxp7ARnxN5pGX"],
        'kraken':["14eQD1QQb8QFVG8YFwGz7skyzsvBLWLwJS"],
        'bitfinex':["1DKxBfaSJX9YmuKgxsxqV36Ngh8pETaQjp", "19VNw8EQWKTmN7u1X15hqyWHrymrCCVWdK"]
    }
    for name, known_addrs in exchanges.items():
        print("Start dumping addresses for %s" % (name))
        start_time = time.time()
        with open("/home/ubuntu/exchanges/%s.csv" % (name), 'w') as f:
            for known_addr in known_addrs:
                addr_object = blocksci.Address.from_string(known_addr)
                c = cm.cluster_with_address(addr_object)
                for s in c.scripts:
                    if s.type == blocksci.address_type.pubkey:
                        addr = s.script.address_string
                        f.write(addr+"\n")
                    elif s.type == blocksci.address_type.scripthash:
                        addr = s.script.address
                        f.write(addr+"\n")
                    elif s.type == blocksci.address_type.multisig:
                        addrs = s.script.addresses
                        for addr in addrs:
                            f.write(addr+"\n")
        end_time = time.time()
        print("Finished dumping addresses for %s | %i s" % (name, end_time - start_time))

def move_clusters():
    # move all cluster file to the parser server
    os.system("scp -i /home/ubuntu/.ssh/merkle-nv.pem /home/ubuntu/exchanges/* ubuntu@ec2-34-239-139-135.compute-1.amazonaws.com:/historical/merkle-bitcoin-parser/data/exchanges")
