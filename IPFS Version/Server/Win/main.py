# -*- coding: utf8 -*-
import getopt
import json
import os.path
import socket
import sys
import time

import pymysql
from _thread import *
import threading
from basefunctions import *

'''
    Here some parameters that used in global
    ----------------------------------------
    | cs: Mysql cursor
    | db: MySql handle
    | user: DataBase username
    | DBname: DataBase name
    | pwd: The password for DB
    ----------------------------------------
'''
db = None
cs = None
user = None
DBname = None
pwd = None
mistake_hash = "0000000000000000000000000000000000000000000000000000000000000000"
rank_cache = {}
mutex = False
selfip = "192.168.10.128"
is_closed = False

'''
    This function is used for connecting the
    DataBase. And the connection parameters
    are got from the console.
'''


def setupDB(data=None):
    global user, DBname, pwd
    if user is None or DBname is None:
        user = data['user']
        DBname = data['database']
    try:
        if pwd is None:
            pwd = data['password']
    except:
        pwd = input("Please input the password for the DB:")

    try:
        global db
        db = pymysql.connect(host='localhost', user=user, password=pwd, database=DBname)
    except:
        print("MySql Connection Error!")
        sys.exit()

    global cs
    cs = db.cursor()


'''
    This function is used for init the listening work
    which will set up TCP work.
'''


def setupNet(data):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error as err:
        print("Setup Failed With Error %s" % err)
        sys.exit()

    try:
        # server.bind((data['ip'], int(data['port'])))
        global selfip
        selfip = data["ip"]
        port = int(data["port"])
        server.bind((selfip, port))
    except socket.error as err:
        print("Setup Failed With Error %s" % err)
        sys.exit()

    server.listen(10)

    return server


def getsingle(host, op_id, size):
    try:
        data = {"code": "E000", "size": size}
        s = send_data(host)
        send_time = time.time()
        s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
        buf = s.recv(1024)
        s.close()
        buf = json.loads(buf.decode('utf-8'))
        recv_time = time.time()
        if buf["code"] != "E001":
            print("RECV ERROR CODE FROM %s" % host)
            return
        if buf["acceptance"] == 0:
            print("Node %s refused this election" % host)
            value = -1
        else:
            t = recv_time - send_time
            value = int(buf["space"]) / (1024 * 1024 * 1024)
            if t < 1:
                t = 1
            value /= t
    except Exception as e:
        print(f"ASK FAILED AT {host}")
        value = -1
    finally:
        global rank_cache, mutex

        while True:
            if not mutex:
                mutex = True
                #rank_cache[op_id][host] = value
                rank_cache[op_id].append([host, value])
                mutex = False
                break


def begin_election(op_id, size):
    with open("./nodes.dat", "r") as f:
        nodes = f.read().split("\n")

    global rank_cache, mutex, selfip

    while True:
        if not mutex:
            mutex = True
            rank_cache[op_id] = []
            mutex = False
            break

    cnt_n = 0
    for n in nodes:
        if n != "" and n != selfip:
            getsingle(n, op_id, size)
            # td = threading.Thread(target=getsingle, kwargs={"host": n, "op_id": op_id, "size": size})
            # td.start()
            cnt_n += 1

    while True:
        if not mutex:
            mutex = True
            if len(rank_cache[op_id]) == cnt_n:
                mutex = False
                break
            mutex = False


def C000(client, data):
    res = {}
    op_id = data["data"]["op_id"]
    data = data["data"]
    needs = int(data["this_size"])
    newsize = data["newbk_size"]
    res["code"] = "S000"
    res["data"] = {}

    if needs < get_free_space_mb("./"):
        res["data"]["status"] = 1
        client.send(json.dumps(res, ensure_ascii=False).encode('utf-8'))
        time.sleep(1)
        # bordcast for the next election
        begin_election(op_id, newsize)
        # pass
        res.clear()
        res["code"] = "S001"
        data = {}
        # There might need send the rank size in advance
        global rank_cache, mutex
        while True:
            if not mutex:
                mutex = True
                rank = rank_cache[op_id]
                mutex = False
                break

        data["rank_size"] = len(json.dumps(rank, ensure_ascii=False).encode('utf-8'))
        print(data["rank_size"])
        data["op_id"] = op_id
        res["data"] = data
        client.send(json.dumps(res, ensure_ascii=False).encode('utf-8'))
        res.clear()

        res["code"] = "S002"
        # need sort to rank
        rank = sort_rank(rank)
        print("rank_soreted:", rank)
        res["rank"] = rank
        res["rec"] = rank[0][0]
        client.send(json.dumps(res, ensure_ascii=False).encode('utf-8'))
        data = client.recv(1024)
        client.close()
        del rank_cache[op_id]
    else:
        res["data"]["status"] = 0
        client.send(json.dumps(res, ensure_ascii=False).encode('utf-8'))
        client.close()


def E000(client, data):
    inf = {}

    free = get_free_space_mb("\\")
    inf["code"] = "E001"

    if free > data["size"]:
        inf["acceptance"] = 1
        inf["space"] = free
    else:
        inf["acceptance"] = 0

    client.send(json.dumps(inf, ensure_ascii=False).encode('utf-8'))
    client.close()


def F000(client, data):
    global mistake_hash
    size = int(data["bk_size"])
    bk_hash = data["bk_id"]
    temp = data["temp"]
    base = int(data["base"])
    cache_path = "./cache/bk_cache%s.dat" % temp
    cache_path = cache_path.replace("\\", "/")

    if sys.platform.startswith('win'):
        ready = {"status": 0}
    else:
        ready = {"status": 1, "base": 1460}
        base = 1460

    client.send(json.dumps(ready, ensure_ascii=False).encode('utf-8'))



    print("beging to block %s at base %d" % (bk_hash, base))
    '''bk_cache = open(cache_path, "wb")
    cnt = 0
    while (cnt + base) < size:
        buf = client.recv(base)
        bk_cache.write(buf)
        cnt += base
        time.sleep(0.005)

    if (size - cnt) > 0:
        buf = client.recv(size - cnt)
        bk_cache.write(buf)
        time.sleep(0.005)

    bk_cache.close()
    client.close()'''
    
    print("write", cache_path)
    bufcnt = 0
    #try:
    with open(cache_path, "wb") as bk_cache:
        '''
        cnt = 0
        while (cnt + base) < size:
            buf = client.recv(base)
            bufcnt += sys.getsizeof(buf)
            bk_cache.write(buf)
            bk_cache.flush()
            client.send(bytes([1]))
            cnt += base

        if (size - cnt) > 0:
            buf = client.recv(size - cnt)
            bufcnt += sys.getsizeof(buf)
            bk_cache.write(buf)
            bk_cache.flush()
            client.send(bytes([1]))
        '''

        buf = client.recv(base)
        while buf:
            bk_cache.write(buf)
            buf = client.recv(base)

        print("size: %d, buf-size: %d, current: %d" % (size, bufcnt, bk_cache.tell() ) )

        bk_cache.close()

    client.send(json.dumps({"status": 1}, ensure_ascii=False).encode('utf-8'))
    
    #except Exception as e:
        #print(f"Error : {e}")
        #client.send(json.dumps({"status": 0}, ensure_ascii=False).encode('utf-8'))

    client.close()

    print("Write over cache %s" % cache_path, "\ndata: ", data )



def F002(client, data):
    with open("./nodes.dat", "r") as f:
        nodes = f.read().split("\n")

    ask = data["block_hash"]
    data = {"code": "F003", "ask": ask}

    for i in nodes:
        try:
            if i == "":
                continue
            s = send_data(i)
            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            ans = s.recv(1024)
            s.close()
            ans = json.loads(ans.decode('utf-8'))

            if ans["code"] != "F004":
                print("The node given an error code in asking the block.")
                continue

            if ans["statu"] == 1:
                client.send(json.dumps({"code": "F005", "is_get": 1, "loc": i}, ensure_ascii=False).encode('utf-8'))
                client.close()
                return
        except:
            print(f"ASK FAILED At NODE {i}")
            continue

    client.send(json.dumps({"code": "F005", "is_get": 0}, ensure_ascii=False).encode('utf-8'))
    client.close()


def F003(client, data):
    res = {}
    ask = data["ask"]
    ask = "cache\\bk_cache%s.dat" % ask
    print(ask, os.path.exists(ask))
    if os.path.exists(ask):
        res["statu"] = 1
    else:
        res["statu"] = 0

    res["code"] = "F004"

    client.send(json.dumps(res, ensure_ascii=False).encode('utf-8'))
    client.close()


def F006(client, data):
    block = "./cache/bk_cache%s.dat" % data["block"]
    data.clear()
    block_size = get_FileSize(block)
    base = block_size

    data["code"] = "F007"
    data["base"] = base
    data["block_size"] = block_size

    global mistake_hash

    fhash = sm3().sm3_file(block)
    cnt = 0
    while fhash == mistake_hash and cnt < 20:
        cnt += 1
        fhash = sm3().sm3_file(block)

    data["fhash"] = fhash

    client.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))


    f = open(block, "rb")
    client.sendfile(f)

    f.close()
    client.close()


def FC07(client, data):
    print("FC07", client, data)
    cache_path = "cache/bk_cache%s.dat" % data["block"]
    cache_path = cache_path.replace("\\", "/")
    print("FC07-2", cache_path)
    if os.path.exists(cache_path):
        print("FC07 in if")
        global mistake_hash
        value = sm3().sm3_file(cache_path)
        print("we")
        cnt = 0
        while value == mistake_hash and cnt < 20:
            cnt += 1
            print("\ncalculating sm3 %s\n" % value)
            value = sm3().sm3_file(cache_path)
        print("EW")
        if value == data["bk_id"]:
            client.send(json.dumps({"code": 1}, ensure_ascii=False).encode('utf-8'))
        else:
            print("\n\n\ncal: %s\nacs: %s\n\n\n\n\n" % (value, data["bk_id"]))
            client.send(json.dumps({"code": 0, "value": value}, ensure_ascii=False).encode('utf-8'))
    else:
        print("FC07 in else")
        client.send(json.dumps({"code": 0, "value": "not_exit"}, ensure_ascii=False).encode('utf-8'))
    print("FC07 closing")
    client.close()
    print("FC07 Over")


def EX00(client, data):
    try:
        global is_closed

        is_closed = True

        client.send(json.dumps({"status": 0}, ensure_ascii=False).encode("utf-8"))

    except:
        client.send(json.dumps({"status": 1}, ensure_ascii=False).encode("utf-8"))

    client.close()

def EX01(client, data):
    try:
        global is_closed

        is_closed = False

        client.send(json.dumps({"status": 0}, ensure_ascii=False).encode("utf-8"))

    except:
        client.send(json.dumps({"status": 1}, ensure_ascii=False).encode("utf-8"))

    client.close()



def process(client):
    try:
        data = client.recv(1024)
        data = json.loads(data.decode('utf-8'))
    except UnicodeDecodeError as e:
        print(f"\n\nError:{e}")
        print("\nED: ", data)
        print("\n\n\n\n\n\n")
    code = data["code"]
    print(data)

    #try:
    global is_closed

    if code not in ("EX00", "EX01") and is_closed == True:
        client.close()
        return


    if code == 'C000':
        C000(client, data)
    elif code == "F000":
        F000(client, data)
    elif code == "F002":
        F002(client, data)
    elif code == "F003":
        F003(client, data)
    elif code == "F006":
        F006(client, data)
    elif code == "FC07":
        FC07(client, data)
    elif code == "E000":
        E000(client, data)
    elif code == "EX00":
        EX00(client, data)
    elif code == "EX01":
        EX01(client, data)
    else:
        client.close()
    #except:
    #    print(client, data, )


'''
    Initializing the Server.
'''


def setup(data):
    # setupDB(data)
    server = setupNet(data)

    print("Server Started Successfully!\nServer is waiting for connections... ...")
    while True:
        client, addr = server.accept()
        start_new_thread(process, (client,))

    server.close()


'''
    Analyzing the commands
'''


def analyze(*argc, **argv):
    opts, args = getopt.getopt(sys.argv[1:], "hi:p:d:w:u:n:")
    data = {}
    if len(args) != 0:
        print("The command have mistakes, Please check the command!")
        sys.exit()
    for i in opts:
        if i[0] == '-h':
            print("This is help for you:\n")
            print("\t-h:\n\t\tThis parameter is used for get help just like you just done.\n")
            print("\t-i:\n\t\tThe ip your want to  bind.\n")
            print("\t-p:\n\t\tThe port that you want to listen to.\n")
            print("\t-d:\n\t\tThe database name.\n")
            print("\t-w:\n\t\tThis parameter isn't necessarily required.")
            print("\t\tThe password for connecting the data database.\n")
            print("\t-u:\n\t\tThis parameter is used for DB user.")
            sys.exit()

        elif i[0] == '-i':
            if i[1] != "":
                data['ip'] = i[1]
            else:
                print("You don't have input the ip address.")
                sys.exit()
        elif i[0] == '-p':
            if i[1] != "":
                data['port'] = i[1]
            else:
                print("You don't have input the port.")
                sys.exit()
        elif i[0] == '-d':
            if i[1] != "":
                data['database'] = i[1]
            else:
                print("You don't have input the DB name.")
                sys.exit()
        elif i[0] == '-w':
            if i[1] != "":
                data['password'] = i[1]
            else:
                print("You don't have input the password.")
                sys.exit()
        elif i[0] == '-u':
            if i[1] != "":
                data['user'] = i[1]
            else:
                print("You don't have input the DB username.")
                sys.exit()

    if len(data) < 4:
        print("The parameters is not enough!")
        sys.exit()

    return data


if __name__ == "__main__":
    setup(setup(analyze()))
