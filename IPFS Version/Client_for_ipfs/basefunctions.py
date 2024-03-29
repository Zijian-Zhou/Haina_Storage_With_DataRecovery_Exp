import ctypes
import json
import os
import random
import socket
import platform
import threading
import time
from ctypes import *
import win32file

mistake_hash = "0000000000000000000000000000000000000000000000000000000000000000"
tds = []


def is_used(file_name):
    try:
        vHandle = win32file.CreateFile(file_name, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                       win32file.FILE_ATTRIBUTE_NORMAL, None)
        return int(vHandle) == win32file.INVALID_HANDLE_VALUE
    except:
        return True


def send_data(host, port=5656):
    try:
        s = socket.socket()
        s.connect((host, port))
        return s
    except:
        print("%s网络错误!" % host)
        return None


def get_free_space_mb(folder):
    """
    Get the free space of path
    :param folder: the path
    :return: free size KB
    """
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value
    else:
        st = os.statvfs(folder)
        return st.f_bavail * st.f_frsize


def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    return fsize


def sort_rank(rank):
    for i in range(len(rank)):
        for j in range(i, len(rank)):
            if rank[j][1] > rank[i][1]:
                rank[i], rank[j] = rank[j], rank[i]
    return rank


def ready2rec(test, st, num):
    meta = open("metafile.dat", "r")
    inf = json.loads(meta.read())

    source = inf["source"]
    head = inf["first"]

    meta.close()

    """
    meta.seek(32)
    source = meta.read(32).hex().upper()
    head = meta.read(32).hex().upper()
    """

    re = recovery("downloads", head, num)
    re.start()
    election(test, st).clear_blocks(flag=True)


def error_checker(buf):
    if buf is None or buf == "":
        return True
    return False


class election:

    def __init__(self, test, flag=None):
        # nodes records all the nodes, op_id records this time's event id
        if flag is not None:
            self.stime = flag
        else:
            self.stime = time.time()
        self.nodes = {}
        self.op_id = None
        self.block_num = len(os.listdir("cache")) - 1
        self.uploaded = []
        self.test = test
        self.done = 0
        self.ps = int((self.block_num - 1) / 2)
        self.prio_loc = {}
        self.firsterror = {}

    # This function is used for checking the latest nodes file.
    def check_nodes(self):
        global mistake_hash
        res = sm3().sm3_file("nodes.dat")
        while res == mistake_hash:
            res = sm3().sm3_file("nodes.dat")
        # this value should get from a random node next!
        if "25B054C794EEEAB45188ED8C688E67A1198F84B4B31935F59E492ECE1FAD7D28" != res:
            return False
        else:
            return True

    # This function is used for selecting the header node
    def sele_headn(self):

        with open("nodes.dat", "r") as f:
            temp = f.read()
        temp = temp.split("\n")
        for i in temp:
            if i != "":
                self.nodes[i] = 0
        head = random.randint(0, len(self.nodes) - 1)
        head = list(self.nodes.keys())[head]

        return head

    """
    send2beginner is used for send the init data to the beginner node.
    :param node: beginner node ip
    :return: the new storage node
    """

    def send2beginner(self, node, blockseq, origin):
        print(node, blockseq, origin)
        """
        code "C000": inform the node to be the header node & get ready to the next election.
        data
            "this_size": the first data block's size
            "source": the original file's SM3 hash value
            "op_id": this time's event id, calculated by SM3, the parameters are source and the timestamp
            "newbk_size": the next data block's size
        """
        global mistake_hash
        data = {"code": "C000", "data": {"this_size": 0, "source": 0, "op_id": 0, "newbk_size": 0}}

        firsiz = get_FileSize("cache\\\\cache_%d" % blockseq)
        if blockseq + 1 < self.block_num:
            newbk_size = get_FileSize("cache\\\\cache_%d" % (blockseq + 1))
        else:
            newbk_size = 0
        source = sm3().sm3_file(origin)
        while source == mistake_hash:
            source = sm3().sm3_file(origin)
        self.op_id = sm3().cal_sm3(source + str(time.time()))
        while self.op_id == mistake_hash:
            self.op_id = sm3().cal_sm3(source + str(time.time()))

        data["data"]["this_size"] = firsiz
        data["data"]["source"] = source
        data["data"]["op_id"] = self.op_id
        data["data"]["newbk_size"] = newbk_size
        s = send_data(node)
        s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))

        if s is not None:
            buf = s.recv(1024).decode('utf-8')
            if error_checker(buf):
                exit(1)
            data.clear()
            data = json.loads(buf)
            if data["code"] != "S000":
                s.close()
                print("Code Error at S000!")
                return False
            elif data["data"]["status"] == 0:
                print("Header Node Error, the header refused the request!")
                s.close()
                return False
            elif data["data"]["status"] == 1:
                """
                code "C001": giving the acceptance result
                data 
                    "status": 0 / 1, 0: Refuse, 1: Accept
                    "op_id": consistent with the op_id of 'C000'
                    if status is 0, then:
                        "next": the reselected node
                """
                try:
                    data = s.recv(1024).decode('utf-8')
                    if error_checker(data):
                        exit(1)
                    data = json.loads(data)
                except Exception as e:
                    print(f"{e}")
                    print(data)
                    exit(1)
                if data["code"] != "S001":
                    print("Recv Error rank code!")
                    s.close()
                    return False
                elif data["data"]["op_id"] != self.op_id:
                    print("Recv Error op_id at the rank_list")
                    s.close()
                    return False
                rank = s.recv(100 + int(data["data"]["rank_size"]))
                if error_checker(rank):
                    exit(1)
                rank = json.loads(rank.decode('utf-8'))
                if rank["code"] != "S002":
                    print("Recv Error rank-list code!")
                    s.close()
                    return False

                # next_node = rank["rec"]
                rank = rank["rank"]

                data["code"] = "C001"
                data["data"] = {}
                data["data"]["op_id"] = self.op_id

                '''
                if self.check_node_rate(next_node):
                    # over rate
                    data["data"]["status"] = 0
                    next_node = self.get_new_node(rank)
                    data["data"]["next"] = next_node
                else:
                    data["data"]["status"] = 1
                '''

                self.nodes[node] += 1
                data["data"]["status"] = 0
                next_node = self.get_new_node(rank, node)
                data["data"]["next"] = next_node


                s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
                s.close()


                path = "cache\\\\cache_%d" % blockseq
                temp = sm3().get_block_hash(path)
                while temp == mistake_hash:
                    time.sleep(0.3)
                    temp = sm3().get_block_hash(path)

                if blockseq == 1:
                    self.firsterror["sec"] = [temp, node]
                elif blockseq == self.block_num - 1:
                    self.firsterror["end"] = [temp, node]

                td = threading.Thread(target=self.sendblock, args=(node, blockseq, self.op_id))
                td.start()
                tp = threading.Thread(target=self.transpri, args=(node, blockseq, self.op_id))
                tp.start()
                while not td.is_alive():
                    td.start()
                while not tp.is_alive():
                    tp.start()
                global tds
                tds.append(td)
                self.send2other(next_node, blockseq + 1, origin)
                return True

    def clear_blocks(self, flag=None):
        if flag is None:
            files = os.listdir("cache")
            for i in files:
                try:
                    os.remove(os.path.join("cache", i))
                except Exception as e:
                    pass
            while self.done != self.block_num:
                continue
        else:
            files = os.listdir("downloads")
            for i in files:
                os.remove(os.path.join("downloads", i))
        etime = time.time()
        if flag is None:
            data = open("test_file_range.csv", "a+")
        else:
            data = open("download_out.csv", "a+")
        data.write("%s, %s\n" % (str(etime), str(etime - self.stime)))
        data.close()
        self.test.mutex = False

    """
        other block
    """

    def send2other(self, node, blockid, origin):

        print("processing block %d, self.block_num = %d" % (blockid, self.block_num))

        if blockid >= self.block_num:
            return
        if node is None:
            print("The node is None in ready to send")
            return
        self.send2beginner(node=node, blockseq=blockid, origin=origin)

    """
        If the node' selection is not good choice, then the get_new_node function
        will select the new storage node based on the rank-list's order & the node's rate.
        :param: rank: the nodes' rank list
        :return the new node
        if there aren't a new node satisfy the condition return None.
    """

    def get_new_node(self, rank, node, flag=True):
        if flag:
            for i in range(len(rank)):
                if rank[i][0] != node and self.nodes[rank[i][0]] == 0:
                    return rank[i][0]
            for i in range(len(rank)):
                if rank[i][0] != node and not self.check_node_rate(rank[i][0]):
                    return rank[i][0]
            return self.get_new_node(rank, node, False)
        for i in range(len(rank)):
            if rank[i][0] != node and not self.check_node_rate(rank[i][0]):
                return rank[i][0]

    """
    This function is used for checking the node's rate.
    As a default the rate is 20%.
    :param: node: the node that ready to calculate
    :param: stander: the rate stander with default value 20%.
    :return: A bool value, True: overflow, False: not overflow
    """

    def check_node_rate(self, node, stander=0.6):
        cnt = self.nodes[node]
        if cnt == 0:
            return False
        rate = (cnt + 1) / self.block_num
        if rate > stander:
            return True
        else:
            return False

    """
    sendblock is used for send the a block's data to a node after the node confirmed.
    :param node: the confirmed node
    :param blockid: identification of the block
    :param session: the event_id ( generated at the operation C000)
    """

    def sendblock(self, node, blockid, session=None, errortime=0):
        print("send block %d to %s" % (blockid, node))
        global mistake_hash
        be = time.time()
        req = {}

        path = "cache\\\\cache_%d" % blockid
        size = get_FileSize(path)

        bkh = sm3().sm3_file(path)
        # print("b%s-w1" % blockid)
        while bkh == mistake_hash:
            time.sleep(0.3)
            bkh = sm3().sm3_file(path)
        temp = sm3().get_block_hash(path)
        while temp == mistake_hash:
            time.sleep(0.3)
            temp = sm3().get_block_hash(path)

        # print(blockid, self.firsterror)

        # ready upload
        s = send_data(node)

        # print("b%s-w3" % blockid)
        while s is None:
            s = send_data(node)
        req["code"] = "F000"
        # base = 1500
        if session is not None:
            req["op_id"] = session
        else:
            print("Uoload block" + blockid + " failed, due to without session.")
            s.close()
            return
        req["bk_id"] = bkh
        req["temp"] = temp
        req["bk_size"] = size

        # """
        req["base"] = size
        base = size
        # """

        # req["base"] = 1024
        # base = 1024

        s.send(json.dumps(req, ensure_ascii=False).encode('utf-8'))

        data = s.recv(1024)
        data = json.loads(data.decode('utf-8'))

        if data["status"] == 1:
            base = data["base"]

        time.sleep(0.005)

        cnt = 0
        # print("b%s-w4" % blockid)

        try:
            f = open(path, "rb")
            '''
            while (cnt + base) < size:
                buf = f.read(base)
                s.send(buf)
                cnt += base
                s.recv(1)

            if size - cnt > 0:
                buf = f.read(size - cnt)
                s.send(buf)
                s.recv(1)
            '''

            s.sendfile(f)

            f.close()
        except Exception as e:
            f.close()

        s.shutdown(socket.SHUT_WR)

        data = s.recv(1024)

        s.close()

        td = threading.Thread(target=self.check_storage, args=(temp, bkh, blockid, node))
        td.start()

        exit(0)

    def check_storage(self, temp, bkh, blockid, node, pid=0, flag=True):

        inf = {}
        s = send_data(node)
        inf["code"] = "FC07"
        inf["block"] = temp
        inf["bk_id"] = bkh
        s.send(json.dumps(inf, ensure_ascii=False).encode('utf-8'))

        # print("\n\nid:%s\ninf: %s\n\n\n" % (blockid, json.dumps(inf, ensure_ascii=False)))

        status = s.recv(1024)
        if error_checker(status):
            exit(1)
        status = json.loads(status.decode('utf-8'))

        # print("b%s-w5" % blockid)

        end = time.time()
        '''
        if status["code"] == 1:
            print("block %d send time is %f" % (blockid, end - be + errortime))
            s.close()
            self.uploaded.append(blockid)
            print("lenth: ", len(self.uploaded))
            if len(self.uploaded) == self.block_num:
                self.clear_blocks()
        else:
            print("status: ", status)
            print("The block has been broken during the upload process, reload now to the block %d" % blockid)
            print("The block %d hash is %s, and the node is %s" % (blockid, bkh, node))
            errortime += 1
            td = threading.Thread(target=self.sendblock, args=(node, blockid, self.op_id, end - be))
            global tds
            tds.append(td)
            td.start()
            while not td.is_alive():
                td.start()
        '''
        # print("\n\nid:%s\ninf: %s\n\n\n" % (blockid, json.dumps(inf)))
        if status["code"] == 1:
            if flag:
                print("block %d check successful" % blockid)
                self.uploaded.append(blockid)
            else:
                print("priority %d check successful" % (pid % self.block_num))
                self.done += 1
        else:
            if flag:
                print("block %d failed to check and reloading... ..." % blockid)
                td = threading.Thread(target=self.sendblock, args=(node, blockid, self.op_id))
                td.start()
            else:
                print("priority %d failed to check and reloading... ..." % (pid % self.block_num))
                td = threading.Thread(target=self.transpri, args=(node, pid - self.ps, self.op_id))
                td.start()
                print("self.done = ", self.done)

        if len(self.uploaded) == self.block_num:
            self.clear_blocks()

        exit(0)

    """
    Begin to election and transmission
    """

    def start(self, origin):
        if not self.check_nodes():
            print("NODES HASH ERROR!")
            # get new nodes from network
            pass
        global mistake_hash
        head = self.sele_headn()

        f = open("metafile.dat", "r+")

        data = json.loads(f.read())

        f.close()

        path = "cache\\\\cache_0"
        bkh = sm3().get_block_hash(path)
        while bkh == mistake_hash:
            bkh = sm3().get_block_hash(path)
        source = sm3().sm3_file(origin)
        while source == mistake_hash:
            source = sm3().sm3_file(origin)

        """
        f.write(bytes.fromhex(source))
        f.write(bytes.fromhex(bkh))
        f.write(head.encode())
        f.write(("\n" + str(self.block_num)).encode('utf-8'))
        """

        self.send2beginner(head, 0, origin)

        while len(self.uploaded) != self.block_num:
            continue

        with open("metafile.dat", "w") as f:
            data["source"] = source
            data["first"] = bkh
            data["header"] = head
            data["num"] = self.block_num
            data["offset"] = self.ps
            data["prio_locs"] = self.prio_loc
            data["firsterror"] = self.firsterror

            data = json.dumps(data)
            f.write(data)
            #print("%s wirtie over" % time.time())

        rec = open("rec/%s.dat" % time.time(), "w")
        rec.write(json.dumps(self.nodes, ensure_ascii=False))
        rec.close()

    '''
        transfer priority block
    '''

    def transpri(self, node, pid, session):
        blockid = pid
        pid += self.ps
        print("send priority %d_%d to %s" % (pid % self.block_num, (pid + 1) % self.block_num, node))
        global mistake_hash
        be = time.time()
        req = {}

        path = ".\\\\cblocks\\\\priority_%d_%d" % (pid % self.block_num, (pid + 1) % self.block_num)
        # print(blockid, path)
        size = get_FileSize(path)

        bkh = sm3().sm3_file(path)
        # print("b%s-w1" % blockid)
        while bkh == mistake_hash:
            time.sleep(0.3)
            bkh = sm3().sm3_file(path)
        temp = bkh

        b1 = os.path.join(".\\cache", "cache_%d" % (pid % self.block_num))
        b2 = os.path.join(".\\cache", "cache_%d" % ((pid + 1) % self.block_num))

        #print("%s add pri %d %d" % (time.time(), pid % self.block_num, (pid + 1) % self.block_num))
        self.prio_loc["%d_%d" % (pid % self.block_num, (pid + 1) % self.block_num)] = [bkh, node, get_FileSize(b1),
                                                                                       get_FileSize(b2)]

        # ready upload
        s = send_data(node)

        # print("b%s-w3" % blockid)
        while s is None:
            s = send_data(node)
        req["code"] = "F000"
        # base = 1500
        if session is not None:
            req["op_id"] = session
        else:
            print("Uoload priority" + blockid + " failed, due to without session.")
            s.close()
            return
        req["bk_id"] = bkh
        req["temp"] = temp
        req["bk_size"] = size
        req["base"] = size
        base = size

        s.send(json.dumps(req, ensure_ascii=False).encode('utf-8'))

        data = s.recv(1024)
        data = json.loads(data.decode('utf-8'))

        if data["status"] == 1:
            base = data["base"]

        time.sleep(0.005)

        cnt = 0
        # print("b%s-w4" % blockid)

        try:
            f = open(path, "rb")

            s.sendfile(f)

            f.close()

        except Exception as e:
            f.close()

        s.shutdown(socket.SHUT_WR)

        data = s.recv(1024)

        s.close()

        td = threading.Thread(target=self.check_storage, args=(temp, bkh, blockid, node, pid, False))
        td.start()

        exit(0)


class FileDownload:
    def __init__(self):
        self.prio_loc = None
        self.mask = None
        self.source = None
        self.first = None
        self.header = None
        self.num = None
        self.firsterror = None
        self.self_set()
        self.finished = False
        self.rebuilding = False
        self.found = {}
        self.downloaded = []
        self.processing = []
        self.repaired = []
        self.clear_dir("./downloads")

    def self_set(self):
        with open("metafile.dat", "r") as f:
            data = json.loads(f.read())
            self.mask = bytes.fromhex(data["mask"])
            self.source = data["source"]
            self.first = data["first"]
            self.header = data["header"]
            self.num = data["num"]
            self.prio_loc = data["prio_locs"]
            self.firsterror = data["firsterror"]

    def get_num(self):
        return self.num

    def find_block(self, hash, index):
        with open("nodes.dat", "r") as f:
            nodes = f.read().split("\n")

        class asked:
            def __init__(self):
                self.asked = 0
                self.found = False

            def wait(self):
                while self.asked != 0:
                    if not self.found:
                        continue
                    else:
                        break

            def p(self):
                self.asked += 1

            def v(self):
                self.asked -= 1

        askedc = asked()

        for i in nodes:
            if i == "":
                continue
            askedc.p()
            td = threading.Thread(target=self.ask, kwargs={"hash": hash, "i": i, "index": index, "mutex": askedc})
            td.start()

        askedc.wait()

        if not askedc.found:

            ef, ref_hash = self.block_cached((index + 1) % self.num)
            if ef:
                p1 = self.prio_loc["%d_%d" % (index, (index + 1) % self.num)]
                reinf = (p1, True, ref_hash, hash)
                td = threading.Thread(target=self.get_priority,
                                      kwargs={"block_hash": p1[0], "loc": p1[1], "inf": reinf, "index": index})
                td.start()
            else:
                ef, ref_hash = self.block_cached((index - 1) % self.num)
                if ef:
                    p2 = self.prio_loc["%d_%d" % ((index - 1) % self.num, index)]
                    reinf = (p2, False, ref_hash, hash)
                    td = threading.Thread(target=self.get_priority,
                                          kwargs={"block_hash": p2[0], "loc": p2[1], "inf": reinf, "index": index})
                    td.start()
                else:
                    print("FILE LOST! UNCACHED!")
                    exit(1)
        exit(0)

    # inf def repaire(self, pinf(phash, node), flag, ref_hash, hash):
    def repaire(self, inf, index):
        # print("REP-INF: ", inf)
        if inf[-1] in self.repaired:
            exit(0)
        c = generate_priority(os.path.join(os.getcwd(), "recovery"))

        if inf[1]:
            self.rebuild_block(os.path.join(".\\downloads", "down_%s.dat" % inf[-2]), False)
            res = c.decode(inf[-1], inf[0][0], inf[-2], inf[1], inf[0][2])
            td = threading.Thread(target=self.rebuild_block,
                                  kwargs={"block": os.path.join(".\\downloads", "down_%s.dat" % inf[-2])})
            td.start()
        else:
            self.rebuild_block(os.path.join(".\\downloads", "down_%s.dat" % inf[-2]), False)
            res = c.decode(inf[-2], inf[0][0], inf[-1], inf[1], inf[0][3])
            td = threading.Thread(target=self.rebuild_block,
                                  kwargs={"block": os.path.join(".\\downloads", "down_%s.dat" % inf[-2])})
            td.start()

        if res:
            print("Recover Successful @ %s !" % inf[-1])

            with open(os.path.join(".\\downloads", "down_%s.dat" % inf[-1]), "rb") as f:
                pre = f.read(32)
                cur = f.read(32)
                next = f.read(32)

            ans1 = ""
            ans2 = ""

            for i in range(32):
                temp1 = hex(pre[i] ^ self.mask[i])
                temp2 = hex(next[i] ^ self.mask[i])
                if len(temp1) == 4:
                    ans1 += temp1[2::].upper()
                else:
                    ans1 += "0" + temp1[2::].upper()
                if len(temp2) == 4:
                    ans2 += temp2[2::].upper()
                else:
                    ans2 += "0" + temp2[2::].upper()

            pre = ans1
            next = ans2

            td = threading.Thread(target=self.rebuild_block,
                                  kwargs={"block": os.path.join(".\\downloads", "down_%s.dat" % inf[-1])})
            td.start()
            self.downloaded.append(inf[-1])
            self.repaired.append(inf[-1])
            print(len(self.downloaded), self.num, len(self.found))

            if next not in self.downloaded:
                td = threading.Thread(target=self.find_block, kwargs={"hash": next, "index": (index + 1) % self.num})
                td.start()
            if pre not in self.downloaded:
                td = threading.Thread(target=self.find_block, kwargs={"hash": pre, "index": (index - 1) % self.num})
                td.start()

            if int(len(self.downloaded)) == int(self.num):
                print("end")
                self.finished = True
        else:
            print("Recover Failed @ %s !" % inf[-1])
        exit(0)

    def block_cached(self, id):
        for hash in self.found:
            if self.found[hash][1] == id:
                return True, hash
        return False, None

    def get_block_inf(self, block):
        p = block.read(32)
        c = block.read(32)
        n = block.read(32)
        return p, c, n

    def rebuild_block(self, block, flag=True):
        try:

            self.rebuilding = True
            block_hash = block[block.find("down_") + 5:block.find("down_") + 69]
            cache_path = "recache_%s" % block_hash

            src = open(block, "rb")
            cache = open(cache_path, "wb")

            src_inf = list(self.get_block_inf(src))
            cache_inf = [""] * 3

            for i in range(32):
                temp0 = hex(src_inf[0][i] ^ self.mask[i])
                temp2 = hex(src_inf[2][i] ^ self.mask[i])
                if len(temp0) == 4:
                    cache_inf[0] += temp0[2::].upper()
                else:
                    cache_inf[0] += "0" + temp0[2::].upper()

                if len(temp2) == 4:
                    cache_inf[2] += temp2[2::].upper()
                else:
                    cache_inf[2] += "0" + temp2[2::].upper()

            cache_inf[0] = bytes.fromhex(cache_inf[0])
            cache_inf[2] = bytes.fromhex(cache_inf[2])

            cache.write(cache_inf[0])
            cache.write(src_inf[1])
            cache.write(cache_inf[2])
            cache.write(src.read())

            cache.close()
            src.close()

            while self.is_used(block):
                print("block using 2" % block)
                continue

            os.remove(block)
            os.rename(cache_path, block)
            self.rebuilding = False
            if flag:
                exit(0)
        except Exception as e:
            print(f"\033[91mERROR INF: {e}\033[0m")
            self.rebuilding = False
            if flag:
                exit(0)

    def is_used(self, file_name):
        try:
            vHandle = win32file.CreateFile(file_name, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL, None)
            return int(vHandle) == win32file.INVALID_HANDLE_VALUE
        except:
            return True

    def p2p_get1(self, block_hash, loc, index, flag=None):
        try:
            if block_hash in self.downloaded or block_hash in self.processing:
                exit(0)
            print(block_hash, loc)
            self.processing.append(block_hash)
            global mistake_hash
            path = "downloads\\down_%s.dat" % block_hash
            data = {}
            s = send_data(loc)
            data["code"] = "F006"
            data["block"] = block_hash
            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))

            res = s.recv(1024)
            if error_checker(res):
                exit(1)
            res = json.loads(res.decode('utf-8'))

            if res["code"] != "F007":
                print("The node given an error code.")
                return

            base = res["base"]
            size = res["block_size"]
            fhash = res["fhash"]

            # while True:
            f = open(path, "wb+")

            '''
            cnt = 0
            while (cnt + base) < size:
                f.write(s.recv(base))
                cnt += base
                time.sleep(0.0005)

            if size - cnt > 0:
                f.write(s.recv(size - cnt))
            '''

            buf = s.recv(base)
            while buf:
                f.write(buf)
                buf = s.recv(base)


            f.close()
            s.close()

            check = threading.Thread(target=self.check_download, args=(path, index, block_hash, loc, fhash))
            check.start()

        except Exception as e:
            self.processing.remove(block_hash)
            td = threading.Thread(target=self.p2p_get1, kwargs={"block_hash": block_hash, "loc": loc, "index": index, "flag": e})
            print("Error! Reloading block-%s from %s" % (block_hash, loc))
            print(f"\033[91mERROR INF: {e}\033[0m")
            if flag is None:
                td.start()
            elif str(e) != str(flag):
                td.start()
            exit(0)

    def check_priority_download(self, block_hash, loc, inf, index, path, fhash):
        calhash = sm3().sm3_file(path)
        cnt = 0
        global mistake_hash
        while calhash == mistake_hash and cnt < 20:
            cnt += 1
            calhash = sm3().sm3_file(path)

        if calhash != fhash:
            self.processing.remove(block_hash)
            os.remove(path)
            td = threading.Thread(target=self.get_priority,
                                  kwargs={"block_hash": block_hash, "loc": loc, "inf": inf, "index": index})
            print("reloading block-%s from %s" % (block_hash, loc))
            td.start()
            exit(0)
        else:
            self.processing.remove(block_hash)
            # do the repair work
            self.repaire(inf, index)


        pass



    def check_download(self, path, index, block_hash, loc, fhash):
        recv_fh = sm3().get_block_hash(path)
        while recv_fh == mistake_hash:
            recv_fh = sm3().get_block_hash(path)

        calhash = sm3().sm3_file(path)
        cnt = 0
        while calhash == mistake_hash and cnt < 20:
            cnt += 1
            calhash = sm3().sm3_file(path)

        flagg = False

        if block_hash != recv_fh or calhash != fhash:
            flagg = True
            self.processing.remove(block_hash)
            os.remove(path)
            td = threading.Thread(target=self.p2p_get1,
                                  kwargs={"block_hash": block_hash, "loc": loc, "index": index})
            print("reloading block-%s from %s" % (block_hash, loc))
            td.start()
            exit(0)

        f = open(path, "rb")
        pre = f.read(32)
        cur = f.read(32)
        next = f.read(32)
        f.close()

        ans1 = ""
        ans2 = ""

        for i in range(32):
            temp1 = hex(pre[i] ^ self.mask[i])
            temp2 = hex(next[i] ^ self.mask[i])
            if len(temp1) == 4:
                ans1 += temp1[2::].upper()
            else:
                ans1 += "0" + temp1[2::].upper()
            if len(temp2) == 4:
                ans2 += temp2[2::].upper()
            else:
                ans2 += "0" + temp2[2::].upper()

        pre = ans1
        next = ans2

        if block_hash in self.downloaded or len(self.downloaded) == self.num:
            exit(0)

        td = threading.Thread(target=self.rebuild_block, kwargs={"block": path})
        td.start()
        self.downloaded.append(cur.hex().upper())
        # print(self.downloaded, "\nrebuildblock-%s" % path)
        print(len(self.downloaded), self.num, len(self.found))
        if int(len(self.downloaded)) == int(self.num):
            print("end")
            self.finished = True

        if next not in self.downloaded:
            td = threading.Thread(target=self.find_block, kwargs={"hash": next, "index": (index + 1) % self.num})
            td.start()
        if pre not in self.downloaded:
            td = threading.Thread(target=self.find_block, kwargs={"hash": pre, "index": (index - 1) % self.num})
            td.start()
        self.processing.remove(block_hash)

    def get_priority(self, block_hash, loc, inf, index, flag=None):
        try:
            if block_hash in self.processing:
                exit(0)
            print("PRIORITY ", block_hash, loc)
            self.processing.append(block_hash)
            global mistake_hash
            path = "recovery\\priority_%s.dat" % block_hash
            data = {}
            s = send_data(loc)
            data["code"] = "F006"
            data["block"] = block_hash
            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))

            res = s.recv(1024)
            if error_checker(res):
                exit(1)
            res = json.loads(res.decode('utf-8'))

            if res["code"] != "F007":
                print("The node given an error code.")
                return

            base = res["base"]
            size = res["block_size"]
            fhash = res["fhash"]

            f = open(path, "wb+")
            buf = s.recv(base)
            while buf:
                f.write(buf)
                buf = s.recv(base)
            f.close()
            s.close()

            check = threading.Thread(target=self.check_priority_download, args=(block_hash, loc, inf, index, path, fhash))
            check.start()

        except Exception as e:
            self.processing.remove(block_hash)
            td = threading.Thread(target=self.get_priority,
                                  kwargs={"block_hash": block_hash, "loc": loc, "inf": inf, "index": index, "flag": (e, True)})
            print("Error! Reloading block-%s from %s" % (block_hash, loc))
            print(f"\033[91mERROR INF: {e}\033[0m")
            if flag is None:
                td.start()
            elif str(e) != str(flag[0]):
                td.start()
            else:
                if inf[1] and flag[1]:
                    ef, ref_hash = self.block_cached((index - 1) % self.num)
                    if ef:
                        p2 = self.prio_loc["%d_%d" % ((index - 1) % self.num, index)]
                        reinf = (p2, False, ref_hash, inf[-1])
                        td = threading.Thread(target=self.get_priority,
                                              kwargs={"block_hash": p2[0], "loc": p2[1], "inf": reinf, "index": index, "flag": (e, False)})
                        td.start()
                        #print("first repair failed, inf2: ", reinf)
                elif flag[1]:
                    ef, ref_hash = self.block_cached((index + 1) % self.num)
                    if ef:
                        p1 = self.prio_loc["%d_%d" % (index, (index + 1) % self.num)]
                        reinf = (p1, True, ref_hash, inf[-1])
                        td = threading.Thread(target=self.get_priority,
                                              kwargs={"block_hash": p1[0], "loc": p1[1], "inf": reinf, "index": index, "flag": (e, False)})
                        #print("first repair failed, inf2: ", reinf)
                        td.start()
            exit(0)

    def ask(self, hash, i, index, mutex):
        try:
            data = {"code": "F003", "ask": hash}
            s = send_data(i)
            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            ans = s.recv(1024)
            if error_checker(ans):
                exit(1)
            s.close()
            ans = json.loads(ans.decode('utf-8'))

            if ans["code"] != "F004":
                print("The node given an error code in asking the block.")
                exit(0)

            mutex.v()

            if ans["statu"] == 1:
                mutex.found = True
                self.found[str(hash)] = [i, index]
                # self.p2p_get1(hash, i)
                td = threading.Thread(target=self.p2p_get1, kwargs={"block_hash": hash, "loc": i, "index": index})
                td.start()

            exit(0)
        except Exception as e:
            print(f"ASK FAILED at node %s: {e}" % (i))
            mutex.v()
            exit(0)

    def clear_dir(self, folder_path):
        try:
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return True
        except Exception as e:
            return False

    def download(self, ):
        try:
            data = {}
            s = send_data(self.header)
            data["code"] = "F002"
            data["block_hash"] = self.first

            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            res = s.recv(1024)
            if error_checker(res):
                exit(1)
            res = json.loads(res.decode('utf-8'))
            s.close()

            if res["is_get"] == 1:
                td = threading.Thread(target=self.p2p_get1,
                                      kwargs={"block_hash": self.first, "loc": res["loc"], "index": 0})
                self.found[str(self.first)] = [res["loc"], 0]
                td.start()
                # self.p2p_get1(self.first, res["loc"])
            else:
                self.firstlost()
        except :
            self.firstlost()

        while not self.finished or self.rebuilding:
            continue

    def firstlost(self):
        data = {}
        s = send_data(self.firsterror["sec"][1])
        data["code"] = "F002"
        data["block_hash"] = self.firsterror["sec"][0]

        s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
        res = s.recv(1024)
        if error_checker(res):
            exit(1)
        res = json.loads(res.decode('utf-8'))
        s.close()

        if res["is_get"] == 1:
            td = threading.Thread(target=self.p2p_get1,
                                  kwargs={"block_hash": self.firsterror["sec"][0], "loc": res["loc"], "index": 1})
            self.found[str(self.firsterror["sec"][0])] = [res["loc"], 1]
            td.start()
            # self.p2p_get1(self.first, res["loc"])
        else:
            s = send_data(self.firsterror["end"][1])
            data["code"] = "F002"
            data["block_hash"] = self.firsterror["end"][0]

            s.send(json.dumps(data, ensure_ascii=False).encode('utf-8'))
            res = s.recv(1024)
            if error_checker(res):
                exit(1)
            res = json.loads(res.decode('utf-8'))
            s.close()

            if res["is_get"] == 1:
                td = threading.Thread(target=self.p2p_get1,
                                      kwargs={"block_hash": self.firsterror["end"][0], "loc": res["loc"],
                                              "index": self.num - 1})
                self.found[str(self.firsterror["end"][0])] = [res["loc"], self.num - 1]
                td.start()
            else:
                print("\033[91mThe File Lost !\033[0m")


class sm3:
    def __init__(self):
        self.sm3dll = cdll.LoadLibrary('./sm3.dll')

    def return_res(self, out):
        #return out.value.hex().upper()
        return out.raw.hex().upper()

    def sm3_file(self, path):
        while self.is_used(path):
            # print("the file %s is using..." % path)
            continue
        path = create_string_buffer(path.encode('utf-8'))
        # buf = (c_char * 32)()
        buf = create_string_buffer(32)
        # flag = self.sm3dll.sm3_file(path, byref(buf))
        flag = self.sm3dll.sm3_file(path, buf)
        while flag != 0:
            flag = self.sm3dll.sm3_file(path, buf)
        return self.return_res(buf)

    def cal_sm3(self, buf):
        # output = (c_ubyte * 32)()
        output = create_string_buffer(32)
        inp = create_string_buffer(buf.encode('utf-8'))
        self.sm3dll.sm3(inp, len(buf), byref(output))
        return self.return_res(output)

    def get_block_hash(self, path):
        f = open(path, "rb")
        f.read(32)
        buf = f.read(32)
        f.close()
        return buf.hex().upper()

    def is_used(self, file_name):
        try:
            vHandle = win32file.CreateFile(file_name, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL, None)
            return int(vHandle) == win32file.INVALID_HANDLE_VALUE
        except:
            return True


"""
    This class is designed for recovery the encrypted file.
    New this class needs 2 parameters respectively, the downloaded cache path
    and the head block's hash value.
    Run the start() function only to get start with processing.
"""


class recovery:
    """
        :param: cachepath:  str, the downloaded block cache's directory's path
        :param: headhashL:  hex str, the head block's hash value
        class global parameters:
            :param self.capath:     str, the downloaded block cache's directory's path
            :param self.head:       str, the head block hash value of SM3
            :param self.files:      list, at the beginning, it's the all the downloaded cache's paths.
                            It's converted to dict-list when the function construct() called, each
                            item is a dictionary, in the item, there are 4 key-value pairs:
                                * "path":   the block's physical path
                                * "pre":    previous block's SM3 hash value
                                * "cur":    this block's hash value of SM3
                                * "next":   the next block hash value
            :param block_inf: list, the original block's order

    """

    def __init__(self, cachepath, headhash, num):
        self.capath = cachepath
        self.head = headhash
        self.files = os.listdir(self.capath)
        while int(num) > len(self.files):
            self.files = os.listdir(self.capath)
            print("len:", len(self.files))
        self.block_inf = []
        self.offset = None
        self.key = ""

    """
        It is a recursive function which truly construct the block_inf's order.
        Input:
            :param hash:    hex str, the block's hash which ready to insert.
            :param flag:    bool, the function begin flag.
                                False: Function has not be started.
                                True: Function has been called.
            :param id:      int, the index in the self.files
    """

    def find(self, hash, flag, id):
        # checking the cycle has been traversed.
        if hash == self.head and flag:
            return

        # append into the order
        self.block_inf.append(self.files[id]["path"])

        # traver the self.files to find the next block
        for i in range(len(self.files)):
            if self.files[i]["cur"] == self.files[id]["next"]:
                nextid = i
                break

        # append the next block
        self.find(self.files[id]["next"], True, nextid)

    """
        This function is used for getting a block's inf.
        Input:
            :param path:    str, the block's physical directory
        Output:
            :param inf:     dictionary, the block's inf which ready to be a item in the self.files
    """

    def __get_inf(self, path):
        # init
        inf = {"path": path, "pre": "", "cur": "", "next": ""}
        while inf["pre"] == "" or inf["cur"] == "" or inf["next"] == "":
            if is_used(path):
                time.sleep(1)
                continue
            f = open(path, "rb")
            inf["pre"] = f.read(32).hex().upper()  # the previous block hash value
            inf["cur"] = f.read(32).hex().upper()  # the current block hash value
            inf["next"] = f.read(32).hex().upper()  # the next block hash value
            f.close()
        return inf

    """
        The construction is prepared for recovering the original block order.
        There 2 steps respectively.
        The 1st step is preparing the all the blocks' information to the self.files.
        The 2ed step is get the order with each block's hash values information.
    """

    def construct(self):
        # :param length:    int, the block's amount
        # :param headid:    int, the head block's index in the self.files
        length = len(self.files)
        headid = None

        # travel the self.files, to get all the blocks' information
        for i in range(length):
            #   get the full path
            path = os.path.join(self.capath, self.files[i])
            #   get the block inf
            self.files[i] = self.__get_inf(path)
            # checking the head block
            if self.files[i]['cur'] == self.head:
                headid = i

        # the 2ed step
        self.find(self.files[headid]["cur"], False, headid)

    """
        this function is designed for appending a block to recovery file.
        :param src: str, the ready to append block's path
        :param dst: file obj, the recovery file
    """

    def append_block(self, src, dst, idx):
        tof = open(src, "rb")
        # passing by the hash pointer domain
        tof.seek(96 + self.offset[idx], 0)
        # reading the remain data to write in the recovery file
        buf = tof.read()
        dst.write(buf)
        tof.close()

    def __get_key(self):
        files = []
        key = ""
        for i in self.block_inf:
            files.append(open(i, "rb"))

        cnt = -1
        leng = len(self.block_inf)
        self.offset = [0] * leng
        for i in range(32):
            mod = i % leng
            if mod == 0 and 32 / leng > mod:
                cnt += 1

            files[mod].seek(96 + cnt)
            buf = files[mod].read(1)
            self.offset[mod] += 1
            key += buf.hex().upper()

        for i in files:
            i.close()

        self.key = key

    """
        rebuilding the encryption file
    """

    def rebuild(self):
        self.__get_key()
        f = open("download.dat", "wb")
        # constructing the recovery file according to the original block order
        for i in self.block_inf:
            self.append_block(i, f, self.block_inf.index(i))
        f.close()

    def decrypt(self):
        keyfile = open("key.temp", "wb+")
        key = bytes.fromhex(self.key)
        keyfile.write(key)
        keyfile.close()
        build_block(True).decryption()

    """
        getting start
    """

    def start(self):
        self.construct()
        self.rebuild()
        self.decrypt()


class build_block:
    def __init__(self, flag=None):
        # if flag is None:
        self.dll = cdll.LoadLibrary('./encryption1101.dll')
        #    # self.dll = cdll.LoadLibrary('./export10122.dll')
        # else:
        # self.dll = cdll.LoadLibrary('./encryptionbak.dll')

    def trans2ct(self, str):
        return create_string_buffer(str.encode(), len(str))

    def prepare(self, path, blocks=10):
        path = self.trans2ct(path)
        return self.dll.init(path, blocks)

    def decryption(self):
        download = self.trans2ct("download.dat")
        output = self.trans2ct("decryption.dat")
        key = self.trans2ct("key.temp")
        self.dll.decrypt(download, output, key)


class generate_priority:
    def __init__(self, path):
        self.path = path
        # self.dll = cdll.LoadLibrary('./export103105.dll')
        # self.dll = cdll.LoadLibrary('./export110102.dll')
        self.dll = cdll.LoadLibrary('./rs1126.dll')
        self.nums = len(os.listdir("./cache")) - 1
        self.done = 0

    def clear_dir(self, folder_path):
        try:
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            return True
        except Exception as e:
            return False

    def generate(self):
        self.clear_dir(self.path)
        # print("entring... ...", self.done, self.nums)
        for i in range(self.nums):
            td = threading.Thread(target=self.encode_blocks, args=(i,))
            td.start()

        while self.done != self.nums:
            # print(self.done, self.nums)
            continue

    def encode_blocks(self, i):
        blockfile_i = os.path.join(".\\cache", "cache_%d" % (i % self.nums))
        blockfile_iplus = os.path.join(".\\cache", "cache_%d" % ((i + 1) % self.nums))
        priority = os.path.join(self.path, "priority_%s_%s" % ((i % self.nums), (i + 1) % self.nums))

        # print(priority)

        blockfile_i = create_string_buffer(blockfile_i.encode())
        blockfile_iplus = create_string_buffer(blockfile_iplus.encode())
        priority = create_string_buffer(priority.encode())

        flag = self.dll.rs_encodef(blockfile_i, blockfile_iplus, priority)

        if flag == 0:
            self.done += 1

        exit(0)

    def decode(self, src, rs, target, fec, size):

        print(src, rs, target, fec)

        src = os.path.join(".\\downloads", "down_%s.dat" % src)
        ssrc = src
        src = create_string_buffer(src.encode())

        rs = os.path.join(".\\recovery", "priority_%s.dat" % rs)
        rs = create_string_buffer(rs.encode())

        target = os.path.join(".\\downloads", "down_%s.dat" % target)
        starget = target
        target = create_string_buffer(target.encode())

        print("FEC: ", fec)

        if fec:
            fec = 1
        else:
            fec = 0

        flag = self.dll.rs_fec(src, rs, target, fec)

        print("FLAG: ", flag)

        if flag == 0:
            if fec:
                self.size_fix(ssrc, size)
            else:
                self.size_fix(starget, size)
            return True
        else:
            return False

    def size_fix(self, path, size):
        # print("r-size %s" % size)
        with open(path, 'rb+') as file:
            file.seek(size)
            file.truncate()

    def is_used(self, file_name):
        try:
            vHandle = win32file.CreateFile(file_name, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL, None)
            return int(vHandle) == win32file.INVALID_HANDLE_VALUE
        except:
            return True
