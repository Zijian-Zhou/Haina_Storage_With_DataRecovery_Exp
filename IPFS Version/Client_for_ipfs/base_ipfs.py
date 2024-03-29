from basefunctions import *
import ipfshttpclient as api

'''
This Class over write the election class, mainly change
the upload block process to fit IPFS system.
'''

class ipfs_election(election):

    def __init__(self, test, flag=None):
        super().__init__(test, flag)
        self.mapping = {}

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

        # print("%s add pri %d %d" % (time.time(), pid % self.block_num, (pid + 1) % self.block_num))
        self.prio_loc["%d_%d" % (pid % self.block_num, (pid + 1) % self.block_num)] = [bkh, node, get_FileSize(b1),
                                                                                       get_FileSize(b2)]

        # ready upload

        url = "/dns/%s/tcp/5001/http" % node
        s = api.connect(url)
        res = s.add(path)
        block_cid = res["Hash"]

        self.prio_loc["%d_%d" % (pid % self.block_num, (pid + 1) % self.block_num)].append(block_cid)

        print("priority %d check successful" % (pid % self.block_num))
        self.done += 1


        if len(self.uploaded) == self.block_num:
            self.clear_blocks()

        exit(0)

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

        url = "/dns/%s/tcp/5001/http" % node
        s = api.connect(url)
        res = s.add(path)
        block_cid = res["Hash"]

        self.mapping["%s" % blockid] = [node, block_cid]

        print("block %d check successful" % blockid)
        self.uploaded.append(blockid)

        if len(self.uploaded) == self.block_num:
            self.clear_blocks()

        exit(0)

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
            data["mapping"] = self.mapping

            data = json.dumps(data)
            f.write(data)
            #print("%s wirtie over" % time.time())

        rec = open("rec/%s.dat" % time.time(), "w")
        rec.write(json.dumps(self.nodes, ensure_ascii=False))
        rec.close()


class ipfs_Download(FileDownload):
    def __init__(self):
        self.mapping = {}
        super().__init__()
        self.get_repath()

    def get_repath(self):
        with open("metafile.dat", "r") as f:
            data = json.loads(f.read())
            self.mapping = data["mapping"]

    def block_cached(self, id):
        for hash in self.found:
            if self.found[hash][1] == id:
                return True, self.mapping[str(id)][1]
        return False, None

    def ready_repaire(self, index, hash):
        ef, ref_hash = self.block_cached((index + 1) % self.num)
        print(ef, ref_hash)
        if ef:
            p1 = self.prio_loc["%d_%d" % (index, (index + 1) % self.num)]
            reinf = (p1, True, ref_hash, hash)
            self.get_priority(block_hash=p1[-1], loc=p1[1], inf=reinf, index=index, flag=True)
        else:
            ef, ref_hash = self.block_cached((index - 1) % self.num)
            print(ef, ref_hash)
            if ef:
                p2 = self.prio_loc["%d_%d" % ((index - 1) % self.num, index)]
                reinf = (p2, False, ref_hash, hash)
                self.get_priority(block_hash=p2[-1], loc=p2[1], inf=reinf, index=index, flag=False)
            else:
                print("FILE LOST! UNCACHED!")
                exit(1)

    def get_priority(self, block_hash, loc, inf, index, flag=None):
        print("PRIORITY-", index, block_hash, loc)
        try:
            self.processing.append(block_hash)
            path = "recovery\\priority_%s.dat" % block_hash

            s = api.connect("/dns/%s/tcp/5001/http" % loc)
            content = s.cat(block_hash)

            with open(path, "wb") as f:
                f.write(content)

            self.processing.remove(block_hash)
            # do the repair work
            self.repaire(inf, index)
        except:
            if flag is not None:
                if flag:
                    ef, ref_hash = self.block_cached((index - 1) % self.num)
                    print(ef, ref_hash)
                    if ef:
                        p2 = self.prio_loc["%d_%d" % ((index - 1) % self.num, index)]
                        reinf = (p2, False, ref_hash, inf[-1])
                        self.get_priority(block_hash=p2[-1], loc=p2[1], inf=reinf, index=index)
                    else:
                        print("FILE LOST! UNCACHED! 0x1-", index)
                        exit(1)
                else:
                    ef, ref_hash = self.block_cached((index + 1) % self.num)
                    print(ef, ref_hash)
                    if ef:
                        p1 = self.prio_loc["%d_%d" % (index, (index + 1) % self.num)]
                        reinf = (p1, True, ref_hash, inf[-1])
                        self.get_priority(block_hash=p1[-1], loc=p1[1], inf=reinf, index=index)
                    else:
                        print("FILE LOST! UNCACHED! 0x1b-", index)
                        exit(1)
            else:
                print("FILE LOST! UNCACHED! 0x2-", index)
                exit(1)


    def p2p_get1(self, block_hash, loc, index, flag=None):
        if block_hash in self.downloaded or block_hash in self.processing:
            exit(0)

        print(block_hash, loc)
        self.processing.append(block_hash)
        path = "downloads\\down_%s.dat" % block_hash

        try:

            s = api.connect("/dns/%s/tcp/5001/http" % loc)
            content = s.cat(block_hash)

            with open(path, "wb") as f:
                f.write(content)

            self.found[str(block_hash)] = [loc, index]

            check = threading.Thread(target=self.check_download, args=(path, index, block_hash, loc, block_hash))
            check.start()

        except Exception as e:
            #print(e, type(e))
            self.processing.remove(block_hash)
            if str(type(e)) in ["<class 'ipfshttpclient.exceptions.ConnectionError'>", "<class 'ipfshttpclient.exceptions.StatusError'>"]:
                print("Error in", index)
                if index != 0 :
                    self.ready_repaire(index, block_hash)
                else:
                    pre = self.mapping["%s" % ((index - 1) % self.num)][1]
                    next = self.mapping["%s" % ((index + 1) % self.num)][1]
                    if next not in self.downloaded:
                        td = threading.Thread(target=self.find_block,
                                              kwargs={"hash": next, "index": (index + 1) % self.num})
                        td.start()
                    if pre not in self.downloaded:
                        td = threading.Thread(target=self.find_block,
                                              kwargs={"hash": pre, "index": (index - 1) % self.num})
                        td.start()


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
            pass

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

        pre = self.mapping["%s" % ((index - 1) % self.num)][1]
        next = self.mapping["%s" % ((index + 1) % self.num)][1]

        if block_hash in self.downloaded or len(self.downloaded) == self.num:
            exit(0)

        td = threading.Thread(target=self.rebuild_block, kwargs={"block": path})
        td.start()
        self.downloaded.append(block_hash)
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

    def find_block(self, hash, index):
        mapi = self.mapping["%s" % index]
        td = threading.Thread(target=self.p2p_get1, kwargs={"block_hash": mapi[1], "loc": mapi[0], "index": index})
        td.start()

    def download(self, ):
        mapi = self.mapping["0"]
        td = threading.Thread(target=self.p2p_get1, kwargs={"block_hash": mapi[1], "loc": mapi[0], "index": 0})
        td.start()

        while not self.finished or self.rebuilding:
            continue
    def repaire(self, inf, index):
        print("REP-INF: ", inf)
        if inf[-1] in self.repaired:
            exit(0)
        c = generate_priority(os.path.join(os.getcwd(), "recovery"))

        if inf[1]:
            self.rebuild_block(os.path.join(".\\downloads", "down_%s.dat" % inf[-2]), False)
            res = c.decode(inf[-1], inf[0][-1], inf[-2], inf[1], inf[0][2])
            td = threading.Thread(target=self.rebuild_block,
                                  kwargs={"block": os.path.join(".\\downloads", "down_%s.dat" % inf[-2])})
            td.start()
        else:
            self.rebuild_block(os.path.join(".\\downloads", "down_%s.dat" % inf[-2]), False)
            res = c.decode(inf[-2], inf[0][-1], inf[-1], inf[1], inf[0][3])
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

            pre = self.mapping["%s" % ((index - 1) % self.num)][1]
            next = self.mapping["%s" % ((index + 1) % self.num)][1]

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