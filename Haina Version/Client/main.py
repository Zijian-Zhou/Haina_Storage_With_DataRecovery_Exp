"""
This part is used for storage node's election and upload the data.


"""
import json
import os
import random
import threading
import time
from basefunctions import *
from ctypes import *


def upload(origin, test):
    #print(os.getcwd())
    data = open("test_file_range.csv", "a+")
    st = time.time()
    blocks = build_block().prepare(origin, 15)
    t1 = time.time()
    print("block nums : %d" % blocks)
    generate_priority(os.path.join(os.getcwd(), "cblocks")).generate()
    t2 = time.time()
    data.write("%d, %s, %s, %s," % (get_FileSize(origin), str(st), str(t1), str(t2)))
    ele = election(test)
    ele.start(origin)


def download(test):
    st = time.time()
    filedown = FileDownload()
    num = filedown.get_num()
    filedown.download()
    ready2rec(test, st, num)


if __name__ == "__main__":
    '''
    origin = "E:\\\\programing_projects\\\\paper_python_part\\\\Client\\\\pic.jpg"
    '''
    # upload("E:\\\\programing_projects\\\\paper_python_part\\\\Client\\\\pic.jpg")
    # download()
    from monitor import *

    test().test_file_range(2)
