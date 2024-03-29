import ctypes
import json
import os
import socket
import platform
import time
from ctypes import *
from gmssl import *
#import win32file
# import psutil

def send_data(host, port=5656):
    try:
        s = socket.socket()
        s.connect((host, port))
        return s
    except:
        print("网络错误!")
        return None


def get_free_space_mb(folder):
    """
    获取磁盘剩余空间
    :param folder: 磁盘路径 例如 D:\\
    :return: 剩余空间 单位 G
    """
    if platform.system() == 'Windows':
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
        return free_bytes.value
    else:
        st = os.statvfs(folder)
        return st.f_bavail * st.f_frsize


def sort_rank(rank):
    for i in range(len(rank)):
        for j in range(i, len(rank)):
            if rank[j][1] > rank[i][1]:
                rank[i], rank[j] = rank[j], rank[i]
    return rank

def get_FileSize(filePath):
    fsize = os.path.getsize(filePath)
    return fsize

'''
class sm3:
    def __init__(self):
        #self.sm3dll = cdll.LoadLibrary('./sm3.dll')
        #self.sm3dll = cdll.LoadLibrary('./1006sm3.dll')
        #self.sm3dll = cdll.LoadLibrary('./sm3231113.so')
        self.sm3dll = cdll.LoadLibrary('./sm3test.dll')

    def return_res(self, out):
        return out.value.hex().upper()

    def sm3_file(self, path):
        p2 = path
        while self.is_used(path):
            pass
            #print("the file %s is using..." % path)
        path = create_string_buffer(path.encode('utf-8'))
        #buf = (c_char * 32)()
        buf = create_string_buffer(32)
        #flag = self.sm3dll.sm3_file(path, byref(buf))
        flag = self.sm3dll.sm3_file(path, buf)
        cnt = 0
        while flag != 0 and cnt < 20:
            cnt += 1
            while self.is_used(p2):
                pass
                #print("the file %s is using..." % p2)
            #flag = self.sm3dll.sm3_file(create_string_buffer(p2.encode(), len(p2)), buf)
            flag = self.sm3dll.sm3_file(create_string_buffer(p2.encode()), buf)
            time.sleep(0.01)
        return self.return_res(buf)

    def cal_sm3(self, buf):
        #output = (c_char * 32)()
        output = create_string_buffer(32)
        #inp = create_string_buffer(buf.encode(), len(buf))
        inp = create_string_buffer(buf.encode())
        flag = self.sm3dll.sm3(inp, len(buf), output)
        cnt = 0
        while flag != 0 and cnt < 20:
            cnt += 1
            flag = self.sm3dll.sm3(inp, len(buf), output)
        return self.return_res(output)

    def get_block_hash(self, path):
        f = open(path, "rb")
        f.read(32)
        buf = f.read(32)
        f.close()
        return buf.hex().upper()

    def is_used(self, file_name):
        """
        try:
            vHandle = win32file.CreateFile(file_name, win32file.GENERIC_READ, 0, None, win32file.OPEN_EXISTING,
                                           win32file.FILE_ATTRIBUTE_NORMAL, None)
            return int(vHandle) == win32file.INVALID_HANDLE_VALUE
        except:
            return True
        """
        for proc in psutil.process_iter(['pid', 'open_files']):
            try:
                files = proc.info['open_files']
                for f in files:
                    if f.path == file_name:
                        return True
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
        return False
'''

class sm3:

    def __init__(self):
        self.constructor = Sm3()


    def release(self):
        self.constructor.reset()

    def cal_sm3(self, input):
        self.release()
        input = input.encode()
        self.constructor.update(input)
        res = self.constructor.digest()
        return res.hex().upper()

    def sm3_file(self, path):
        self.release()

        with open(path, "rb") as f:
            self.constructor.update(f.read())

        res = self.constructor.digest()
        return res.hex().upper()

    def get_block_hash(self, path):
        f = open(path, "rb")
        f.read(32)
        buf = f.read(32)
        f.close()
        return buf.hex().upper()