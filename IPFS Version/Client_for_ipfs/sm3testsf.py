"""from gmssl import sm3


def calculate_sm3_hash(data):
    hash_ctx = sm3.SM3()
    hash_ctx.update(data.encode('utf-8'))
    return hash_ctx.hexdigest()"""
from basefunctions import *



if __name__ == "__main__":
    print(sm3().sm3_file("./GmSSL-master.zip"))
