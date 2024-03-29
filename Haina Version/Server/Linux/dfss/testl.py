from basefunctions import *


def cal(v):
    print(sm3().cal_sm3(v))


def file(path):
    print(sm3().sm3_file(path))


if __name__ == "__main__":
    cal("123456")
    file("./77.bat")

