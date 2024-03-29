from basefunctions import *
#from gmssl import sm3


#data = "123456"
#hmac = sm3.SM3()
#hmac.update(data.encode('utf-8'))

#print(hmac.hexdigest())



path = './cache/bk_cache6508904DEDD304E5AE7DAFA25395AC39FAF0933B46603AE669B3FEDE21998F2F.dat'



print(sm3().sm3_file(path))
print(sm3().cal_sm3('abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd'))
