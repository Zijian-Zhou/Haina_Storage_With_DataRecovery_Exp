for i in range(51, 100):
    with open("%d.bat"%i, "w") as f:
        f.write("python main.py -i 172.17.16.%d -p 5656 -n test -w test -d test" % i)
