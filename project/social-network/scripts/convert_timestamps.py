import time
import sys

for line in sys.stdin:
    epoch_time = line.split(",")[0][1:]
    date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(epoch_time) / 1000))
    print("({},{},{})".format(date_time, line.split(",")[1], line.split(",")[2][:-2]))

