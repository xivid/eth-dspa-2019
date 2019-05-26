import sys

for line in sys.stdin:
    timestamp = line.split(",")[0][1:]
    print(timestamp)

