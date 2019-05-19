#!/usr/bin/env python3
lines = open("resolved_stream.txt", "r").readlines()
fout = open("wrong_timestamps.txt", "w")
cvt = lambda x: x[:10] + "T" + x[11:19]
for line in lines:
  a, b = line[1:20], line.split("|")[3][:19]
  if a != b and a[:16] != b[:16]:
    fout.write(line)
fout.write("Done!")
fout.close()
