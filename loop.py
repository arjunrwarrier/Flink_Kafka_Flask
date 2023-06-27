import  time
start = time.time()
while time.time() < start + 0.15:
    pass
end_time=time.time()
print((end_time-start)*1000)