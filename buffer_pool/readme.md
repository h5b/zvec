### 1. compile

```bash
g++ -std=c++17 -O3 generate_bin.cc -o generate_bin
g++ -std=c++17 -I./parallel-hashmap/parallel_hashmap -O3 bench.cc -pthread -o bench
```

### 2. generate binary data file

```bash
./generate_bin data_4g.bin 4 # generate a 4GB data file
```

### 3. run

```bash
./bench \
    data_4g.bin \   # data file
    6 \             # buffer pool capacity is 6GB (larger than data file)
    1024 \          # access block size is 1024 Bytes
    1000 \          # each request access 1000 blocks, total 100000 requests
    32              # 32 threads
```
./bench data_4g.bin 6 1024 1000 32