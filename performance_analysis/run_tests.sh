#!/bin/bash

for APIS in 1 2 4 8 16 32 64 128 256 512 1024 2048
do
    python3 timeit_test.py --number-of-results 1 --number-of-apis $APIS
done

for APIS in 1 2 4 8 16 32 64 128 256 512 1024 2048
do
    python3 timeit_test.py --number-of-apis $APIS
done
