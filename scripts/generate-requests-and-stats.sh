#!/usr/bin/env bash

# ethspam: https://github.com/shazow/ethspam
# versus: https://github.com/INFURA/versus
# Use a total request count for --stop-after. The duration form has a timer bug.

ethspam --rpc=http://127.0.0.1:8544/ --ratelimit=50 --method=eth_call:1 \
    | versus --concurrency=4 --stop-after=100 http://127.0.0.1:8544/
