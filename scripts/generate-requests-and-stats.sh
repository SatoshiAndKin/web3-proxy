# Got eth spam from here
# https://github.com/shazow/ethspam

# Got versus from here
# https://github.com/INFURA/versus
# ./ethspam | ./versus --stop-after 100 "http://localhost:8544/"

./ethspam http://127.0.0.1:8544/ | ./versus --concurrency=4 --stop-after 10000 http://localhost:8544/

./ethspam http://127.0.0.1:8544/ | ./versus --concurrency=4 --stop-after 10000 http://localhost:8544/
