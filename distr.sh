#!/bin/bash

for i in 1 2 3 4 5; do
  scp udpsmoke.py targets.list "root@cor-test$i.srv.nix.cz:~"
  scp "root@cor-test$i.srv.nix.cz:~/cor-test$i.csv" .
done

