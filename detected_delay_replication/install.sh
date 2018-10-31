#!/bin/bash
make
cp ./include/* /usr/include/
ln  -s libmysqlclient.so.18  /usr/lib/libmysqlclient.so.18

