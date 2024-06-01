#/bin/bash

# 获取脚本的绝对路径
script_path=$(readlink -f "$0")
script_dir_path=$(dirname "$script_path")
workloads="/home/db/YCSB-YXLFE-CPP/workloads/blockdb_rh_workload"
blockdb_properties="/home/db/YCSB-YXLFE-CPP/blockdb/blockdb.properties"

exec="/home/db/YCSB-YXLFE-CPP/build/ycsb" 
log="ycsb-log-test.txt"
dbpath="/home/db/blockdb_ycsb_test/$(date +%Y%m%d)"  # 指定日期格式为 YYYYMMDD

if [ -d "$dbpath" ]; then  
    echo "Directory exist: $dbpath."    
else
    echo "Directory does not exist: $dbpath."
    exit 1  
fi

if [ -f "$exec" ];then
    cp -f $exec $script_dir_path
else
    echo "exec ycsbc does not exist."
fi

date >> $log
cmd="./ycsb -run -threads 16 -db blockdb -dbpath $dbpath -P $workloads -P $blockdb_properties -s -dbstatistics >> $log"
echo "${cmd}" >> $log
echo "${cmd}"
eval "${cmd}" >> $log
if [ $? -ne 0 ]; then
    echo "Error: Command failed during load phase."
    exit 1
fi
