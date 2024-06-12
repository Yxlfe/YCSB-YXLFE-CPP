#/bin/bash

# 获取脚本的绝对路径
script_path=$(readlink -f "$0")
script_dir_path=$(dirname "$script_path")
workloads="/home/zc/code/YCSB-YXLFE-CPP/workloads/blockdb_workload"
blockdb_properties="/home/zc/code/YCSB-YXLFE-CPP/blockdb/blockdb.properties"

exec="/home/zc/code/YCSB-YXLFE-CPP/build/ycsb" 
log="ycsb-blockdb-sdd-log.txt"
dbpath="/database_sdd/blockdb_ycsb_test/$(date +%Y%m%d)"  # 指定日期格式为 YYYYMMDD

if [ -d "$dbpath" ]; then  
    rm -rf $dbpath/*  
else
    echo "Directory does not exist: $dbpath. Creating directory."  
    mkdir -p $dbpath  
fi

if [ -f "$exec" ];then
    cp -f $exec $script_dir_path
else
    echo "exec ycsbc does not exist."
fi

date >> $log
cmd="./ycsb -load -threads 1 -db blockdb -dbpath $dbpath -P $workloads -P $blockdb_properties -s -dbstatistics >> $log"
echo "${cmd}" >> $log
echo "${cmd}"
eval "${cmd}" >> $log
if [ $? -ne 0 ]; then
    echo "Error: Command failed during load phase."
    exit 1
fi

# # ./ycsbc -db leveldb -dbpath $dbpath -threads 8 -P $workload -run true -dboption 1

# for file_name in $workloads; do
#   echo "Running  $file_name"
#   wait
# done