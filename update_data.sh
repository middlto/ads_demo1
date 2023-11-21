#!/bin/bash

turing="./turing_hadoop"
hadoop="./hadoop"
if [[ $# -eq 2 ]]
then
    event_day=$2
else
    event_day=`date -d "1 days ago" +"%Y%m%d"`
fi
turing_path="userpath.tu_beslevel_pv/${event_day}"

work_path=$(cd $(dirname $0);pwd)
local_path="${work_path}/data/${event_day}"
output_path="afs://xxx.xx.xx/tu_beslevel_pv/${event_day}"
cmatch_appsid_tu_path="${work_path}/data/cmatch_appsid_tu.txt"
cmatch_appsid_tu_hadoop="afs://xxx.xx.xx/cmatch_appsid_tu.txt"

max_try_time=282
function download() {
    cur_try_time=0
    ${turing} fs -test -e $1
    while [[ ${cur_try_time} -lt ${max_try_time} ]]
    do
        ${turing} fs -test -e $1
        if [[ $? -eq 0 ]]
        then
            if [[ -f $2 ]]
            then
                rm $2
            fi
            ${turing} fs -getmerge $1 $2
            if [[ `ls -l $2 | awk '{print $5}'` -gt 0 ]]
            then
                break
            fi
        fi
        cur_try_time=`expr ${cur_try_time} + 1`
        sleep 300
    done
}

function upload() {
    ${hadoop} fs -test -e $2
    if [[ $? -eq 0 ]]
    then
        ${hadoop} fs -rmr $2
    fi
    ${hadoop} fs -put $1 $2
}

function update_cmatch_appsid_tu() {
    if [[ ! -f ${local_path} ]]
    then
        echo "source file does not exist, stop update cmatch_appsid_tu."
        return
    fi

    awk '{print $2"\t"$3"\t"$4}' ${local_path} | sort | uniq > ${cmatch_appsid_tu_path}
    touch ${cmatch_appsid_tu_path}.done

    ${hadoop} fs -test -e ${cmatch_appsid_tu_hadoop}.done
    if [[ $? -eq 0 ]]
    then
        ${hadoop} fs -rmr ${cmatch_appsid_tu_hadoop}.done
    fi

    upload ${cmatch_appsid_tu_path} ${cmatch_appsid_tu_hadoop}
    upload ${cmatch_appsid_tu_path}.done ${cmatch_appsid_tu_hadoop}.done
}

download ${turing_path} ${local_path}
upload ${local_path} ${output_path}
update_cmatch_appsid_tu
