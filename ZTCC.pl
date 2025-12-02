#!/bin/bash
# !!!this is bash script not perl script!!!
#always get remote server ip address by command nmcli via DHCP option
#dhcp servcie must has dhcp option 'next_server'
# see spec: https://confluence.ztsystems.com/display/IMT/MFG+Test+Script+Deployment+Improvement+Arch+and+Plan
#version 1
#init by deping liang
#date: 2025-11-10
#
next_server=$(nmcli -f DHCP4 device show | grep next_server | tail -1 | awk '{print $NF}')
if ! test ${next_server}; then 
	echo "**exit/stop from ZTCC.pl causing by unable to get *key*:next_server/local repo server ip by command: nmcli -f DHCP4 device show"
	exit 1
fi

#set the url 
repo_url="http://${next_server}/deployment/pull"

#set the repo url in system environment 
# this is for the pull.py script to use but it can be overridden by the environment variable REPO_URL for pull.py script to use in debug/engineering/test mode
# for example: 
#   export REPO_URL="http://<local repo/stage/debug/engineering repo server ip>/deployment/pull" 
#   export REPO_URL="http://<local repo/stage/debug/engineering repo server ip>/deployment/stage/pull" 
#   export REPO_URL="http://${next_server}/deployment/engineering/pull" 
#export REPO_URL=http://${next_server}/deployment/pull


#make sure we have wget then can tell when you creating the new pxe image you may missing install 
if ! type wget >/dev/null 2>&1; then
    echo "**exit/stop from ZTCC.pl causing by missing application /usr/bin/wget"
    exit 1
fi

#get 'pull' script set from local repo
#wget retry configuration: --tries=6 (6 attempts total), --timeout=30 (30 seconds timeout), --waitretry=10 (wait 10 seconds between retries)
#show wget version 
wget -V 
#
wget --tries=6 --timeout=30 --waitretry=10 -O /usr/sbin/pull.py -P /usr/sbin/  ${repo_url}/pull.py
pull_py=$?

if [[ $pull_py -ne 0 ]];then
    echo "** exit/stop from ZTCC.pl causing by wget command to get script pull.py failure after 6 attempts"
    exit 1
fi

wget --tries=6 --timeout=30 --waitretry=10 -O /usr/sbin/pull.py.md5 -P /usr/sbin/  ${repo_url}/pull.py.md5
pull_py_md5=$?

if [[ $pull_py_md5 -ne 0 ]];then
    echo "** exit/stop from ZTCC.pl causing by wget command to get script pull.py.md5 failure after 6 attempts"
    exit 1
fi

#check all condition are good
if ! test -e /usr/sbin/pull.py.md5; then
    echo "**exit/stop from ZTCC.pl causing by unable to get MD5 file: pull.py.md5 from ${repo_url}/"
    exit 1
else 
    #compare the MD5 for pull.py
    expected_md5=$(cat /usr/sbin/pull.py.md5)
    actual_md5=$(md5sum /usr/sbin/pull.py | awk '{print $1}')
    if [[ "${actual_md5}X" == "${expected_md5}X" ]]; then
        echo "**pull.py MD5 checksum matches, MD5 is: ** ${actual_md5} **"
    else
        echo "**exit/stop from ZTCC.pl causing by script pull.py MD5 checksum does NOT match, **actual: ${actual_md5} ** VS. **expected ${expected_md5} **"
        exit 1
    fi
fi

if test -e /usr/sbin/pull.py; then
    #change anyways no hurts
    chmod 777 /usr/sbin/pull.py
else
    echo "**exit/stop from ZTCC.pl causing by unable to get script: pull.py from ${repo_url}/"
    exit 1
fi

###run script pull.py get from http repo in each local MFG /deployment/pull/pull.py
#enter /opt so we can have the each gz module in this folder
cd /opt/

#run it
/usr/sbin/pull.py
pull=$?
if [[ $pull -ne 0 ]];then
    echo "** exit/stop from ZTCC.pl causing by script pull.py, check log: /var/log/pull.py.log"
    exit 1
fi

#exit with success
exit 0
