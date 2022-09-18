#!/bin/bash


# Change this to your netid
netid=ewc180001

#
# Root directory of your project
PROJDIR=$HOME/cs6378/proj0

#
# Directory where the config file is located on your local system
CONFIGLOCAL=./config.txt

n=0


sed -n 3,7p $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $i ]]
    do
    	read line
        host=$( echo $line | awk '{ print $2 }' )

        echo $host
        xterm -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host killall -u $netid" &
        sleep 1

        n=$(( n + 1 ))
    done
   
)


echo "Cleanup complete"
