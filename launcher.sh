#!/bin/bash

# Change this to your netid
netid=ewc180001

# Root directory of your project
PROJDIR=$HOME/cs6378/proj1

# Directory where the config file is located on your local system
CONFIGLOCAL=$HOME/launch/config.txt

# Directory your java classes are in
BINDIR=$PROJDIR

# Your main project class
PROG=Node

n=0

cat $CONFIGLOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
    read i
    echo $i
    while [[ $n -lt $i ]]
    do
    	read line
    	node=$( echo $line | awk '{ print $1 }' )
        host=$( echo $line | awk '{ print $2 }' )
        port=$( echo $line | awk '{ print $3 }' )
	
        xterm -e "ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host java -cp $BINDIR $PROG $node $port; exec bash" &

        n=$(( n + 1 ))
    done
)
