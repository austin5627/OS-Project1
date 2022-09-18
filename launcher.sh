#!/bin/bash

./cleanup.sh
# Change this to your netid
export netid=$(whoami)

# Root directory of your project
if [[ $netid == ash170000 ]]; then
    export PROJDIR=$HOME/CS-6378/cs6378proj1
else
    export PROJDIR=$HOME/cs6378/proj1/cs6378proj1
fi

# Directory where the config file is located on your local system
export CONFIGLOCAL=./config.txt

# Directory your java classes are in
export BINDIR=$PROJDIR

# Your main project class
export PROG=Node

javac Node.java Launcher.java
java Launcher