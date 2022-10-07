#!/bin/bash

# Change this to your netid
export netid=$(whoami)

# Root directory of your project
if [[ $netid -eq "ash170000" ]]; then
  export PROJDIR=$HOME/CS-6378/cs6378proj1/
else
  export PROJDIR=$HOME/cs6378/proj1/
fi

# Directory where the config file is located on your local system
export CONFIGLOCAL=./config-dual.txt

# Directory your java classes are in
export BINDIR=$PROJDIR

# Your main project class
export PROG=Node

# Directory to place log files
# ends with a slash
export OUTPUTDIR=$PROJDIR/

javac Node.java Launcher.java
java Launcher