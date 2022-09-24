Austin Harris, ash170000  
Ethan Cooper, ewc18001

# Running
Edit [launcher.sh](launcher.sh) to add the correct environment variables

- netid: user to run as
- PROJDIR: path to project
- CONFIGLOCAL: path to config file
- BINDIR: path to java files
- PROG: Node

run [launcher.sh](launcher.sh) from same directory as the java files with `./launcher.sh`

it will automatically compile and run everything using the config file

# Cleanup
edit [cleanup.sh](cleanup.sh) with the same environment variables: `./cleanup.sh`

it will terminate all the running programs on the servers from the config file