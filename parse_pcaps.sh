#!/bin/bash
#takes name of a file, and creates a parse file in the same directory
fullpath=$1
#if [[ $(find $1 -type f -size +500 >/dev/null) ]] ; then
echo $1
cmd="./gradlew readCMEPcaps --args='$fullpath ${fullpath}.parsed'"
eval $cmd

