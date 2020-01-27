#!/bin/bash
#this script iterates through a selection of pca files in a directory, parses them, then stores the results in a new file
rootpath="/marketdata/scratch/ice_definition_files/"
file_names=$(find $rootpath -iname  "*pcap.00008" -size +1000)
for fullpath in $file_names; do
    echo "$fullpath"
#    filesize=$(stat -c%s "$fullpath")
#    if [ $filesize gt "1000" ]; then
        echo "$filesize"
        cmd="./gradlew readCMEPcaps --args='$fullpath ${fullpath}.parsed'"
        echo "$cmd"
        eval $cmd
#    fi
done
