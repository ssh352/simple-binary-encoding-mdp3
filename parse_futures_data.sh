files=$(find /marketdata/ice_s3/ags_futures_a/ -type f -size +10k | grep pcap | grep -v parsed)
for fullpath in $files
do
    cmd="./gradlew readCMEPcaps --args='$fullpath ${fullpath}.parsed'"
    echo $cmd
    eval $cmd
    wait
    gzip="gzip -f ${fullpath}.parsed"
    eval $gzip 
    wait

done
