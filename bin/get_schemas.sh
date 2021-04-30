#!/usr/bin/env bash

storage_dir="/Volumes/Data/Reddit/"
process_dir="/Users/fgehrlicher/dev/github.com/fgehrlicher/pushshift-reddit-utilities/bin"

cd $process_dir || exit 1

while read -r file; do

  cp "${storage_dir}${file}" .

  case "$file" in
  *.zst) prefix=".zst" ;;
  esac


  file=${file%$prefix}
  out_file="${file}_schemas.json"
  command="./schemadetect \"$file\" \"$out_file\""

  echo "In: $file"
  echo "Out: $out_file"
  echo "Command: $command"

  eval "$command"

  rm "$file"

done <files_to_scan
