#!/bin/sh

if [ -z "$1" ]
  then
    echo "Decripts list at assets/exclude.yml.enc. Usage:"
    echo "./decrypt path/to/private/key"
    exit 1
fi

openssl rsautl -decrypt -inkey "$1" -in assets/exclude.yml.enc
