#!/bin/sh

# Encrypts the file used to list validators who don't have to meet the minimum self-stake requirement.
# Keys should be RSA. Put additional public keys in /pubkeys and add as an -inkey argument here

openssl rsautl -encrypt -pubin -inkey pubkeys/ab.pub -inkey pubkeys/listkey.pub -in exclude.yml -out assets/exclude.yml.enc
