#!/bin/env python

import base64
from passlib.hash import scrypt


def secure_hash(password, base64_encoded_salt):
    hash = scrypt.using(salt=base64.b64decode(base64_encoded_salt), rounds=4, block_size=8, parallelism=1).hash(password)
    return hash


passwords=["password", "thisIsABadPassword", "bWZerzZo6fw9ZrDz*YfM6CVj2Ktx(YJd"]
salts=["AAAAAAAAAAAAAAAAAAAAAA==", "ABCDEFGHIJKLMNOPQRSTUV==", "eO+UUcKYL2gnpD51QCc+gnywQ7Eg9tZeLMlf0XXr2zc="]

for pw in passwords:
    for s in salts:
        print('Hashed "{}" with salt "{}": \t{}'.format(pw, s, secure_hash(pw, s)))

