#!/bin/bash

# This script drives GPG as well reasonably possible to re-create test keys.

SCRATCH=./gpg-scratch
rm -rf "$SCRATCH"
mkdir -m 700 -p "$SCRATCH"

gpg --homedir "$SCRATCH" --batch --passphrase '' --quick-generate-key rsa-encrypter rsa1024 encrypt
gpg --homedir "$SCRATCH" --batch --passphrase 'password' --quick-generate-key rsa-passphrase rsa1024 encrypt

gpg --homedir "$SCRATCH" --batch --passphrase '' --quick-generate-key dsa-signer dsa1024 sign
gpg --homedir "$SCRATCH" --batch --passphrase 'password' --quick-generate-key dsa-passphrase dsa1024 sign


gpg --homedir "$SCRATCH" --generate-key --batch << EOF
     Key-Type: DSA
     Key-Length: 1024
     Subkey-Type: ELG-E
     Subkey-Length: 1024
     Name-Real: dsa-signer-elg-encrypter
     %no-protection
     %commit
EOF

gpg --homedir "$SCRATCH" --generate-key --batch << EOF
     Key-Type: DSA
     Key-Length: 1024
     Subkey-Type: ELG-E
     Subkey-Length: 1024
     Name-Real: dsa-signer-elg-encrypter-passphrase
     Passphrase: password
     %commit
EOF

gpg --homedir "$SCRATCH" --generate-key --batch << EOF
    Key-Type: eddsa
    Key-Curve: ed25519
    Subkey-Type: ecdh
    Subkey-Curve: cv25519
    Name-Real: ed25519-signer-cv25519-encrypter
    %no-protection
    %commit
EOF

gpg --homedir "$SCRATCH" --generate-key --batch << EOF
    Key-Type: eddsa
    Key-Curve: ed25519
    Subkey-Type: ecdh
    Subkey-Curve: cv25519
    Name-Real: ed25519-signer-cv25519-encrypter-passphrase
    Passphrase: password
    %commit
EOF


## all public keys
gpg --homedir "$SCRATCH" --export > many-public-keys.bin
gpg --homedir "$SCRATCH" --armor --export > many-public-keys.asc

## all secret keys
gpg --homedir "$SCRATCH" --export-secret-keys > many-secret-keys.bin
gpg --homedir "$SCRATCH" --armor --export-secret-keys > many-secret-keys.asc


## one public key
gpg --homedir "$SCRATCH" --export rsa-passphrase > one-public-key.bin
gpg --homedir "$SCRATCH" --armor --export rsa-passphrase > one-public-key.asc

## one secret key
gpg --homedir "$SCRATCH" --export-secret-keys rsa-passphrase > one-secret-key.bin
gpg --homedir "$SCRATCH" --armor --export-secret-keys rsa-passphrase > one-secret-key.asc


rm -rf "$SCRATCH"
