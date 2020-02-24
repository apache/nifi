The `gen-keys.sh` script in this directory recreates the PGP keys
for the various tests.  Run the script to re-create the key files.

The script will prompt for passwords during GPG secret key export,
and there isn't an easy way around that.  For every key that ends
with "-password", just type "password", no quotes, when prompted.

