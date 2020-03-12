#!/usr/bin/env ruby

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'openssl'
require 'base64'

# Run `$ gem install bcrypt` >= 2.1.4
require 'bcrypt'

def bin_to_hex(s)
  s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
end

def hex_to_bin(s)
  s.scan(/../).map { |x| x.hex.chr }.join
end

# Flowfile content from EncryptContent w/ Bcrypt (default cost params) + password: thisIsABadPassword
ciphertext = hex_to_bin("24326124313224674449584f316a54774a774130386d714a76396545754e69466953414c54ef67ce4babc8016c055828a3065806034e69466949567231eac94e4c841508a82d2a5c98e3501b096774e98fc0039db89250f52fbb01")

salt = ciphertext[0..28]
salt_delimiter = ciphertext[29..36]

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.decrypt
iv = ciphertext[37..52]
cipher.iv = iv
iv_delimiter = ciphertext[53..59]

cipher_bytes = ciphertext[59..-1]
puts "Cipher bytes: #{bin_to_hex(cipher_bytes)} #{cipher_bytes.length}"

password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
key_len = cipher.key_len
digest = OpenSSL::Digest::SHA512.new

puts ""

hash = BCrypt::Engine.hash_secret(password, salt)
puts "Hash: #{hash}"
full_salt = hash[0..28]
puts "Full Salt: #{full_salt} #{full_salt.length}"
# hash_radix64 = hash[29..-1]
hash_radix64 = hash
puts "Hash R64: #{hash_radix64} #{hash_radix64.length}"

# Convert from the Bcrypt Radix64 to binary
# Use BcryptSecureHasherTest.groovy which exercises conversion methods in BcryptSecureHasher
# hash_base64 = "K4sgDQwNApbod+8csAnQQo3yj9LYpXA"
# Expected byte value in hex: 2b8b200d0c0d0296e877ef1cb009d0428df28fd2d8a570
# hash_bytes = Base64.decode64 hash_base64
# puts "Hash bytes: #{bin_to_hex(hash_bytes)}"

# key = (digest.digest hash_bytes)[0..key_len - 1]

raw_salt_radix64 = salt[7..-1]
puts "Raw salt R64: #{raw_salt_radix64}"
# Convert from the Bcrypt Radix64 to binary
# Use BcryptSecureHasherTest.groovy which exercises conversion methods in BcryptSecureHasher
# Expected byte value in hex: a28f4e90c971657684c255d69cc4cc93
# Expected Base64 value: oo9OkMlxZXaEwlXWnMTMkw
raw_salt_base64 = "oo9OkMlxZXaEwlXWnMTMkw"
puts "Raw salt B64: #{raw_salt_base64}"
raw_salt_bytes = Base64.decode64(raw_salt_base64)
puts "Raw salt: #{bin_to_hex(raw_salt_bytes)}"

# Only use the hash part of the full output as input to the key stretching digest
key = (digest.digest hash_radix64[-31..-1])[0..key_len - 1]
puts "Salt: #{bin_to_hex(raw_salt_bytes)} #{raw_salt_bytes.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now decrypt the data:

plaintext = cipher.update cipher_bytes
plaintext << cipher.final
puts "Plaintext length: #{plaintext.length}"
puts "Plaintext: #{plaintext}"