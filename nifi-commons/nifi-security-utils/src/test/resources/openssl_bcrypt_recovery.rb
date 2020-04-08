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
ciphertext = hex_to_bin("243261243132246D6D374D694B6A7658565943756A56556C4B524B69754E69466953414C547CBF02AB79CFC352E02BFC9114E3D82C4E6946694956C349433E5DB906CA59FB9578265668EB8BE542701394316092FD1598C2A72E54283EAD9FE2D68F8F55D00B571E5AD94557DB4126FB5CD39ABEBF93DAFBF6773A29BF71603DA09AC9B313A6A75ABD592210")

salt = ciphertext[0..28]
salt_delimiter = ciphertext[29..36]

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.decrypt
iv = ciphertext[37..52]
cipher.iv = iv
iv_delimiter = ciphertext[53..59]

cipher_bytes = ciphertext[59..-2]
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


key = (digest.digest hash_radix64)[0..key_len - 1]
puts "Salt: #{bin_to_hex(raw_salt_bytes)} #{raw_salt_bytes.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now decrypt the data:

plaintext = cipher.update cipher_bytes
plaintext << cipher.final
puts "Plaintext length: #{plaintext.length}"
puts "Plaintext: #{plaintext}"