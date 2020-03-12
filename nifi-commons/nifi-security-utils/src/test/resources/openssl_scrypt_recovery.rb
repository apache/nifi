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

# Run `$ gem install scrypt`
require 'scrypt'

def bin_to_hex(s)
  s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
end

def hex_to_bin(s)
  s.scan(/../).map { |x| x.hex.chr }.join
end

# Extracts r and p directly, converts N from the hex encoding of the exponent to the hex representation of the calculated total iterations cost (e -> 14 -> 2^14 -> 16384 -> 0x4000)
def format_ruby_cost(java_cost)
  n_exp = java_cost[0].to_i(16)
  r = java_cost[1..2].to_i
  p = java_cost[3..-1].to_i

  n_dec = 2 ** n_exp
  [n_dec.to_s(16), r, p].join("$") + "$"
end

# Flowfile content from EncryptContent w/ Scrypt (default cost params) + password: thisIsABadPassword
ciphertext = hex_to_bin("24733024 65303830 31247255 666B594B 784A356E 63557A62 744F7844 59524D51 4E694669 53414C54 E912E385 39EF934D 393CB0F8 B912F32E 4E694669 49562E35 748C7D5E 2762CE7C ACDC1B22 F2AFFCF0 1EB69F73 CC1ECFD9 04F034D7 8A91EFCB 08E23782 0B9F9301 C87B3ABB 65C86FD2 911045CE 7038D66A E0DE56BC 35F84937 D7D5131A 4FD01D0F 10944A43 F57B".gsub("\s", ""))

salt = ciphertext[0..31]
salt_delimiter = ciphertext[32..39]

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.decrypt
iv = ciphertext[40..55]
cipher.iv = iv
iv_delimiter = ciphertext[56..61]

cipher_bytes = ciphertext[62..-1]
puts "Cipher bytes: #{bin_to_hex(cipher_bytes)} #{cipher_bytes.length}"

password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
key_len = cipher.key_len

puts ""

# 10$8$1 -> 0d16, 0d8, 0d1 -> 16 = 2^4
# 4000$8$1 -> 0d16384, 0d8, 0d1 -> 16384 = 2^14
#
# Convert e0801 -> hex(2^0x0e)$8$1
#
# If N != hex_encoded(decimal power of 2), C code returns err -1
#
java_cost = salt[4..8]
puts "Java cost: #{java_cost}"
ruby_cost = format_ruby_cost(java_cost)
puts "Ruby cost: #{ruby_cost}"

# ruby_cost = "4000$8$1"
raw_salt_bytes = Base64.decode64(salt[10..31])
scrypt_ruby_salt = ruby_cost << bin_to_hex(raw_salt_bytes)
puts "Ruby salt: #{scrypt_ruby_salt}"

hash = SCrypt::Engine.hash_secret(password, scrypt_ruby_salt, key_len)
puts "Hash: #{hash}"
full_salt = hash[0..28]
puts "Full Salt: #{full_salt} #{full_salt.length}"

key = hex_to_bin(hash[-(key_len*2)..-1])
puts "Salt: #{bin_to_hex(raw_salt_bytes)} #{raw_salt_bytes.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now decrypt the data:

plaintext = cipher.update cipher_bytes
plaintext << cipher.final
puts "Plaintext length: #{plaintext.length}"
puts "Plaintext: #{plaintext}"