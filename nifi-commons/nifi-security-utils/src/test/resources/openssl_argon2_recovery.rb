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

# Run `$ gem install argon2`
require 'argon2'

def bin_to_hex(s)
  s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
end

def hex_to_bin(s)
  s.scan(/../).map { |x| x.hex.chr }.join
end

# Extracts t and m from m=4096,t=3,p=1 as p is not exposed in the ruby gem
def format_ruby_cost(java_cost)
  elements = java_cost.gsub(/\$/, '').split(",").collect { |pair| pair.split("=") }.to_h
  m = Math.log2(elements["m"].to_i)
  t = elements["t"].to_i

  {:t => t, :m => m}
end

# Flowfile content from EncryptContent w/ Argon2 (default cost params) + password: thisIsABadPassword
ciphertext = hex_to_bin("246172676F6E32696424763D3139246D3D343039362C743D332C703D31247264562B4D39613577696C7A796651334136314C36514E69466953414C541B0D6B57611E7A5406A7F63664BD54514E69466949563A90FA629D0BA1A20545F33788C141469A754D717F8F81E22F8234AD0EC13EA139210D4652D2F448E627A9243C2A1C0D0AFF733793AF3A47A357516BDD940EEA6DB858669E961B234EFA3FE1614715C9".gsub("\s", ""))

salt = ciphertext[0..51]
salt_delimiter = ciphertext[52..59]

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.decrypt
iv = ciphertext[60..75]
cipher.iv = iv
iv_delimiter = ciphertext[76..81]

cipher_bytes = ciphertext[82..-1]
puts "Cipher bytes: #{bin_to_hex(cipher_bytes)} #{cipher_bytes.length}"

password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
key_len = cipher.key_len

puts ""

java_cost = salt[15..29]
puts "Java cost: #{java_cost}"
ruby_cost = format_ruby_cost(java_cost)
puts "Ruby cost: t=#{ruby_cost[:t]}, m=#{ruby_cost[:m]}"

raw_salt_bytes = Base64.decode64(salt[30..51])
# raw_salt_bytes = Base64.decode64("rdV+M9a5wilzyfQ3A61L6Q")
puts "Ruby salt: #{bin_to_hex(raw_salt_bytes)}"

# If the gem allowed setting an output length, the Password class would allow providing salt directly
# through constructor map, but output length is fixed at 32 bytes
hasher = Argon2::Password.new(:t_cost => ruby_cost[:t], :m_cost => ruby_cost[:m], :salt_do_not_supply => raw_salt_bytes)
full_hash = hasher.create(password)
puts "Full Hash: #{full_hash}"

hash_hex = Argon2::Engine.hash_argon2id(password, raw_salt_bytes, ruby_cost[:t], ruby_cost[:m], 16)
hash_bytes = [hash_hex].pack('H*')
puts "Hash (hex): #{hash_hex}"
hash_base64 = Base64.encode64(hash_bytes)
puts "Hash B64: #{hash_base64}"

puts "Full Salt: #{salt} #{salt.length}"

key = hex_to_bin(hash_hex)
puts "Salt: #{bin_to_hex(raw_salt_bytes)} #{raw_salt_bytes.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now decrypt the data:

plaintext = cipher.update cipher_bytes
plaintext << cipher.final
puts "Plaintext length: #{plaintext.length}"
puts "Plaintext: #{plaintext}"