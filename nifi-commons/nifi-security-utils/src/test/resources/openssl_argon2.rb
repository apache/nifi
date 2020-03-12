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

plaintext = "This is a plaintext message."
puts "Plaintext: #{plaintext}"

cipher = OpenSSL::Cipher.new 'AES-256-CBC'
cipher.encrypt
iv = cipher.random_iv
# iv = hex_to_bin("00" * 16)


password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
# The Argon2 gem does not support variable-length hashes, it uses a fixed 32 byte output
hashLength = 32
puts "Hash length: #{hashLength}"
memory = 8
puts "Memory: #{memory}"
parallelism = 1
puts "Parallelism: #{parallelism}"
iterations = 3
puts "Iterations: #{iterations}"

puts ""

hasher = Argon2::Password.new(:t_cost => iterations, :m_cost => memory, :p_parallelism => parallelism)
full_hash = hasher.create(password)
b64Salt = full_hash[29..50]
# Decode b64Salt to binary
salt = Base64.decode64(b64Salt)
# The hash is Blake2b of the generated result, with a static hash length of 32B for the gem.
# We use the first 16B of the 32B output because this is a 128 bit cipher.
key = Base64.decode64(full_hash[52..-1])[0..hashLength-1]

puts "Full Hash: #{full_hash} #{full_hash.length}"
puts "     Salt: #{bin_to_hex(salt)} #{salt.length}"
puts "       IV: #{bin_to_hex(iv)} #{iv.length}"
puts "      Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now encrypt the data:

encrypted = cipher.update plaintext
encrypted << cipher.final
puts "Cipher text length: #{encrypted.length}"
puts "Cipher text: #{bin_to_hex(encrypted)}"
