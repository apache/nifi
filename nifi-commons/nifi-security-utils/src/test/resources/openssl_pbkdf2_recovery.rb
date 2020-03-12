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

def bin_to_hex(s)
  s.each_byte.map { |b| b.to_s(16).rjust(2, '0') }.join
end

def hex_to_bin(s)
  s.scan(/../).map { |x| x.hex.chr }.join
end

# Flowfile content from EncryptContent w/ PBKDF2 (default cost params) + password: thisIsABadPassword
ciphertext = hex_to_bin("A38A9085A702F5FA28C0485C52AF92E84E69466953414C549CB6114F13990FC197D53E05C73CAC9C4E694669495650F025A192EB62A324CA1CEBACF6D563657BE5DFF0BD601801DDA85FBBB350568D0BBA9A688ADE02A00F59C63527045E65D61D0B21C6451AF55B97C4420911C16511EFD59A714C1CABFCD80AF7FF81EA")

salt = ciphertext[0..15]
salt_delimiter = ciphertext[16..23]

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.decrypt
iv = ciphertext[24..39]
cipher.iv = iv
iv_delimiter = ciphertext[40..45]

cipher_bytes = ciphertext[46..-1]

password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
iterations = 160_000
puts "Iterations: #{iterations}"
key_len = cipher.key_len
digest = OpenSSL::Digest::SHA512.new

puts ""

key = OpenSSL::PKCS5.pbkdf2_hmac(password, salt, iterations, key_len, digest)
puts "Salt: #{bin_to_hex(salt)} #{salt.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now decrypt the data:

plaintext = cipher.update cipher_bytes
plaintext << cipher.final
puts "Plaintext length: #{plaintext.length}"
puts "Plaintext: #{plaintext}"