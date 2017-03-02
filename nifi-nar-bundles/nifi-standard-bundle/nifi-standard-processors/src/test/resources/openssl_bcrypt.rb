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

plaintext = "This is a plaintext message."
puts "Plaintext: #{plaintext}"

cipher = OpenSSL::Cipher.new 'AES-128-CBC'
cipher.encrypt
iv = cipher.random_iv

password = 'thisIsABadPassword'
puts "Password: #{password} #{password.length}"
work_factor = 10
puts "Work factor: #{work_factor}"
key_len = cipher.key_len
digest = OpenSSL::Digest::SHA512.new

puts ""

hash = BCrypt::Password.create(password, :cost => work_factor)
puts "Hash: #{hash}"
full_salt = hash.salt
puts "Full Salt: #{full_salt} #{full_salt.length}"

key = (digest.digest hash)[0..key_len - 1]
salt = Base64.decode64(hash.salt[7..-1])

puts "Salt: #{bin_to_hex(salt)} #{salt.length}"
puts "  IV: #{bin_to_hex(iv)} #{iv.length}"
puts " Key: #{bin_to_hex(key)} #{key.length}"
cipher.key = key

# Now encrypt the data:

encrypted = cipher.update plaintext
encrypted << cipher.final
puts "Cipher text length: #{encrypted.length}"
puts "Cipher text: #{bin_to_hex(encrypted)}"