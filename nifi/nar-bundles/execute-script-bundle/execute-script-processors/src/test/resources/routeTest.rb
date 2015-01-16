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
class SimpleJRubyReader < ReaderScript

    @@a = Relationship::Builder.new().name("a").description("some good stuff").build()
    @@b = Relationship::Builder.new().name("b").description("some bad stuff").build()
    @@c = Relationship::Builder.new().name("c").description("some other stuff").build()
  
  def getRelationships
    return [@@a, @@b, @@c]
  end
  
  def getExceptionRoute
    @@c
  end
  
  def route( input )
    input.to_io.each_line do |line|
      return @@b if line.match /^bad/i
      raise "That's no good!" if line.match /^sed/i
    end

    @@a
	end
end

SimpleJRubyReader.new
