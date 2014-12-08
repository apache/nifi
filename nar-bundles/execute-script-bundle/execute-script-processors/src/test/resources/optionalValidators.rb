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
  field_reader :FAIL_RELATIONSHIP, :SUCCESS_RELATIONSHIP, :logger
  
  def getPropertyDescriptors
    logger.debug("Defining descriptors");
    i = StandardValidators::INTEGER_VALIDATOR
    u = StandardValidators::URL_VALIDATOR
    s = StandardValidators::NON_EMPTY_VALIDATOR
    intPropDesc = PropertyDescriptor::Builder.new().name("int").required(true).addValidator(i).build()
    urlPropDesc = PropertyDescriptor::Builder.new().name("url").required(true).addValidator(u).build()
    nonEmptyPropDesc = PropertyDescriptor::Builder.new().name("nonEmpty").addValidator(s).build()
    return [intPropDesc, urlPropDesc, nonEmptyPropDesc]
  end
  
  def route( input )
    logger.debug("Routing input");
    input.to_io.each_line do |line|
      return FAIL_RELATIONSHIP if line.match /^sed/i
    end

    return SUCCESS_RELATIONSHIP
	end
end
$logger.debug("Creating SimpleJRubyReader with props" + @properties.to_s)
SimpleJRubyReader.new