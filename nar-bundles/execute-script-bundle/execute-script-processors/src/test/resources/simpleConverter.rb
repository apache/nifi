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
java_import 'org.apache.nifi.scripting.OutputStreamHandler'
class SimpleConverter < ConverterScript
  field_reader :FAIL_RELATIONSHIP, :SUCCESS_RELATIONSHIP, :logger, :attributes
  
  def convert(input)
    in_io = input.to_io
    createFlowFile("firstLine", FAIL_RELATIONSHIP, OutputStreamHandler.impl do |method, out|
	    out_io = out.to_io
		out_io << in_io.readline.to_java_bytes
	    out_io.close
 	    logger.debug("Wrote data to failure...this message logged with logger from super class")
      end)
	  
    createFlowFile("otherLines", SUCCESS_RELATIONSHIP, OutputStreamHandler.impl do |method, out|
		out_io = out.to_io
		in_io.each_line { |line|
		  out_io << line
		}
		out_io.close
		logger.debug("Wrote data to success...this message logged with logger from super class")
      end)
	in_io.close
  end
   
end

$logger.debug("Creating SimpleConverter...this message logged with logger from shared variables")
SimpleConverter.new