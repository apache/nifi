/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
def flowFile = session.get()
if(!flowFile)return
def selectedColumns = ''
flowFile.write{inputStream, outputStream->
    String[] header = null
    
    outputStream.withWriter("UTF-8"){outputWriter->
		inputStream.eachLine("UTF-8"){line->
			if(header==null){
				header = line.split(',')
				selectedColumns = "${header[2]},${header[3]}"
			}else{
				String[] cols = line.split(',')
				outputWriter.write("${cols[2].capitalize()} ${cols[3].capitalize()}\n")
			}
		}
	}
}
flowFile."selected.columns" = selectedColumns
flowFile."filename" = "split_cols.txt"
REL_SUCCESS << flowFile

