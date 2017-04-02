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

def flowFile = session.create()
//read blob into flowFile content
flowFile.write{out->
	//get id from property with name ID
	def row = SQL.mydb.firstRow("select data from mytable where id = ${ ID.value as Long }")
	assert row : "row with id=`${ID}` not found"
	//write blob stream to flowFile output stream
	out << row.data.getBinaryStream()
}

//transfer new file to output
REL_SUCCESS << flowFile
