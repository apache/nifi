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

import groovy.sql.Sql

def flowFile = session.get()
if(!flowFile)return

//write content of the flow file into database blob
flowFile.read{ rawIn->
	def parms = [
		p_id   : flowFile.ID as Long,
		p_data : Sql.BLOB( rawIn ),
	]
	assert 1==SQL.mydb.executeUpdate(parms, "update mytable set data = :p_data where id = :p_id")
}
//transfer original to output
REL_SUCCESS << flowFile
