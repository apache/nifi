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
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

//read json from input file and insert into db each array element
//return input element as a content for each output file
//drop original file

//expecting for input:
/*
[
  {"field":"value", "field2":"value2", ...},
  ...
]
*/
def flowFile = session.get()
if(!flowFile)return

def outFiles = [] //list for new flow files
def rows = new JsonSlurper().parse( flowFile.read() )

rows.each{row->
	//at this point row is a map with keys corresponding to mytable column names.
	//build query:  insert into mytable(a,b,c,...) values(:a, :b, :c, ...)
	//and pass row-map as an argument to this query
	SQL.mydb.executeInsert(row, "insert into mytable( ${row.keySet().join(',')} ) values( :${row.keySet().join(', :')} )")
	//create new flowfile based on original without copying content, 
	//write new content and add into outFiles list
	outFiles << flowFile.clone(false).write( "UTF-8", JsonOutput.toJson(row) )
}

//just easier to assert sql here
assert 2+rows.size() == SQL.mydb.firstRow("select count(*) cnt from mytable").cnt

flowFile.remove()
//transfer all new files to success relationship
REL_SUCCESS << outFiles
