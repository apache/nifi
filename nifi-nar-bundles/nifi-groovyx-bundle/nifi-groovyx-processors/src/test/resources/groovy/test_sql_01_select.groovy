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

/*
the original script taken from this article
http://funnifi.blogspot.com/2016/04/sql-in-nifi-with-executescript.html
and refactored and simplified for ExecuteGroovyScript 
*/ 

def flowFile = session.create()

flowFile.write("UTF-8"){wout -> 
  //assume SQL.mydb property is linked to desired database connection pool
  SQL.mydb.eachRow('select * from mytable'){ row->
    wout << row.name << '\n'
  }
}
//set filename attribute
flowFile.'filename' = 'test.txt'
REL_SUCCESS << flowFile
