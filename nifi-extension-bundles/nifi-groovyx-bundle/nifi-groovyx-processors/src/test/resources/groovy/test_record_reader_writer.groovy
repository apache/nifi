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


import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.serialization.RecordSetWriterFactory


//just check that it's possible to access controller services
def ff = session.get()
if (!ff) return
def readerFactory = RecordReader.myreader
assert readerFactory instanceof RecordReaderFactory
def writerFactory = RecordWriter.mywriter
assert writerFactory instanceof RecordSetWriterFactory

session.write(ff, { inStream, outStream ->
    def variables = new HashMap<String, String>(ff.attributes)
    def recordReader = readerFactory.createRecordReader(variables, inStream, -1L, log)
    def recordWriter = writerFactory.createWriter(log, recordReader.schema, outStream, variables)
    def record = null
    recordWriter.beginRecordSet()
    while (record = recordReader.nextRecord()) {
        recordWriter.write(record)
    }
    recordWriter.finishRecordSet()
} as StreamCallback)

REL_SUCCESS << ff
