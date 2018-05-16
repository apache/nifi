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

import groovy.json.JsonSlurper
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema


class GroovyXmlRecordReader implements RecordReader {

    def recordIterator
    def recordSchema

    GroovyXmlRecordReader(final String recordTag, final RecordSchema schema, final InputStream inputStream) {
        recordSchema = schema
        def xml = new XmlSlurper().parse(inputStream)
        // Change the XML fields to a MapRecord for each incoming record
        recordIterator = xml[recordTag].collect {r ->
            // Create a map of field names to values, using the field names from the schema as keys into the XML object
            def fields = recordSchema.fieldNames.inject([:]) {result, fieldName ->
                result[fieldName] = r[fieldName].toString()
                result
            }
            new MapRecord(recordSchema, fields)
        }.iterator()
    }

    Record nextRecord(boolean coerceTypes, boolean dropUnknown) throws IOException, MalformedRecordException {
        return recordIterator?.hasNext() ? recordIterator.next() : null
    }

    RecordSchema getSchema() throws MalformedRecordException {
        return recordSchema
    }

    void close() throws IOException {
    }
}

class GroovyXmlRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    // Will be set by the ScriptedRecordReaderFactory
    ConfigurationContext configurationContext

    RecordReader createRecordReader(Map<String, String> variables, InputStream inputStream, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        // Expecting 'schema.text' to have be an JSON array full of objects whose keys are the field name and the value maps to a RecordFieldType
        def schemaText = configurationContext.properties.find {p -> p.key.dynamic && p.key.name == 'schema.text'}?.getValue()
        if (!schemaText) return null
        def jsonSchema = new JsonSlurper().parseText(schemaText)
        def recordSchema = new SimpleRecordSchema(jsonSchema.collect {field ->
            def entry = field.entrySet()[0]
            new RecordField(entry.key, RecordFieldType.of(entry.value).dataType)
        } as List<RecordField>)
        return new GroovyXmlRecordReader(variables.get('record.tag'), recordSchema, inputStream)
    }

}

// Create an instance of RecordReaderFactory called "writer", this is the entry point for ScriptedReader
reader = new GroovyXmlRecordReaderFactory()
