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

package org.apache.nifi.avro;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"avro", "parse", "record", "row", "reader", "delimited", "comma", "separated", "values"})
@CapabilityDescription("Parses Avro data and returns each Avro record as an separate Record object. The Avro data must contain "
    + "the schema itself.")
public class AvroReader extends AbstractControllerService implements RowRecordReaderFactory {

    @Override
    public RecordReader createRecordReader(final FlowFile flowFile, final InputStream in, final ComponentLog logger) throws MalformedRecordException, IOException {
        return new AvroRecordReader(in);
    }

    @Override
    public RecordSchema getSchema(final FlowFile flowFile) throws MalformedRecordException, IOException {
        // TODO: Need to support retrieving schema from registry instead of requiring that it be in the Avro file.
        return null;
    }

}
