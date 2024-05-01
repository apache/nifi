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
package org.apache.nifi.kafka.processors.producer.key;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordFieldConverter;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.util.Map;

public class MessageKeyFactory implements KeyFactory {
    private final FlowFile flowFile;
    private final String messageKeyField;
    private final RecordSetWriterFactory keyWriterFactory;
    private final ComponentLog logger;

    public MessageKeyFactory(
            FlowFile flowFile,
            String messageKeyField,
            RecordSetWriterFactory keyWriterFactory,
            ComponentLog logger) {
        this.flowFile = flowFile;
        this.messageKeyField = messageKeyField;
        this.keyWriterFactory = keyWriterFactory;
        this.logger = logger;
    }

    @Override
    public byte[] getKey(Map<String, String> attributes, Record record) throws IOException {
        final RecordFieldConverter converter = new RecordFieldConverter(record, flowFile, logger);
        return converter.toBytes(messageKeyField, keyWriterFactory);
    }
}
