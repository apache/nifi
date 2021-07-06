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
package org.apache.nifi.processors.script;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Helper class contains all the information necessary to prepare an outgoing flow file.
 */
final class RecordBatchingProcessorFlowFileBuilder {
    private final ProcessSession session;
    private final FlowFile incomingFlowFile;
    private final FlowFile outgoingFlowFile;
    private final OutputStream out;
    private final RecordSetWriter writer;
    private final List<Map<String, String>> attributes = new LinkedList<>();

    private int recordCount = 0;

    RecordBatchingProcessorFlowFileBuilder(
            final FlowFile incomingFlowFile,
            final ProcessSession session,
            final BiFunction<FlowFile, OutputStream, RecordSetWriter> recordSetWriterSupplier
    ) throws IOException {
        this.session = session;
        this.incomingFlowFile = incomingFlowFile;
        this.outgoingFlowFile = session.create(incomingFlowFile);
        this.out = session.write(outgoingFlowFile);
        this.writer = recordSetWriterSupplier.apply(outgoingFlowFile, out);
        this.writer.beginRecordSet();
    }

    int addRecord(final Record record) throws IOException {
        final WriteResult writeResult = writer.write(record);
        attributes.add(writeResult.getAttributes());
        recordCount += writeResult.getRecordCount();
        return recordCount;
    }

    private Map<String, String> getWriteAttributes() {
        final Map<String, String> result = new HashMap<>();
        final Set<String> attributeNames = attributes.stream().map(Map::keySet).flatMap(Set::stream).collect(Collectors.toSet());

        for (final String attributeName : attributeNames) {
            final Set<String> attributeValues = attributes.stream().map(a -> a.get(attributeName)).collect(Collectors.toSet());

            // Only adding values to the flow file attributes from writing if the value is the same for every written record
            if (attributeValues.size() == 1) {
                result.put(attributeName, attributeValues.iterator().next());
            }
        }

        return result;
    }

    FlowFile build() {
        final Map<String, String> attributesToAdd = new HashMap<>(incomingFlowFile.getAttributes());
        attributesToAdd.putAll(getWriteAttributes());
        attributesToAdd.put("mime.type", writer.getMimeType());
        attributesToAdd.put("record.count", String.valueOf(recordCount));

        try {
            writer.finishRecordSet();
            writer.close();
            out.close();
        } catch (final IOException e) {
            throw new ProcessException("Resources used for record writing might not be closed", e);
        }

        return session.putAllAttributes(outgoingFlowFile, attributesToAdd);
    }
}
