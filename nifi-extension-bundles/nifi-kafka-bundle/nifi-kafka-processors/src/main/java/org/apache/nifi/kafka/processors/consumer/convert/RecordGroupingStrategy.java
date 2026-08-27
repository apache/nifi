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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.Map;

/**
 * Groups converted Kafka records into FlowFiles according to a Schema Conflict Resolution strategy.
 * <p>
 * Implementations are stateful: they accumulate open writers or buffered records until
 * {@link #finishAllGroups(ProcessSession)} is called. A new instance must be created for each
 * {@code onTrigger} invocation and must not be reused across calls, so that leftover group state
 * cannot survive an exception or a failed session.
 */
public interface RecordGroupingStrategy {

    void addRecord(
            ProcessSession session,
            ByteRecord consumerRecord,
            Record recordToWrite,
            RecordSchema writeSchema,
            Map<String, String> attributes,
            Map<String, String> groupingAttributes) throws IOException, SchemaNotFoundException;

    void finishAllGroups(ProcessSession session);
}
