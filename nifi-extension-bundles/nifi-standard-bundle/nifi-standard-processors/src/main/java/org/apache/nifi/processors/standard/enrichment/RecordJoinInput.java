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

package org.apache.nifi.processors.standard.enrichment;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.RecordSchema;

public class RecordJoinInput {
    private final FlowFile flowFile;
    private final RecordReaderFactory recordReaderFactory;
    private final RecordSchema recordSchema;

    public RecordJoinInput(final FlowFile flowFile, final RecordReaderFactory recordReaderFactory, final RecordSchema recordSchema) {
        this.flowFile = flowFile;
        this.recordReaderFactory = recordReaderFactory;
        this.recordSchema = recordSchema;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public RecordReaderFactory getRecordReaderFactory() {
        return recordReaderFactory;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }
}
