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
package org.apache.nifi.provenance.journaling;

import java.util.UUID;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;

public class TestUtil {
    public static ProvenanceEventRecord generateEvent(final long id) {
        // Create prov event to add to the stream
        final ProvenanceEventRecord event = new StandardProvenanceEventRecord.Builder()
            .setEventType(ProvenanceEventType.CREATE)
            .setFlowFileUUID("00000000-0000-0000-0000-" + pad(String.valueOf(id), 12, '0'))
            .setComponentType("Unit Test")
            .setComponentId(UUID.randomUUID().toString())
            .setEventTime(System.currentTimeMillis())
            .setFlowFileEntryDate(System.currentTimeMillis() - 1000L)
            .setLineageStartDate(System.currentTimeMillis() - 2000L)
            .setCurrentContentClaim(null, null, null, null, 0L)
            .build();
        
        return event;
    }
    
    public static String pad(final String value, final int charCount, final char padding) {
        if ( value.length() >= charCount ) {
            return value;
        }
        
        final StringBuilder sb = new StringBuilder();
        for (int i=value.length(); i < charCount; i++) {
            sb.append(padding);
        }
        sb.append(value);
        
        return sb.toString();
    }
}
