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

package org.apache.nifi.provenance.util;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;

public class DumpEventFile {

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("java " + DumpEventFile.class.getName() + " <Event File to Dump>");
        System.out.println();
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            printUsage();
            return;
        }

        final File file = new File(args[0]);
        if (!file.exists()) {
            System.out.println("Cannot find file " + file.getAbsolutePath());
            return;
        }

        try (final RecordReader reader = RecordReaders.newRecordReader(file, Collections.emptyList(), 65535)) {
            StandardProvenanceEventRecord event;
            int index = 0;
            while ((event = reader.nextRecord()) != null) {
                final long byteOffset = reader.getBytesConsumed();
                final String string = stringify(event, index++, byteOffset);
                System.out.println(string);
            }
        }
    }

    private static String stringify(final ProvenanceEventRecord event, final int index, final long byteOffset) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Event Index in File = ").append(index).append(", Byte Offset = ").append(byteOffset);
        sb.append("\n\t").append("Event ID = ").append(event.getEventId());
        sb.append("\n\t").append("Event Type = ").append(event.getEventType());
        sb.append("\n\t").append("Event Time = ").append(new Date(event.getEventTime()));
        sb.append("\n\t").append("Event UUID = ").append(event.getFlowFileUuid());
        sb.append("\n\t").append("Component ID = ").append(event.getComponentId());
        sb.append("\n\t").append("Event ID = ").append(event.getComponentType());
        sb.append("\n\t").append("Transit URI = ").append(event.getTransitUri());
        sb.append("\n\t").append("Parent IDs = ").append(event.getParentUuids());
        sb.append("\n\t").append("Child IDs = ").append(event.getChildUuids());
        sb.append("\n\t").append("Previous Attributes = ").append(event.getPreviousAttributes());
        sb.append("\n\t").append("Updated Attributes = ").append(event.getUpdatedAttributes());

        return sb.toString();
    }
}
