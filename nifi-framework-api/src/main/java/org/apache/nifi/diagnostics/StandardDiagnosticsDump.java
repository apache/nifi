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
package org.apache.nifi.diagnostics;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.List;

public class StandardDiagnosticsDump implements DiagnosticsDump {
    private final List<DiagnosticsDumpElement> dumpElements;
    private final long timestamp;

    public StandardDiagnosticsDump(final List<DiagnosticsDumpElement> dumpElements, final long timestamp) {
        this.dumpElements = dumpElements;
        this.timestamp = timestamp;
    }

    public void writeTo(final OutputStream out) throws IOException {
        final Writer outputStreamWriter = new OutputStreamWriter(out);
        final BufferedWriter writer = new BufferedWriter(outputStreamWriter);

        writer.write("Diagnostic Dump taken at ");
        final Date date = new Date(timestamp);
        writer.write(date.toString());
        writer.write("\n\n");

        for (final DiagnosticsDumpElement element : dumpElements) {
            writeHeader(writer, element.getName());

            for (final String line : element.getDetails()) {
                writer.write(line);
                writer.write("\n");
            }

            writer.write("\n\n");
        }

        writer.flush();
    }

    private void writeHeader(final BufferedWriter writer, final String header) throws IOException {
        writer.write(header);
        writer.write("\n");
        for (int i=0; i < header.length(); i++) {
            writer.write("-");
        }
        writer.write("\n");
    }
}
