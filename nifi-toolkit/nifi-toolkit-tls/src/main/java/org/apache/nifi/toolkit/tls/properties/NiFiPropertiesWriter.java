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

package org.apache.nifi.toolkit.tls.properties;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class capable of writing out updated NiFi properties.  It keeps a list of original lines and a map of updates to apply.
 *
 * It first writes all the original properties (with updated values if they exist) and then adds any new properties at the end.
 */
public class NiFiPropertiesWriter {
    private final List<String> lines;
    private final Map<String, String> updatedValues;

    public NiFiPropertiesWriter(List<String> lines) {
        this.lines = new ArrayList<>(lines);
        this.updatedValues = new HashMap<>();
    }

    /**
     * Sets a property value
     *
     * @param key the property key
     * @param value the property value
     */
    public void setPropertyValue(String key, String value) {
        updatedValues.put(key, value);
    }

    /**
     * Write an updated nifi.properties to the given OutputStream
     *
     * @param outputStream the output stream
     * @throws IOException if there is an IO error
     */
    public void writeNiFiProperties(OutputStream outputStream) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
            Map<String, String> remainingValues = new HashMap<>(updatedValues);
            Set<String> keysSeen = new HashSet<>();
            for (String line : lines) {
                String key = line.split("=")[0].trim();
                boolean outputLine = true;
                if (!key.isEmpty() && !key.startsWith("#")) {
                    if (!keysSeen.add(key)) {
                        throw new IOException("Found key more than once in nifi.properties: " + key);
                    }
                    String value = remainingValues.remove(key);
                    if (value != null) {
                        writer.write(key);
                        writer.write("=");
                        writer.write(value);
                        outputLine = false;
                    }
                }
                if (outputLine) {
                    writer.write(line);
                }
                writer.newLine();
            }
            for (Map.Entry<String, String> keyValueEntry : remainingValues.entrySet()) {
                writer.write(keyValueEntry.getKey());
                writer.write("=");
                writer.write(keyValueEntry.getValue());
                writer.newLine();
            }
        }
    }
}
