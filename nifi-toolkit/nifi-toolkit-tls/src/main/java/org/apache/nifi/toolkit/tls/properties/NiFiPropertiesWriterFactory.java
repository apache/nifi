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

import org.apache.nifi.toolkit.tls.TlsToolkitMain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Factory for creating NifiPropertiesWriters so that the lines only have to be read once
 */
public class NiFiPropertiesWriterFactory {
    private final List<String> lines;

    public NiFiPropertiesWriterFactory() throws IOException {
        this(TlsToolkitMain.class.getClassLoader().getResourceAsStream("conf/nifi.properties"));
    }

    public NiFiPropertiesWriterFactory(InputStream inputStream) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                lines.add(line.trim());
            }
        }
        this.lines = Collections.unmodifiableList(lines);
    }

    /**
     * Returns a NifiPropertiesWriter with based on the read nifi.properties
     *
     * @return a NifiPropertiesWriter with based on the read nifi.properties
     */
    public NiFiPropertiesWriter create() {
        return new NiFiPropertiesWriter(lines);
    }
}
