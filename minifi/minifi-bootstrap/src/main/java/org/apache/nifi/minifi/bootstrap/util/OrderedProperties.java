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

package org.apache.nifi.minifi.bootstrap.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class OrderedProperties extends Properties {
    private final Map<String, String> textBeforeMap = new HashMap<>();
    private final LinkedHashSet<Object> orderedKeys = new LinkedHashSet<>();

    @Override
    public synchronized Object put(Object key, Object value) {
        orderedKeys.add(key);
        return super.put(key, value);
    }

    @Override
    public synchronized Enumeration<Object> keys() {
        return Collections.enumeration(orderedKeys.stream().filter(this::containsKey).collect(Collectors.toList()));
    }

    @Override
    public synchronized Set<Map.Entry<Object, Object>> entrySet() {
        return orderedKeys.stream()
                .map(k -> new AbstractMap.SimpleImmutableEntry<>(k, this.get(k)))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public synchronized Object setProperty(String key, String value, String textBefore) {
        textBeforeMap.put(key, textBefore);
        return setProperty(key, value);
    }

    @Override
    public synchronized void store(OutputStream out, String comments) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        super.store(byteArrayOutputStream, comments);

        try(BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), "8859_1"));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, "8859_1"))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                int equalsIndex = line.indexOf('=');
                if (equalsIndex != -1) {
                    String textBefore = textBeforeMap.get(line.substring(0, equalsIndex));
                    if (textBefore != null) {
                        bufferedWriter.write(textBefore);
                        bufferedWriter.newLine();
                    }
                }
                bufferedWriter.write(line);
                bufferedWriter.newLine();
            }
        }
    }
}
