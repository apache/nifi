/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.registry.flow.git.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SortedStringSetSerializer extends StdSerializer<Set<String>> {

    public SortedStringSetSerializer() {
        super((Class<Set<String>>) (Class<?>) Set.class);
    }

    @Override
    public void serialize(final Set<String> value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
        final List<String> sorted = new ArrayList<>(value);
        Collections.sort(sorted);
        gen.writeStartArray();
        for (final String str : sorted) {
            gen.writeString(str);
        }
        gen.writeEndArray();
    }
}
