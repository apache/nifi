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
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedParameter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class VersionedComponentCollectionSerializer extends StdSerializer<Collection<?>> {

    public VersionedComponentCollectionSerializer() {
        super((Class<Collection<?>>) (Class<?>) Collection.class);
    }

    @Override
    public void serialize(final Collection<?> value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
        // shortcut for empty:
        if (value.isEmpty()) {
            gen.writeStartArray();
            gen.writeEndArray();
            return;
        }

        // only sort if these are VersionedComponent instances
        List<?> list = new ArrayList<>(value);
        if (list.get(0) instanceof VersionedComponent) {
            list.sort(Comparator.comparing(c -> ((VersionedComponent) c).getIdentifier()));
        } else if (list.get(0) instanceof VersionedParameter) {
            list.sort(Comparator.comparing(c -> ((VersionedParameter) c).getName()));
        }

        // now write the (possibly sorted) list
        gen.writeStartArray();
        for (Object e : list) {
            serializers.defaultSerializeValue(e, gen);
        }
        gen.writeEndArray();
    }
}
