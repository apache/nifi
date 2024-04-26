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
package org.apache.nifi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;

/**
 * Override the GenericDatumReader to provide a much more efficient implementation of #readString. The one that is provided by
 * GenericDatumReader performs very poorly in some cases because it uses an IdentityHashMap with the key being the Schema so that
 * it can stash away the "stringClass" but that performs far worse than just calling JsonNode#getProp. I.e., {@link #readString(Object, Schema, Decoder)}
 * in GenericDatumReader calls #getStringClass, which uses an IdentityHashMap to cache results in order to avoid calling {@link #findStringClass(Schema)}.
 * However, {@link #findStringClass(Schema)} is much more efficient than using an IdentityHashMap anyway. Additionally, the performance of {@link #findStringClass(Schema)}}
 * can be improved slightly and made more readable.
 */
public class NonCachingDatumReader<T> extends GenericDatumReader<T> {
    public NonCachingDatumReader() {
        super();
    }

    public NonCachingDatumReader(final Schema schema) {
        super(schema);
    }

    @Override
    protected Object readString(final Object old, final Schema expected, final Decoder in) throws IOException {
        final Class<?> stringClass = findStringClass(expected);
        if (stringClass == String.class) {
            return in.readString();
        }

        if (stringClass == CharSequence.class) {
            return readString(old, in);
        }

        return newInstanceFromString(stringClass, in.readString());
    }

    protected Class findStringClass(Schema schema) {
        final String name = schema.getProp(GenericData.STRING_PROP);
        if ("String".equals(name)) {
            return String.class;
        }

        return CharSequence.class;
    }
}
