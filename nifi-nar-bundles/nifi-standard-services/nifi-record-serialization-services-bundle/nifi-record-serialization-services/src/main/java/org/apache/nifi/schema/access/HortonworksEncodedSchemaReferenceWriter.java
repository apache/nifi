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

package org.apache.nifi.schema.access;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

public class HortonworksEncodedSchemaReferenceWriter implements SchemaAccessWriter {
    private static final int LATEST_PROTOCOL_VERSION = 1;

    @Override
    public void writeHeader(final RecordSchema schema, final OutputStream out) throws IOException {
        final SchemaIdentifier identifier = schema.getIdentifier();
        final OptionalLong identifierOption = identifier.getIdentifier();
        if (!identifierOption.isPresent()) {
            throw new IOException("Cannot write Encoded Schema Reference for Schema because the Schema Identifier is not known");
        }

        final OptionalInt versionOption = identifier.getVersion();
        if (!versionOption.isPresent()) {
            throw new IOException("Cannot write Encoded Schema Reference for Schema because the Schema Version is not known");
        }

        final ByteBuffer bb = ByteBuffer.allocate(13);
        bb.put((byte) LATEST_PROTOCOL_VERSION);
        bb.putLong(identifierOption.getAsLong());
        bb.putInt(versionOption.getAsInt());

        out.write(bb.array());
    }

    @Override
    public Map<String, String> getAttributes(final RecordSchema schema) {
        return Collections.emptyMap();
    }

}
