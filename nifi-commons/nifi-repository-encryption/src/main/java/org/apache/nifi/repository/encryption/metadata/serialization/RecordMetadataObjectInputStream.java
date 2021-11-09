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
package org.apache.nifi.repository.encryption.metadata.serialization;

import org.apache.nifi.repository.encryption.metadata.RecordMetadata;
import org.apache.nifi.repository.encryption.metadata.RecordMetadataReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * Custom subclass of ObjectInputStream to support compatibility with different metadata objects
 */
public class RecordMetadataObjectInputStream extends ObjectInputStream implements RecordMetadataReader {
    /** Metadata Package Name */
    private static final String METADATA_PACKAGE_NAME = "org.apache.nifi";

    public RecordMetadataObjectInputStream(final InputStream inputStream) throws IOException {
        super(inputStream);
    }

    /**
     * Get Record Metadata reading from serialized object
     *
     * @return Record Metadata
     * @throws IOException Thrown on readObject() failures
     */
    @Override
    public RecordMetadata getRecordMetadata() throws IOException {
        try {
            return (RecordMetadata) readObject();
        } catch (final ClassNotFoundException e) {
            throw new IOException("Metadata Class not found", e);
        }
    }

    /**
     * Read Class Descriptor returns definition of SerializableRecordMetadata for metadata implementation classes
     *
     * @return Object Stream Class describing Record Metadata
     */
    @Override
    protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass classDescriptor = super.readClassDescriptor();
        final String name = classDescriptor.getName();
        if (name.startsWith(METADATA_PACKAGE_NAME)) {
            if (classDescriptor.getFields().length == 0) {
                // Return standard class for descriptor with no fields
                classDescriptor = ObjectStreamClass.lookup(StandardRecordMetadata.class);
            } else {
                // Return serializable class for descriptor with fields
                classDescriptor = ObjectStreamClass.lookup(SerializableRecordMetadata.class);
            }
        }
        return classDescriptor;
    }
}
