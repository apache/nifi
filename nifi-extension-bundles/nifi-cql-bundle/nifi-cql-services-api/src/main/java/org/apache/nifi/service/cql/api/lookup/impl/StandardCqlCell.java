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
package org.apache.nifi.service.cql.api.lookup.impl;

import org.apache.nifi.service.cql.api.lookup.CqlCell;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A {@link CqlCell} holding an already-decoded value, shared by both session providers.
 *
 * @param columnName          the column name as the server sent it
 * @param value               the decoded value, or {@code null} for a CQL null; must be a JDK type, or use
 *                            {@link #unsupported}
 * @param unsupportedTypeName the CQL type name when it has no JDK representation, else {@code null}; the
 *                            failure is deferred to {@link #getObject()} so one unreadable column does not
 *                            fail the whole row
 */
public record StandardCqlCell(String columnName, Object value, String unsupportedTypeName) implements CqlCell {

    public StandardCqlCell {
        Objects.requireNonNull(columnName, "columnName is required");
    }

    /**
     * A cell whose value is a JDK type the caller can read.
     */
    public StandardCqlCell(final String columnName, final Object value) {
        this(columnName, value, null);
    }

    /** A cell whose CQL type has no JDK representation (a UDT, a tuple, {@code duration}); reading it throws. */
    public static StandardCqlCell unsupported(final String columnName, final String cqlTypeName) {
        return new StandardCqlCell(columnName, null, Objects.requireNonNull(cqlTypeName, "cqlTypeName is required"));
    }

    @Override
    public boolean isNull() {
        return value == null && unsupportedTypeName == null;
    }

    @Override
    public byte[] getBytes() {
        requireSupported();

        return switch (value) {
            case null -> null;
            case byte[] bytes -> bytes.clone();
            // Read via a duplicate so the shared buffer's position is left alone: the backend may hand the
            // same buffer to more than one reader, and consuming it here would empty it for the next.
            case ByteBuffer buffer -> {
                final ByteBuffer readable = buffer.duplicate();
                final byte[] bytes = new byte[readable.remaining()];
                readable.get(bytes);
                yield bytes;
            }
            default -> throw new UnsupportedOperationException(String.format(
                    "Column '%s' is not binary; its value is a %s", columnName, value.getClass().getName()));
        };
    }

    @Override
    public Object getObject() {
        requireSupported();
        return value;
    }

    private void requireSupported() {
        if (unsupportedTypeName != null) {
            throw new UnsupportedOperationException(String.format(
                    "Column '%s' has CQL type %s, which has no JDK representation. Use CQLExecutionService.query "
                            + "for this column; it maps such types to NiFi records.", columnName, unsupportedTypeName));
        }
    }
}
