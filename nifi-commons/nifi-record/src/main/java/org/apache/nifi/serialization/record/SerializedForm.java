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

package org.apache.nifi.serialization.record;

import java.util.Objects;

public interface SerializedForm {
    /**
     * @return the serialized form of the record. This could be a byte[], String, ByteBuffer, etc.
     */
    Object getSerialized();

    /**
     * @return the MIME type that the data is serialized in
     */
    String getMimeType();

    public static SerializedForm of(final java.util.function.Supplier<Object> serializedSupplier, final String mimeType) {
        Objects.requireNonNull(serializedSupplier);
        Objects.requireNonNull(mimeType);

        return new SerializedForm() {
            private volatile Object serialized = null;

            @Override
            public Object getSerialized() {
                if (serialized != null) {
                    return serialized;
                }

                final Object supplied = serializedSupplier.get();
                this.serialized = supplied;
                return supplied;
            }

            @Override
            public String getMimeType() {
                return mimeType;
            }

            @Override
            public int hashCode() {
                return 31 + 17 * mimeType.hashCode() + 15 * getSerialized().hashCode();
            }

            @Override
            public boolean equals(final Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj == null) {
                    return false;
                }

                if (!(obj instanceof SerializedForm)) {
                    return false;
                }

                final SerializedForm other = (SerializedForm) obj;
                return other.getMimeType().equals(mimeType) && Objects.deepEquals(other.getSerialized(), getSerialized());
            }
        };
    }

    public static SerializedForm of(final Object serialized, final String mimeType) {
        Objects.requireNonNull(serialized);
        Objects.requireNonNull(mimeType);

        return new SerializedForm() {
            @Override
            public Object getSerialized() {
                return serialized;
            }

            @Override
            public String getMimeType() {
                return mimeType;
            }

            @Override
            public int hashCode() {
                return 31 + 17 * mimeType.hashCode() + 15 * serialized.hashCode();
            }

            @Override
            public boolean equals(final Object obj) {
                if (obj == this) {
                    return true;
                }

                if (obj == null) {
                    return false;
                }

                if (!(obj instanceof SerializedForm)) {
                    return false;
                }

                final SerializedForm other = (SerializedForm) obj;
                return other.getMimeType().equals(mimeType) && Objects.deepEquals(other.getSerialized(), getSerialized());
            }
        };
    }
}
