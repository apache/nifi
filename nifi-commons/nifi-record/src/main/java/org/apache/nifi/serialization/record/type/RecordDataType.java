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

package org.apache.nifi.serialization.record.type;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldRemovalPath;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class RecordDataType extends DataType {
    private RecordSchema childSchema;
    private final Supplier<RecordSchema> childSchemaSupplier;

    public RecordDataType(final RecordSchema childSchema) {
        super(RecordFieldType.RECORD, null);
        this.childSchema = childSchema;
        this.childSchemaSupplier = null;
    }

    public RecordDataType(final Supplier<RecordSchema> childSchemaSupplier) {
        super(RecordFieldType.RECORD, null);
        this.childSchema = null;
        this.childSchemaSupplier = childSchemaSupplier;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.RECORD;
    }

    public RecordSchema getChildSchema() {
        if (childSchema == null) {
            if (childSchemaSupplier != null) {
                childSchema = childSchemaSupplier.get();
            }
        }
        return childSchema;
    }

    @Override
    public void removePath(final RecordFieldRemovalPath path) {
        if (path.length() == 0) {
            return;
        }
        getChildSchema().removePath(path);
    }

    @Override
    public boolean isRecursive(final List<RecordSchema> schemas) {
        // allow for childSchema to be null during schema inference
        final RecordSchema childSchema = getChildSchema();
        if (!schemas.isEmpty() && childSchema != null) {
            if (childSchema.sameAsAny(schemas)) {
                return true;
            } else {
                final List<RecordSchema> schemasWithChildSchema = new ArrayList<>(schemas);
                schemasWithChildSchema.add(childSchema);

                for (final RecordField childField : childSchema.getFields()) {
                    if (childField.getDataType().isRecursive(schemasWithChildSchema)) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getFieldType().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RecordDataType)) {
            return false;
        }

        final RecordDataType other = (RecordDataType) obj;
        return Objects.equals(childSchema, other.childSchema);
    }

    @Override
    public String toString() {
        return RecordFieldType.RECORD.toString();
    }
}
