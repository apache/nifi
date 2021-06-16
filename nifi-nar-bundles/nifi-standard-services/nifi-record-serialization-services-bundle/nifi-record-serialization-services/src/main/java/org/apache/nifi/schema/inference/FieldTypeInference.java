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
package org.apache.nifi.schema.inference;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class FieldTypeInference {
    private static final DataType DEFAULT_DATA_TYPE = RecordFieldType.STRING.getDataType();

    // We don't actually need a singleDataType and a Set of DataTypes - we could use
    // just the Set. However, the most common case will be the case where there is only a single
    // unique value for the data type, and so this paradigm allows us to avoid the cost of creating
    // and using the HashSet.
    private DataType singleDataType = null;
    private Set<DataType> possibleDataTypes = new HashSet<>();

    public void addPossibleDataType(final DataType dataType) {
        if (dataType == null) {
            return;
        }

        if (singleDataType == null) {
            singleDataType = dataType;
            return;
        }

        if (singleDataType.equals(dataType) || possibleDataTypes.contains(dataType)) {
            return;
        }

        final RecordFieldType singleFieldType = singleDataType.getFieldType();
        final RecordFieldType additionalFieldType = dataType.getFieldType();

        if (singleFieldType == RecordFieldType.RECORD && additionalFieldType == RecordFieldType.RECORD) {
            // If we currently believe the field must be a Record, and the new possibility is also a record but the schemas
            // are different, then consider the inferred type to be a Record with all possible fields. This is done, in comparison
            // to using a UNION of the two because we can have a case where we have Records with many optional fields, and using a
            // UNION could result in a UNION whose possible types are as long as number of permutations of those, which can be very
            // expensive and not any more correct than just having a Record all of whose fields are optional.
            final RecordSchema singleDataTypeSchema = ((RecordDataType) singleDataType).getChildSchema();
            final RecordSchema newSchema = ((RecordDataType) dataType).getChildSchema();

            final RecordSchema mergedSchema = DataTypeUtils.merge(singleDataTypeSchema, newSchema);
            possibleDataTypes.remove(singleDataType);
            singleDataType = RecordFieldType.RECORD.getRecordDataType(mergedSchema);
            possibleDataTypes.add(singleDataType);
            return;
        }

        if (possibleDataTypes.isEmpty()) {
            possibleDataTypes.add(singleDataType);
        }

        for (DataType possibleDataType : possibleDataTypes) {
            RecordFieldType possibleFieldType = possibleDataType.getFieldType();
            if (!possibleFieldType.equals(RecordFieldType.STRING) && possibleFieldType.isWiderThan(additionalFieldType)) {
                return;
            }
        }

        Iterator<DataType> possibleDataTypeIterator = possibleDataTypes.iterator();
        while (possibleDataTypeIterator.hasNext()) {
            DataType possibleDataType = possibleDataTypeIterator.next();
            RecordFieldType possibleFieldType = possibleDataType.getFieldType();

            if (!additionalFieldType.equals(RecordFieldType.STRING) && additionalFieldType.isWiderThan(possibleFieldType)) {
                possibleDataTypeIterator.remove();
            }
        }

        possibleDataTypes.add(dataType);
    }

    /**
     * Creates a single DataType that represents the field
     * @return a single DataType that represents the field
     */
    public DataType toDataType() {
        if (possibleDataTypes.isEmpty()) {
            if (singleDataType == null) {
                return DEFAULT_DATA_TYPE;
            }

            return singleDataType;
        }

        DataType aggregate = null;
        for (final DataType dataType : possibleDataTypes) {
            aggregate = DataTypeUtils.mergeDataTypes(aggregate, dataType);
        }

        return aggregate;
    }
}
