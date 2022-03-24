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

package org.apache.nifi.record.path.filter;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public abstract class NumericBinaryOperatorFilter extends BinaryOperatorFilter {

    public NumericBinaryOperatorFilter(final RecordPathSegment lhs, final RecordPathSegment rhs) {
        super(lhs, rhs);
    }

    @Override
    protected boolean test(final FieldValue fieldValue, final Object rhsValue) {
        if (fieldValue.getValue() == null) {
            return false;
        }

        final Object value = fieldValue.getValue();

        boolean lhsNumeric;
        final boolean lhsLongCompatible = DataTypeUtils.isLongTypeCompatible(value);
        final boolean lhsDoubleCompatible;
        if (lhsLongCompatible) {
            lhsNumeric = true;
        } else {
            lhsDoubleCompatible = DataTypeUtils.isDoubleTypeCompatible(value);
            lhsNumeric = lhsDoubleCompatible;
        }

        if (!lhsNumeric) {
            return false;
        }


        boolean rhsNumeric;
        final boolean rhsLongCompatible = DataTypeUtils.isLongTypeCompatible(rhsValue);
        final boolean rhsDoubleCompatible;
        if (rhsLongCompatible) {
            rhsNumeric = true;
        } else {
            rhsDoubleCompatible = DataTypeUtils.isDoubleTypeCompatible(rhsValue);
            rhsNumeric = rhsDoubleCompatible;
        }

        if (!rhsNumeric) {
            return false;
        }

        final String fieldName = fieldValue.getField() == null ? "<Anonymous Inner Field>" : fieldValue.getField().getFieldName();
        final Number lhsNumber = lhsLongCompatible ? DataTypeUtils.toLong(value, fieldName) : DataTypeUtils.toDouble(value, fieldName);
        final Number rhsNumber = rhsLongCompatible ? DataTypeUtils.toLong(rhsValue, fieldName) : DataTypeUtils.toDouble(rhsValue, fieldName);
        return compare(lhsNumber, rhsNumber);
    }

    protected abstract boolean compare(final Number lhsNumber, final Number rhsNumber);
}