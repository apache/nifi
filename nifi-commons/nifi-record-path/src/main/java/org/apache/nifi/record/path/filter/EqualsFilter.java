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

public class EqualsFilter extends BinaryOperatorFilter {

    public EqualsFilter(final RecordPathSegment lhs, final RecordPathSegment rhs) {
        super(lhs, rhs);
    }

    @Override
    protected boolean test(final FieldValue fieldValue, final Object rhsValue) {
        final Object lhsValue = fieldValue.getValue();
        if (lhsValue == null) {
            return rhsValue == null;
        }

        if (lhsValue instanceof Number) {
            if (rhsValue instanceof Number) {
                return compareNumbers((Number) lhsValue, (Number) rhsValue);
            } else {
                return lhsValue.toString().equals(rhsValue.toString());
            }
        } else if (rhsValue instanceof Number) {
            return lhsValue.toString().equals(rhsValue.toString());
        }

        return lhsValue.equals(rhsValue);
    }

    @Override
    protected String getOperator() {
        return "=";
    }

    private boolean compareNumbers(final Number lhs, final Number rhs) {
        final boolean lhsLongCompatible = (lhs instanceof Long || lhs instanceof Integer || lhs instanceof Short || lhs instanceof Byte);
        final boolean rhsLongCompatible = (rhs instanceof Long || rhs instanceof Integer || lhs instanceof Short || lhs instanceof Byte);

        if (lhsLongCompatible && rhsLongCompatible) {
            return lhs.longValue() == rhs.longValue();
        }

        return lhs.doubleValue() == rhs.doubleValue();
    }
}
