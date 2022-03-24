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
package org.apache.nifi.hl7.query.evaluator.comparison;

import java.util.Collection;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Field;
import org.apache.nifi.hl7.query.evaluator.BooleanEvaluator;
import org.apache.nifi.hl7.query.evaluator.Evaluator;

public abstract class AbstractComparisonEvaluator extends BooleanEvaluator {

    private final Evaluator<?> lhs;
    private final Evaluator<?> rhs;

    public AbstractComparisonEvaluator(final Evaluator<?> lhs, final Evaluator<?> rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public final Boolean evaluate(final Map<String, Object> objectMap) {
        final Object lhsValue = lhs.evaluate(objectMap);
        if (lhsValue == null) {
            return false;
        }

        final Object rhsValue = rhs.evaluate(objectMap);
        if (rhsValue == null) {
            return false;
        }

        return compareRaw(lhsValue, rhsValue);
    }

    private Boolean compareRaw(Object lhsValue, Object rhsValue) {
        if (lhsValue == null || rhsValue == null) {
            return false;
        }

        if (lhsValue instanceof HL7Field) {
            lhsValue = ((HL7Field) lhsValue).getValue();
        }

        if (rhsValue instanceof HL7Field) {
            rhsValue = ((HL7Field) rhsValue).getValue();
        }

        if (lhsValue == null || rhsValue == null) {
            return false;
        }

        /**
         * both are collections, and compare(lhsValue, rhsValue) is false.
         * this would be the case, for instance, if we compared field 1 of one segment to
         * a field in another segment, and both fields had components.
         */
        if (lhsValue instanceof Collection && rhsValue instanceof Collection) {
            return false;
        }

        /**
         * if one side is a collection but the other is not, check if any element in that
         * collection compares to the other element in a way that satisfies the condition.
         * this would happen, for instance, if we check Segment1.Field5 = 'X' and field 5 repeats
         * with a value "A~B~C~X~Y~Z"; in this case we do want to consider Field 5 = X as true.
         */
        if (lhsValue instanceof Collection) {
            for (final Object lhsObject : (Collection<?>) lhsValue) {
                if (compareRaw(lhsObject, rhsValue)) {
                    return true;
                }
            }

            return false;
        }

        if (rhsValue instanceof Collection) {
            for (final Object rhsObject : (Collection<?>) rhsValue) {
                if (compareRaw(rhsObject, lhsValue)) {
                    return true;
                }
            }

            return false;
        }

        if (lhsValue != null && rhsValue != null && compare(lhsValue, rhsValue)) {
            return true;
        }

        return false;
    }

    protected abstract boolean compare(Object lhs, Object rhs);
}
