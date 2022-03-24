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
package org.apache.nifi.hl7.query.evaluator.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.nifi.hl7.model.HL7Component;
import org.apache.nifi.hl7.model.HL7Message;
import org.apache.nifi.hl7.model.HL7Segment;
import org.apache.nifi.hl7.query.evaluator.Evaluator;
import org.apache.nifi.hl7.query.evaluator.IntegerEvaluator;

public class DotEvaluator implements Evaluator<Object> {

    private final Evaluator<?> lhs;
    private final IntegerEvaluator rhs;

    public DotEvaluator(final Evaluator<?> lhs, final IntegerEvaluator rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Object evaluate(final Map<String, Object> objectMap) {
        final Object lhsValue = this.lhs.evaluate(objectMap);
        final Integer rhsValue = this.rhs.evaluate(objectMap);

        if (lhsValue == null || rhsValue == null) {
            return null;
        }

        final List<Object> results = new ArrayList<>();
        if (lhsValue instanceof Collection) {
            final Collection<?> lhsCollection = (Collection<?>) lhsValue;
            for (final Object obj : lhsCollection) {
                final Object val = getValue(obj, rhsValue);
                results.add(val);
            }
        } else {
            final Object val = getValue(lhsValue, rhsValue);
            return val;
        }

        return results;
    }

    private Object getValue(final Object lhsValue, final int rhsValue) {
        final List<?> list;
        if (lhsValue instanceof HL7Message) {
            list = ((HL7Message) lhsValue).getSegments();
        } else if (lhsValue instanceof HL7Segment) {
            list = ((HL7Segment) lhsValue).getFields();
        } else if (lhsValue instanceof HL7Component) {
            list = ((HL7Component) lhsValue).getComponents();
        } else {
            return null;
        }

        if (rhsValue > list.size()) {
            return null;
        }

        // convert from 0-based to 1-based
        return list.get(rhsValue - 1);
    }

    @Override
    public Class<? extends Object> getType() {
        return Object.class;
    }

}
