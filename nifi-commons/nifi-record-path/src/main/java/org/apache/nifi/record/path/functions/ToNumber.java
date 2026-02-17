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
package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.math.MathTypeUtils;
import org.apache.nifi.record.path.paths.RecordPathSegment;

import java.util.stream.Stream;

public class ToNumber extends RecordPathSegment {
    private final RecordPathSegment valuePath;

    public ToNumber(final RecordPathSegment valuePath, final boolean absolute) {
        super("toNumber", null, absolute);
        this.valuePath = valuePath;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final FieldValue fieldValue = valuePath.evaluate(context).findFirst().orElseThrow(() -> new IllegalArgumentException("toNumber function requires an operand"));
        final Object value = fieldValue.getValue();

        if (value == null || value instanceof Number) {
            return Stream.of(fieldValue);
        } else {
            return Stream.of(MathTypeUtils.toNumber(fieldValue));
        }
    }
}
