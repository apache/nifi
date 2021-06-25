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
import org.apache.nifi.record.path.filter.RecordPathFilter;
import org.apache.nifi.record.path.paths.RecordPathSegment;

import java.util.stream.Stream;

/**
 * <p>
 * A Filter Function is responsible for taking a RecordPathFilter and turning it into a function that is capable of
 * being evaluated against a RecordPathEvaluationContext such that the return value is a Stream&lt;FieldValue&gt; whose
 * values are booleans.
 * </p>
 *
 * <p>
 * So while a RecordPathFilter would be evaluated against a RecordPathEvaluationContext and return a Stream of FieldValues representing
 * all elements that match the filter, the FilterFunction would instead return a true/false for each element indicating whether or not
 * it passes the filter.
 * </p>
 */
public class FilterFunction extends RecordPathSegment {

    private final RecordPathFilter filter;

    public FilterFunction(final String functionName, final RecordPathFilter filter, final boolean absolute) {
        super(functionName, null, absolute);
        this.filter = filter;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        return filter.mapToBoolean(context);
    }
}
