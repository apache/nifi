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
import org.apache.nifi.record.path.StandardRecordPathEvaluationContext;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.Record;

import java.util.Arrays;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Anchored extends RecordPathSegment {

    private final RecordPathSegment anchorPath;
    private final RecordPathSegment evaluationPath;

    public Anchored(final RecordPathSegment anchorPath, final RecordPathSegment evaluationPath, final boolean absolute) {
        super("anchored", null, absolute);

        this.anchorPath = anchorPath;
        this.evaluationPath = evaluationPath;
    }


    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> anchoredStream = anchorPath.evaluate(context);

        return anchoredStream.flatMap(fv -> {
            final Object value = fv.getValue();
            return evaluate(value);
        });
    }

    private Stream<FieldValue> evaluate(final Object value) {
        if (value == null) {
            return Stream.of();
        }
        if (value instanceof Record) {
            final RecordPathEvaluationContext recordPathEvaluateContext = new StandardRecordPathEvaluationContext((Record) value);
            return evaluationPath.evaluate(recordPathEvaluateContext);
        }
        if (value instanceof final Record[] array) {
            return Arrays.stream(array).flatMap(element -> {
                final RecordPathEvaluationContext recordPathEvaluateContext = new StandardRecordPathEvaluationContext(element);
                return evaluationPath.evaluate(recordPathEvaluateContext);
            });
        }
        if (value instanceof final Iterable<?> iterable) {
            return StreamSupport.stream(iterable.spliterator(), false).flatMap(element -> {
                if (!(element instanceof Record)) {
                    return Stream.of();
                }

                final RecordPathEvaluationContext recordPathEvaluateContext = new StandardRecordPathEvaluationContext((Record) element);
                return evaluationPath.evaluate(recordPathEvaluateContext);
            });
        }

        return Stream.of();
    }
}
