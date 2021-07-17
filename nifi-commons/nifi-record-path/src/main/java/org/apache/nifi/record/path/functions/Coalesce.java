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
import org.apache.nifi.record.path.paths.RecordPathSegment;

import java.util.Optional;
import java.util.stream.Stream;

public class Coalesce extends RecordPathSegment {

    private final RecordPathSegment[] valuePaths;

    public Coalesce(final RecordPathSegment[] valuePaths, final boolean absolute) {
        super("coalesce", null, absolute);
        this.valuePaths = valuePaths;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        for (final RecordPathSegment valuePath : valuePaths) {
            final Stream<FieldValue> stream = valuePath.evaluate(context);
            final Optional<FieldValue> firstFieldValue = stream.findFirst();

            if (firstFieldValue.isPresent()) {
                // If the Optional is Present, it means that it found the field, but the value may still be explicitly null.
                final FieldValue fieldValue = firstFieldValue.get();
                if (fieldValue.getValue() != null) {
                    return Stream.of(firstFieldValue.get());
                }
            }
        }

        return Stream.empty();
    }
}
