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
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.stream.Stream;

public abstract class AbstractStringFunction extends RecordPathSegment {
    protected final RecordPathSegment[] valuePaths;
    protected String pathValue;

    public AbstractStringFunction(final String path, final RecordPathSegment[] valuePaths, final boolean absolute) {
        super(path, null, absolute);
        this.valuePaths = valuePaths;
        this.pathValue = path;
    }

    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context, EvaluationCallback callback) {
        Stream<FieldValue> evaluated = Stream.empty();

        for (final RecordPathSegment valuePath : valuePaths) {
            final Stream<FieldValue> stream = valuePath.evaluate(context);
            evaluated = Stream.concat(evaluated, stream);
        }

        StringBuilder sb = callback.evaluate(evaluated);

        final RecordField field = new RecordField(pathValue, RecordFieldType.STRING.getDataType());
        final FieldValue responseValue = new StandardFieldValue(sb.toString(), field, null);
        return Stream.of(responseValue);
    }

    interface EvaluationCallback {
        StringBuilder evaluate(Stream<FieldValue> stream);
    }
}
