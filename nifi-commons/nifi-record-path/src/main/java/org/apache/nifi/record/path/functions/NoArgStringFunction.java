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
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.stream.Stream;

/**
 * Abstract class for String functions without any argument.
 */
public abstract class NoArgStringFunction extends RecordPathSegment {
    private final RecordPathSegment valuePath;

    public NoArgStringFunction(final String path, final RecordPathSegment valuePath, final boolean absolute) {
        super(path, null, absolute);
        this.valuePath = valuePath;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        return valuePath.evaluate(context).map(fv -> {
            final String original = fv.getValue() == null ? "" : DataTypeUtils.toString(fv.getValue(), (String) null);
            final String processed = apply(original);
            return new StandardFieldValue(processed, fv.getField(), fv.getParent().orElse(null));
        });
    }

    /**
     * Sub-classes apply its function to the given value and return the result.
     * @param value possibly null
     * @return the function result
     */
    abstract String apply(String value);

}
