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

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.stream.Stream;

public class Base64Encode extends RecordPathSegment {
    private final RecordPathSegment recordPath;

    public Base64Encode(final RecordPathSegment recordPath, final boolean absolute) {
        super("base64Encode", null, absolute);
        this.recordPath = recordPath;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {

                    Object value = fv.getValue();
                    if (value instanceof String) {
                        try {
                            return new StandardFieldValue(Base64.getEncoder().encodeToString(value.toString().getBytes("UTF-8")), fv.getField(), fv.getParent().orElse(null));
                        } catch (final UnsupportedEncodingException e) {
                            return null;    // won't happen.
                        }
                    } else if (value instanceof byte[]) {
                        return new StandardFieldValue(Base64.getEncoder().encode((byte[]) value), fv.getField(), fv.getParent().orElse(null));
                    } else {
                        throw new IllegalArgumentException("Argument supplied to base64Encode must be a String or byte[]");
                    }
                });
    }

}
