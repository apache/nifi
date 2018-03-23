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
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.charset.Charset;
import java.util.stream.Stream;

public class ToString extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment charsetSegment;

    public ToString(final RecordPathSegment recordPath, final RecordPathSegment charsetSegment, final boolean absolute) {
        super("toString", null, absolute);
        this.recordPath = recordPath;
        this.charsetSegment = charsetSegment;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {
                    final Charset charset = getCharset(this.charsetSegment, context);
                    Object value = fv.getValue();
                    final String stringValue;

                    if (value instanceof Object[]) {
                        Object[] o = (Object[]) value;
                        if (o.length > 0) {

                            byte[] dest = new byte[o.length];
                            for (int i = 0; i < o.length; i++) {
                                dest[i] = (byte) o[i];
                            }
                            stringValue = new String(dest, charset);
                        } else {
                            stringValue = ""; // Empty array = empty string
                        }
                    } else if (!(fv.getValue() instanceof byte[])) {
                        stringValue = fv.getValue().toString();
                    } else {
                        stringValue = DataTypeUtils.toString(fv.getValue(), (String) null, charset);
                    }
                    return new StandardFieldValue(stringValue, fv.getField(), fv.getParent().orElse(null));
                });
    }

    private Charset getCharset(final RecordPathSegment charsetSegment, final RecordPathEvaluationContext context) {
        if (charsetSegment == null) {
            return null;
        }

        final String charsetString = RecordPathUtils.getFirstStringValue(charsetSegment, context);
        if (charsetString == null || charsetString.isEmpty()) {
            return null;
        }

        return DataTypeUtils.getCharset(charsetString);
    }

}
