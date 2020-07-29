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
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.charset.Charset;
import java.util.stream.Stream;

public class ToBytes extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment charsetSegment;

    public ToBytes(final RecordPathSegment recordPath, final RecordPathSegment charsetSegment, final boolean absolute) {
        super("toBytes", null, absolute);
        this.recordPath = recordPath;
        this.charsetSegment = charsetSegment;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {

                    if (!(fv.getValue() instanceof String)) {
                        throw new IllegalArgumentException("Argument supplied to toBytes must be a String");
                    }

                    final Charset charset = getCharset(this.charsetSegment, context);

                    final byte[] bytesValue;
                    Byte[] src = (Byte[]) DataTypeUtils.toArray(fv.getValue(), fv.getField().getFieldName(), RecordFieldType.BYTE.getDataType(), charset);
                    bytesValue = new byte[src.length];
                    for (int i = 0; i < src.length; i++) {
                        bytesValue[i] = src[i];
                    }

                    return new StandardFieldValue(bytesValue, fv.getField(), fv.getParent().orElse(null));
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
