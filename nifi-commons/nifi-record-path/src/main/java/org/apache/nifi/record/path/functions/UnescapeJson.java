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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class UnescapeJson extends RecordPathSegment {
    private final RecordPathSegment recordPath;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public UnescapeJson(final RecordPathSegment recordPath, final boolean absolute) {
        super("unescapeJson", null, absolute);
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
                            DataType dataType = fv.getField().getDataType();
                            if (fv.getField().getDataType() instanceof ChoiceDataType) {
                                dataType = DataTypeUtils.chooseDataType(value, (ChoiceDataType) fv.getField().getDataType());
                            }

                            return new StandardFieldValue(convertFieldValue(value, fv.getField().getFieldName(), dataType), fv.getField(), fv.getParent().orElse(null));
                        } catch (IOException e) {
                            throw new RecordPathException("Unable to deserialise JSON String into Record Path value", e);
                        }
                    } else {
                        throw new IllegalArgumentException("Argument supplied to unescapeJson must be a String");
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private Object convertFieldValue(final Object value, final String fieldName, final DataType dataType) throws IOException {
        if (dataType instanceof RecordDataType) {
            // convert Maps to Records
            final Map<String, Object> map = objectMapper.readValue(value.toString(), Map.class);
            return DataTypeUtils.toRecord(map, ((RecordDataType) dataType).getChildSchema(), fieldName);
        } else if (dataType instanceof ArrayDataType) {
            final DataType elementDataType = ((ArrayDataType) dataType).getElementType();

            // convert Arrays of Maps to Records
            Object[] arr = objectMapper.readValue(value.toString(), Object[].class);
            if (elementDataType instanceof RecordDataType) {
                arr = Arrays.stream(arr).map(e -> DataTypeUtils.toRecord(e, ((RecordDataType) elementDataType).getChildSchema(), fieldName)).toArray();
            }
            return arr;
        } else {
            // generic conversion for simpler fields
            return objectMapper.readValue(value.toString(), Object.class);
        }
    }
}
