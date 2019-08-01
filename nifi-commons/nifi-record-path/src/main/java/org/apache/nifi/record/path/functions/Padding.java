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
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

abstract class Padding extends RecordPathSegment {

    public final static String DEFAULT_PADDING_STRING = "_";

    private RecordPathSegment paddingStringPath;
    private RecordPathSegment inputStringPath;
    private RecordPathSegment desiredLengthPath;

    Padding( final String path,
             final RecordPathSegment parentPath,
             final RecordPathSegment inputStringPath,
             final RecordPathSegment desiredLengthPath,
             final RecordPathSegment paddingStringPath,
             final boolean absolute) {

        super(path, parentPath, absolute);
        this.paddingStringPath = paddingStringPath;
        this.inputStringPath = inputStringPath;
        this.desiredLengthPath = desiredLengthPath;
    }

    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        String pad = getPaddingString(context);

        final Stream<FieldValue> evaluatedStr = inputStringPath.evaluate(context);
        return evaluatedStr.map(fv -> {

            final OptionalInt desiredLengthOpt = getDesiredLength(context);
            if (!desiredLengthOpt.isPresent()) {
                return new StandardFieldValue("", fv.getField(), fv.getParent().orElse(null));
            }

            int desiredLength = desiredLengthOpt.getAsInt();
            final String value = DataTypeUtils.toString(fv.getValue(), (String) null);
            return new StandardFieldValue(doPad(value, desiredLength, pad), fv.getField(), fv.getParent().orElse(null));
        });
    }

    protected abstract String doPad(String inputString, int desiredLength, String pad);

    private OptionalInt getDesiredLength(RecordPathEvaluationContext context) {

        Optional<FieldValue> lengthOption = desiredLengthPath.evaluate(context).findFirst();

        if (!lengthOption.isPresent()) {
            return OptionalInt.empty();
        }

        final FieldValue fieldValue = lengthOption.get();
        final Object length = fieldValue.getValue();
        if (!DataTypeUtils.isIntegerTypeCompatible(length)) {
            return OptionalInt.empty();
        }

        final String fieldName;
        final RecordField field = fieldValue.getField();
        fieldName = field == null ? "<Unknown Field>" : field.getFieldName();

        return OptionalInt.of(DataTypeUtils.toInteger(length, fieldName));
    }

    private String getPaddingString(RecordPathEvaluationContext context){

        if (null == paddingStringPath) {
            return DEFAULT_PADDING_STRING;
        }

        String padStr = RecordPathUtils.getFirstStringValue(paddingStringPath, context);

        if (null != padStr && !padStr.isEmpty()){
            return padStr;
        }
        return DEFAULT_PADDING_STRING;
    }
}
