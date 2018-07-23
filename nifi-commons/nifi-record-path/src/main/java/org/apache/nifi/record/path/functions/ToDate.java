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

import java.util.Date;
import java.util.stream.Stream;

public class ToDate extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment dateFormat;

    public ToDate(final RecordPathSegment recordPath, final RecordPathSegment dateFormat, final boolean absolute) {
        super("toDate", null, absolute);
        this.recordPath = recordPath;
        this.dateFormat = dateFormat;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {

                    if (!(fv.getValue() instanceof String)) {
                        return fv;
                    }

                    final java.text.DateFormat dateFormat = getDateFormat(this.dateFormat, context);

                    final Date dateValue;
                    try {
                        dateValue = DataTypeUtils.toDate(fv.getValue(), () -> dateFormat, fv.getField().getFieldName());
                    } catch (final Exception e) {
                        return fv;
                    }

                    if (dateValue == null) {
                        return fv;
                    }

                    return new StandardFieldValue(dateValue, fv.getField(), fv.getParent().orElse(null));
                });
    }

    private java.text.DateFormat getDateFormat(final RecordPathSegment dateFormatSegment, final RecordPathEvaluationContext context) {
        if (dateFormatSegment == null) {
            return null;
        }

        final String dateFormatString = RecordPathUtils.getFirstStringValue(dateFormatSegment, context);
        if (dateFormatString == null || dateFormatString.isEmpty()) {
            return null;
        }

        try {
            return DataTypeUtils.getDateFormat(dateFormatString);
        } catch (final Exception e) {
            return null;
        }
    }

}
