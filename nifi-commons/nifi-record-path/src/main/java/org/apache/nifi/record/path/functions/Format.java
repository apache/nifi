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
import org.apache.nifi.util.StringUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Stream;

public class Format extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment dateFormat;
    private final RecordPathSegment timeZoneID;

    public Format(final RecordPathSegment recordPath, final RecordPathSegment dateFormat, final boolean absolute) {
        super("format", null, absolute);
        this.recordPath = recordPath;
        this.dateFormat = dateFormat;
        this.timeZoneID = null;
    }

    public Format(final RecordPathSegment recordPath, final RecordPathSegment dateFormat, final RecordPathSegment timeZoneID, final boolean absolute) {
        super("format", null, absolute);
        this.recordPath = recordPath;
        this.dateFormat = dateFormat;
        this.timeZoneID = timeZoneID;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {
                    final DateTimeFormatter dateTimeFormatter = getDateTimeFormatter(this.dateFormat, this.timeZoneID, context);
                    if (dateTimeFormatter == null) {
                        return fv;
                    }

                    final Object fieldValue = fv.getValue();

                    final Instant instant;
                    if (fieldValue instanceof Date dateField) {
                        instant = Instant.ofEpochMilli(dateField.getTime());
                    } else if (fieldValue instanceof Number numberField) {
                        instant = Instant.ofEpochMilli(numberField.longValue());
                    } else {
                        return fv;
                    }

                    final ZoneId zoneId;
                    if (timeZoneID == null) {
                        zoneId = ZoneId.systemDefault();
                    } else {
                        final String timeZoneId = RecordPathUtils.getFirstStringValue(timeZoneID, context);
                        zoneId = TimeZone.getTimeZone(timeZoneId).toZoneId();
                    }

                    final ZonedDateTime dateTime = instant.atZone(zoneId);
                    final String formatted = dateTimeFormatter.format(dateTime);
                    return new StandardFieldValue(formatted, fv.getField(), fv.getParent().orElse(null));
                });
    }

    private DateTimeFormatter getDateTimeFormatter(final RecordPathSegment dateFormatSegment, final RecordPathSegment timeZoneID, final RecordPathEvaluationContext context) {
        final String dateFormatString = RecordPathUtils.getFirstStringValue(dateFormatSegment, context);
        if (StringUtils.isEmpty(dateFormatString)) {
            return null;
        }

        try {
            if (timeZoneID == null) {
                return DateTimeFormatter.ofPattern(dateFormatString);
            } else {
                final String timeZoneStr = RecordPathUtils.getFirstStringValue(timeZoneID, context);
                if (StringUtils.isEmpty(timeZoneStr)) {
                    return null;
                }
                final ZoneId zoneId = TimeZone.getTimeZone(timeZoneStr).toZoneId();
                return DateTimeFormatter.ofPattern(dateFormatString).withZone(zoneId);
            }
        } catch (final Exception e) {
            return null;
        }
    }
}
