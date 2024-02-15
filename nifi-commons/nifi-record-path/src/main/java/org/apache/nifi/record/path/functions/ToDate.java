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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.util.RecordPathUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.TimeZone;
import java.util.stream.Stream;

public class ToDate extends RecordPathSegment {

    private final RecordPathSegment recordPath;
    private final RecordPathSegment dateFormat;
    private final RecordPathSegment timeZoneID;

    public ToDate(final RecordPathSegment recordPath, final RecordPathSegment dateFormat, final boolean absolute) {
        super("toDate", null, absolute);
        this.recordPath = recordPath;
        this.dateFormat = dateFormat;
        this.timeZoneID = null;
    }

    public ToDate(final RecordPathSegment recordPath, final RecordPathSegment dateFormat, final RecordPathSegment timeZoneID, final boolean absolute) {
        super("toDate", null, absolute);
        this.recordPath = recordPath;
        this.dateFormat = dateFormat;
        this.timeZoneID = timeZoneID;
    }

    @Override
    public Stream<FieldValue> evaluate(RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
                .map(fv -> {

                    final Object fieldValue = fv.getValue();
                    if (!(fieldValue instanceof String)) {
                        return fv;
                    }

                    final DateTimeFormatter dateTimeFormatter = getDateTimeFormatter(dateFormat, context);

                    final Date dateValue;
                    try {
                        final TemporalAccessor parsed = dateTimeFormatter.parse(fieldValue.toString());

                        int year = 0;
                        if (parsed.isSupported(ChronoField.YEAR_OF_ERA)) {
                            year = parsed.get(ChronoField.YEAR_OF_ERA);
                        }

                        int month = 0;
                        if (parsed.isSupported(ChronoField.MONTH_OF_YEAR)) {
                            month = parsed.get(ChronoField.MONTH_OF_YEAR);
                        }

                        int day = 0;
                        if (parsed.isSupported(ChronoField.DAY_OF_MONTH)) {
                            day = parsed.get(ChronoField.DAY_OF_MONTH);
                        }

                        int hour = 0;
                        if (parsed.isSupported(ChronoField.HOUR_OF_DAY)) {
                            hour = parsed.get(ChronoField.HOUR_OF_DAY);
                        }

                        int minute = 0;
                        if (parsed.isSupported(ChronoField.MINUTE_OF_HOUR)) {
                            minute = parsed.get(ChronoField.MINUTE_OF_HOUR);
                        }

                        int second = 0;
                        if (parsed.isSupported(ChronoField.SECOND_OF_MINUTE)) {
                            second = parsed.get(ChronoField.SECOND_OF_MINUTE);
                        }

                        int nano = 0;
                        if (parsed.isSupported(ChronoField.MILLI_OF_SECOND)) {
                            nano = parsed.get(ChronoField.NANO_OF_SECOND);
                        }

                        ZoneId zoneId = getZoneId(context);
                        if (zoneId == null) {
                            zoneId = ZoneId.systemDefault();
                        }

                        final ZonedDateTime zonedDateTime = ZonedDateTime.of(year, month, day, hour, minute, second, nano, zoneId);
                        final Instant instant = zonedDateTime.toInstant();
                        dateValue = Date.from(instant);
                    } catch (final Exception e) {
                        return fv;
                    }

                    return new StandardFieldValue(dateValue, fv.getField(), fv.getParent().orElse(null));
                });
    }

    private DateTimeFormatter getDateTimeFormatter(final RecordPathSegment dateFormatSegment, final RecordPathEvaluationContext context) {
        if (dateFormatSegment == null) {
            return null;
        }

        final String dateFormatString = RecordPathUtils.getFirstStringValue(dateFormatSegment, context);
        if (StringUtils.isEmpty(dateFormatString)) {
            return null;
        }

        try {
            final ZoneId zoneId = getZoneId(context);
            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormatString);
            if (zoneId != null) {
                formatter.withZone(zoneId);
            }
            return formatter;
        } catch (final Exception e) {
            return null;
        }
    }

    private ZoneId getZoneId(final RecordPathEvaluationContext context) {
        final ZoneId zoneId;

        if (timeZoneID == null) {
            zoneId = null;
        } else {
            final String timeZoneStr = RecordPathUtils.getFirstStringValue(timeZoneID, context);
            if (StringUtils.isEmpty(timeZoneStr)) {
                zoneId = null;
            } else {
                zoneId = TimeZone.getTimeZone(timeZoneStr).toZoneId();
            }
        }

        return zoneId;
    }
}
