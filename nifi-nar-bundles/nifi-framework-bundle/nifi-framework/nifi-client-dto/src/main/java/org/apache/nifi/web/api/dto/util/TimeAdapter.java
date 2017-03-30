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
package org.apache.nifi.web.api.dto.util;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * XmlAdapter for (un)marshalling a time.
 */
public class TimeAdapter extends XmlAdapter<String, Date> {

    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss z";

    private static final ZoneId ZONE_ID = TimeZone.getDefault().toZoneId();

    @Override
    public String marshal(Date date) throws Exception {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT, Locale.US);
        final ZonedDateTime localDateTime = ZonedDateTime.ofInstant(date.toInstant(), ZONE_ID);
        return formatter.format(localDateTime);
    }

    @Override
    public Date unmarshal(String date) throws Exception {
        final LocalDateTime now = LocalDateTime.now();
        final DateTimeFormatter parser = new DateTimeFormatterBuilder().appendPattern(DEFAULT_TIME_FORMAT)
                .parseDefaulting(ChronoField.YEAR, now.getYear())
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, now.getMonthValue())
                .parseDefaulting(ChronoField.DAY_OF_MONTH, now.getDayOfMonth())
                .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
                .toFormatter(Locale.US);
        final LocalDateTime parsedDateTime = LocalDateTime.parse(date, parser);
        return Date.from(parsedDateTime.toInstant(ZONE_ID.getRules().getOffset(now)));
    }

}
