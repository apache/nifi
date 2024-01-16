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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import jakarta.xml.bind.annotation.adapters.XmlAdapter;

/**
 * XmlAdapter for (un)marshalling a date/time.
 */
public class DateTimeAdapter extends XmlAdapter<String, Date> {

    private static final String DEFAULT_DATE_TIME_FORMAT = "MM/dd/yyyy HH:mm:ss z";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_DATE_TIME_FORMAT);

    @Override
    public String marshal(Date date) throws Exception {
        final ZonedDateTime zonedDateTime = date.toInstant().atZone(ZoneId.systemDefault());
        return DATE_TIME_FORMATTER.format(zonedDateTime);
    }

    @Override
    public Date unmarshal(String date) throws Exception {
        final ZonedDateTime zonedDateTime = ZonedDateTime.parse(date, DATE_TIME_FORMATTER);
        return Date.from(zonedDateTime.toInstant());
    }

}
