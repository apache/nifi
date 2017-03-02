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
package org.apache.nifi.web.api.request;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Class for parsing integer parameters and providing a user friendly error message.
 */
public class DateTimeParameter {

    private static final String DATE_TIME_FORMAT = "MM/dd/yyyy HH:mm:ss";
    private static final String INVALID_INTEGER_MESSAGE = "Unable to parse '%s' as a date/time. Expected format '%s'.";

    private Date dateTimeValue;

    public DateTimeParameter(String rawDateTime) {
        try {
            dateTimeValue = parse(rawDateTime);
        } catch (ParseException pe) {
            throw new IllegalArgumentException(String.format(INVALID_INTEGER_MESSAGE,
                    rawDateTime, DATE_TIME_FORMAT));
        }
    }

    public Date getDateTime() {
        return dateTimeValue;
    }

    public static String format(final Date date) {
        SimpleDateFormat parser = new SimpleDateFormat(DATE_TIME_FORMAT, Locale.US);
        parser.setLenient(false);
        return parser.format(date);
    }

    public static String format(final DateTimeParameter param) {
        return format(param.getDateTime());
    }

    public static Date parse(final String str) throws ParseException {
        SimpleDateFormat parser = new SimpleDateFormat(DATE_TIME_FORMAT, Locale.US);
        parser.setLenient(false);
        return parser.parse(str);
    }
}
