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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * XmlAdapter for (un)marshalling a date/time.
 */
public class TimestampAdapter extends XmlAdapter<String, Date> {

    public static final String DEFAULT_DATE_TIME_FORMAT = "MM/dd/yyyy HH:mm:ss.SSS z";

    @Override
    public String marshal(Date date) throws Exception {
        final SimpleDateFormat formatter = new SimpleDateFormat(DEFAULT_DATE_TIME_FORMAT, Locale.US);
        formatter.setTimeZone(TimeZone.getDefault());
        return formatter.format(date);
    }

    @Override
    public Date unmarshal(String date) throws Exception {
        final SimpleDateFormat parser = new SimpleDateFormat(DEFAULT_DATE_TIME_FORMAT, Locale.US);
        parser.setTimeZone(TimeZone.getDefault());
        return parser.parse(date);
    }

}
