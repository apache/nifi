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
package org.apache.nifi.schema.inference;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.text.DateTimeMatcher;

import java.util.Optional;

public class TimeValueInference {
    private final Optional<DataType> dateDataType;
    private final Optional<DataType> timeDataType;
    private final Optional<DataType> timestampDataType;

    private final DateTimeMatcher dateMatcher;
    private final DateTimeMatcher timeMatcher;
    private final DateTimeMatcher timestampMatcher;

    public TimeValueInference(final String dateFormat, final String timeFormat, final String timestampFormat) {
        this.dateDataType = Optional.of(RecordFieldType.DATE.getDataType(dateFormat));
        this.timeDataType = Optional.of(RecordFieldType.TIME.getDataType(timeFormat));
        this.timestampDataType = Optional.of(RecordFieldType.TIMESTAMP.getDataType(timestampFormat));

        this.dateMatcher = DateTimeMatcher.compile(dateFormat);
        this.timeMatcher = DateTimeMatcher.compile(timeFormat);
        this.timestampMatcher = DateTimeMatcher.compile(timestampFormat);
    }

    public String getDateFormat() {
        return dateDataType.map(DataType::getFormat).orElse(null);
    }

    public String getTimeFormat() {
        return timeDataType.map(DataType::getFormat).orElse(null);
    }

    public String getTimestampFormat() {
        return timestampDataType.map(DataType::getFormat).orElse(null);
    }

    public Optional<DataType> getDataType(final String value) {
        if (timestampMatcher.matches(value)) {
            return timestampDataType;
        }
        if (dateMatcher.matches(value)) {
            return dateDataType;
        }
        if (timeMatcher.matches(value)) {
            return timeDataType;
        }

        return Optional.empty();
    }
}
