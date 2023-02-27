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
package org.apache.nifi.processors.salesforce.util;

import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.salesforce.QuerySalesforceObject;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class IncrementalContext {

    private final String ageField;
    private final String initialAgeFilter;
    private final String ageFilterUpper;
    private final String ageFilterLower;

    public IncrementalContext(ProcessContext context, StateMap state) {
        ageField = context.getProperty(QuerySalesforceObject.AGE_FIELD).getValue();
        initialAgeFilter = context.getProperty(QuerySalesforceObject.INITIAL_AGE_FILTER).getValue();
        ageFilterLower = state.get(QuerySalesforceObject.LAST_AGE_FILTER);
        Optional<Long> ageDelayMs = Optional.ofNullable(context.getProperty(QuerySalesforceObject.AGE_DELAY).asTimePeriod(TimeUnit.MILLISECONDS));

        if (ageField == null) {
            ageFilterUpper = null;
        } else {
            OffsetDateTime ageFilterUpperTime = ageDelayMs
                    .map(delay -> OffsetDateTime.now().minus(delay, ChronoUnit.MILLIS))
                    .orElse(OffsetDateTime.now());
            ageFilterUpper = ageFilterUpperTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
    }

    public String getAgeField() {
        return ageField;
    }

    public String getInitialAgeFilter() {
        return initialAgeFilter;
    }

    public String getAgeFilterUpper() {
        return ageFilterUpper;
    }

    public String getAgeFilterLower() {
        return ageFilterLower;
    }
}
