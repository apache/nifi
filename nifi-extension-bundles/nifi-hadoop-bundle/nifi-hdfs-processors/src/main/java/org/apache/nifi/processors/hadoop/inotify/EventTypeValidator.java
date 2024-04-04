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
package org.apache.nifi.processors.hadoop.inotify;

import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.ArrayList;
import java.util.List;

class EventTypeValidator implements Validator {

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        final String explanation = isValidEventType(input);
        return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .valid(explanation == null)
                .explanation(explanation)
                .build();
    }

    private String isValidEventType(String input) {
        if (input != null && !"".equals(input.trim())) {
            final String[] events = input.split(",");
            final List<String> invalid = new ArrayList<>();
            for (String event : events) {
                try {
                    Event.EventType.valueOf(event.trim().toUpperCase());
                } catch (IllegalArgumentException e) {
                    invalid.add(event.trim());
                }
            }

            return invalid.isEmpty() ? null : "The following are not valid event types: " + invalid;
        }

        return "Empty event types are not allowed.";
    }
}
