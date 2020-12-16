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

package org.apache.nifi.processor.util.list.validator;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.Optional;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static org.apache.nifi.processor.util.list.AbstractListProcessor.TIME_ADJUSTMENT;

public class TimeAdjustmentValidator implements Validator {
    private static final Pattern SIGNED_INTEGER_OR_SIGNED_HHmm_OR_HHmmss = Pattern.compile("-?(\\d{2}:\\d{2}(:\\d{2})?)|-?\\d+");

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        String evaluatedValue = Optional.ofNullable(context.getProperty(TIME_ADJUSTMENT).evaluateAttributeExpressions().getValue())
            .orElse("");

        boolean matches = evaluatedValue.equalsIgnoreCase("gmt")
            || !TimeZone.getTimeZone(evaluatedValue).getID().equals("GMT") // TimeZone.getTimeZone returns GMT for an unrecognized value
            || SIGNED_INTEGER_OR_SIGNED_HHmm_OR_HHmmss.matcher(evaluatedValue).matches();

        return new ValidationResult.Builder()
            .input(evaluatedValue)
            .subject(TIME_ADJUSTMENT.getDisplayName())
            .valid(matches)
            .explanation(matches ? null : "value is not a recognized as either a valid time zone or a numerical time value.")
            .build();
    }
}
