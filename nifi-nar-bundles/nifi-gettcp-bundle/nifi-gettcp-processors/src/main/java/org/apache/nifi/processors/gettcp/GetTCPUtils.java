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
package org.apache.nifi.processors.gettcp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

class GetTCPUtils {

    private static final Pattern validIpAddressRegex = Pattern.compile(
            "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");

    private static final Pattern validHostnameRegex = Pattern.compile(
            "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$");

    private static final Pattern looksLikeIpRegex = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)$");

    public static final Validator ENDPOINT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (null == value || value.isEmpty()) {
                return new ValidationResult.Builder().subject(subject).input(value).valid(false)
                        .explanation(subject + " cannot be empty").build();
            }
            // The format should be <host>:<port>{,<host>:<port>}
            // first split on ,
            final String[] hostPortPairs = value.split(",");
            boolean validHostPortPairs = true;
            String reason = "";
            String offendingSubject = subject;

            if (0 == hostPortPairs.length) {
                return new ValidationResult.Builder().subject(subject).input(value).valid(false)
                        .explanation(offendingSubject + " cannot be empty").build();
            }

            for (int i = 0; i < hostPortPairs.length && validHostPortPairs; i++) {
                String[] parts = hostPortPairs[i].split(":");

                if (parts.length != 2) {
                    validHostPortPairs = false;
                    reason = " of malformed URL '" + hostPortPairs[i] + "'";
                } else {
                    Matcher validHost = validHostnameRegex.matcher(parts[0]);
                    Matcher validIp = validIpAddressRegex.matcher(parts[0]);
                    Matcher looksLikeValidIp = looksLikeIpRegex.matcher(parts[0]);
                    if (!validHost.find()) {
                        validHostPortPairs = false;
                        reason = " it contains invalid characters '" + parts[0] + "'";
                    } else if (looksLikeValidIp.find() && !validIp.find()) {
                        validHostPortPairs = false;
                        reason = " it appears to be represented as an IP address which is out of legal range '" + parts[0] + "'";
                    }
                    ValidationResult result = StandardValidators.PORT_VALIDATOR.validate(parts[1], parts[1], context);
                    if (!result.isValid()) {
                        validHostPortPairs = false;
                        reason = result.getExplanation();
                    }
                }
            }

            return new ValidationResult.Builder().subject(offendingSubject).input(value).explanation(reason)
                    .valid(validHostPortPairs).build();
        }
    };
}
