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

package org.apache.nifi.kafka.connect.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.net.URL;

public class ConnectHttpUrlValidator implements ConfigDef.Validator {
    @Override
    public void ensureValid(final String name, final Object value) {
        if (value == null) {
            return;
        }

        if (!(value instanceof String)) {
            throw new ConfigException("Invalid value for property " + name + ": The configured value is expected to be a URL");
        }

        try {
            final URL url = new URL((String) value);
            final String protocol = url.getProtocol();
            if (!protocol.equals("http") && !protocol.equals("https")) {
                throw new ConfigException("Invalid value for property " + name + ": The value must be an http or https URL");
            }
        } catch (final Exception e) {
            throw new ConfigException("Invalid value for property " + name + ": The value is not a valid URL");
        }
    }
}
