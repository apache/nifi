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
package org.apache.nifi.vault.hashicorp.config;

import org.apache.nifi.vault.hashicorp.config.lookup.BeanPropertyLookup;
import org.apache.nifi.vault.hashicorp.config.lookup.PropertyLookup;
import org.springframework.core.env.PropertySource;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashiCorpVaultPropertySource extends PropertySource<HashiCorpVaultProperties> {
    private static final String PREFIX = "vault";
    private static final Pattern DASH_LETTER_PATTERN = Pattern.compile("-[a-z]");

    private PropertyLookup propertyLookup;

    public HashiCorpVaultPropertySource(final HashiCorpVaultProperties source) {
        super(HashiCorpVaultPropertySource.class.getName(), source);

        propertyLookup = new BeanPropertyLookup(PREFIX, HashiCorpVaultProperties.class);
    }

    @Override
    public Object getProperty(final String key) {
        Objects.requireNonNull(key, "Property key cannot be null");

        return propertyLookup.getPropertyValue(getPropertyKey(key), getSource());
    }

    /**
     * Converts key names from format test-value to testValue
     * @param springPropertyKey A Spring Vault property key
     * @return
     */
    private String getPropertyKey(String springPropertyKey) {
        final Matcher m = DASH_LETTER_PATTERN.matcher(springPropertyKey);
        final StringBuffer result = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(result, m.group(0).substring(1).toUpperCase());
        }
        m.appendTail(result);
        return result.toString();
    }
}
