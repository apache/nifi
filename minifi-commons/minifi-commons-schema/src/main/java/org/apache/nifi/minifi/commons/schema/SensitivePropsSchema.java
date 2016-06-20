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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SENSITIVE_PROPS_KEY;

/**
 *
 */
public class SensitivePropsSchema extends BaseSchema {
    public static final String SENSITIVE_PROPS_KEY_KEY = "key";
    public static final String SENSITIVE_PROPS_ALGORITHM_KEY = "algorithm";
    public static final String SENSITIVE_PROPS_PROVIDER_KEY = "provider";

    public static final String DEFAULT_ALGORITHM = "PBEWITHMD5AND256BITAES-CBC-OPENSSL";
    public static final String DEFAULT_PROVIDER = "BC";

    private String key;
    private String algorithm = DEFAULT_ALGORITHM;
    private String provider = DEFAULT_PROVIDER;

    public SensitivePropsSchema() {
    }

    public SensitivePropsSchema(Map map) {
        key = getOptionalKeyAsType(map, SENSITIVE_PROPS_KEY_KEY, String.class, SENSITIVE_PROPS_KEY, "");
        algorithm = getOptionalKeyAsType(map, SENSITIVE_PROPS_ALGORITHM_KEY, String.class, SENSITIVE_PROPS_KEY, DEFAULT_ALGORITHM);
        provider = getOptionalKeyAsType(map, SENSITIVE_PROPS_PROVIDER_KEY, String.class, SENSITIVE_PROPS_KEY, DEFAULT_PROVIDER);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(SENSITIVE_PROPS_KEY_KEY, key);
        result.put(SENSITIVE_PROPS_ALGORITHM_KEY, algorithm);
        result.put(SENSITIVE_PROPS_PROVIDER_KEY, provider);
        return result;
    }

    public String getKey() {
        return key;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getProvider() {
        return provider;
    }
}
