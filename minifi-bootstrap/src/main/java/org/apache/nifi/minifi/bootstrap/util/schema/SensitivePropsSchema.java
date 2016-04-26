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
package org.apache.nifi.minifi.bootstrap.util.schema;

import org.apache.nifi.minifi.bootstrap.util.schema.common.BaseSchema;

import java.util.Map;

import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.SENSITIVE_PROPS_KEY;

/**
 *
 */
public class SensitivePropsSchema extends BaseSchema {
    public static final String SENSITIVE_PROPS_KEY_KEY = "key";
    public static final String SENSITIVE_PROPS_ALGORITHM_KEY = "algorithm";
    public static final String SENSITIVE_PROPS_PROVIDER_KEY = "provider";

    private String key = "";
    private String algorithm = "PBEWITHMD5AND256BITAES-CBC-OPENSSL";
    private String provider = "BC";

    public SensitivePropsSchema() {
    }

    public SensitivePropsSchema(Map map) {
        key = getOptionalKeyAsType(map, SENSITIVE_PROPS_KEY_KEY, String.class, SENSITIVE_PROPS_KEY, "");

        algorithm = getOptionalKeyAsType(map, SENSITIVE_PROPS_ALGORITHM_KEY, String.class, SENSITIVE_PROPS_KEY, "PBEWITHMD5AND256BITAES-CBC-OPENSSL");

        provider = getOptionalKeyAsType(map, SENSITIVE_PROPS_PROVIDER_KEY, String.class, SENSITIVE_PROPS_KEY, "BC");
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
