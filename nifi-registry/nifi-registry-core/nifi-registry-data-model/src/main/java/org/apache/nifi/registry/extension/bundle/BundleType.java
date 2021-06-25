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
package org.apache.nifi.registry.extension.bundle;

import io.swagger.annotations.ApiModel;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * The possible types of extension bundles.
 */
@ApiModel
@XmlJavaTypeAdapter(BundleTypeAdapter.class)
public enum BundleType {

    NIFI_NAR(BundleTypeValues.NIFI_NAR_VALUE),

    MINIFI_CPP(BundleTypeValues.MINIFI_CPP_VALUE);

    private final String displayName;

    BundleType(String displayName) {
        this.displayName = displayName;
    }

    // Note: This method must be name fromString for JAX-RS/Jersey to use it on query and path params
    public static BundleType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        for (final BundleType type : values()) {
            if (type.toString().equals(value)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown BundleType: " + value);
    }


    @Override
    public String toString() {
        return displayName;
    }

}
