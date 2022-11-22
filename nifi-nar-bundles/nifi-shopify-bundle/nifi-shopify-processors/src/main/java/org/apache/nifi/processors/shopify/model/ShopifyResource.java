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
package org.apache.nifi.processors.shopify.model;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

public class ShopifyResource implements DescribedValue, Resource {

    private final String value;
    private final String displayName;
    private final String description;
    private final IncrementalLoadingParameter incrementalLoadingParameter;

    public ShopifyResource(final String value, final String displayName, final String description,
                           final IncrementalLoadingParameter incrementalLoadingParameter) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
        this.incrementalLoadingParameter = incrementalLoadingParameter;
    }

    public static ShopifyResource newInstance(final String value, final String displayName, final String description,
            final IncrementalLoadingParameter incrementalLoadingParameter) {
        return new ShopifyResource(value, displayName, description, incrementalLoadingParameter);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public IncrementalLoadingParameter getIncrementalLoadingParameter() {
        return incrementalLoadingParameter;
    }

    @Override
    public AllowableValue getAllowableValue() {
        return new AllowableValue(value, displayName, description);
    }
}
