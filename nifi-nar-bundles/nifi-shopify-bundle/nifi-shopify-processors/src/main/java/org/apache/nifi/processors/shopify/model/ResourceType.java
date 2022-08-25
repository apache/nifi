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

public enum ResourceType implements DescribedValue {

    CUSTOMERS("Customers", "Customer resources to query"),
    DISCOUNTS("Discounts", "Discount resources to query"),
    INVENTORY("Inventory", "Inventory resources to query"),
    ONLINE_STORE("Online Store", "Online Store resources to query"),
    ORDERS("Orders", "Order resources to query"),
    PRODUCT("Products", "Product resources to query"),
    SALES_CHANNELS("Sales Channels", "Sales Channel resources to query"),
    STORE_PROPERTIES("Store Properties", "Store Property resources to query");

    private final String displayName;
    private final String description;

    ResourceType(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public AllowableValue getAllowableValue() {
        return new AllowableValue(name(), displayName, description);
    }
}
