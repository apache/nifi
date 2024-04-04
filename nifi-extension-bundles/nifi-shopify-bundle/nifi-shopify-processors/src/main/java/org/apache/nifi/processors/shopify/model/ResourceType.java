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

import java.util.List;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

public enum ResourceType implements DescribedValue {

    CUSTOMERS(
            "Customers",
            "Query a Customer resource",
            "Customer Category",
            "Customer resource to query",
            ResourceDirectory.CUSTOMER_RESOURCES
    ),
    DISCOUNTS(
            "Discounts",
            "Query a Discount resource",
            "Discount Category",
            "Discount resource to query",
            ResourceDirectory.DISCOUNT_RESOURCES
    ),
    INVENTORY(
            "Inventory",
            "Query an Inventory resource",
            "Inventory Category",
            "Inventory resource to query",
            ResourceDirectory.INVENTORY_RESOURCES
    ),
    ONLINE_STORE(
            "Online Store",
            "Query an Online Store resource",
            "Online Store Category",
            "Online Store resource to query",
            ResourceDirectory.ONLINE_STORE_RESOURCES
    ),
    ORDERS(
            "Orders",
            "Query an Order resource",
            "Order Category",
            "Order resource to query",
            ResourceDirectory.ORDER_RESOURCES
    ),
    PRODUCT(
            "Products",
            "Query a Product resource",
            "Product Category",
            "Product resource to query",
            ResourceDirectory.PRODUCT_RESOURCES
    ),
    SALES_CHANNELS(
            "Sales Channels",
            "Query a Sales Channel resource",
            "Sales Channel Category",
            "Sales Channel resource to query",
            ResourceDirectory.SALES_CHANNEL_RESOURCES
    ),
    STORE_PROPERTIES(
            "Store Properties",
            "Query a Store Property resource",
            "Store Property Category",
            "Store Property resource to query",
            ResourceDirectory.STORE_PROPERTY_RESOURCES
    );

    private final String allowableValueDisplayName;
    private final String allowableValueDescription;
    private final String propertyDisplayName;
    private final String propertyDescription;
    private final List<ShopifyResource> resources;

    ResourceType(
            final String allowableValueDisplayName,
            final String allowableValueDescription,
            String propertyDisplayName, String propertyDescription, List<ShopifyResource> resources
    ) {
        this.allowableValueDisplayName = allowableValueDisplayName;
        this.allowableValueDescription = allowableValueDescription;
        this.propertyDisplayName = propertyDisplayName;
        this.propertyDescription = propertyDescription;
        this.resources = resources;
    }
    @Override
    public String getValue() {
        return name();
    }
    @Override
    public String getDisplayName() {
        return allowableValueDisplayName;
    }
    @Override
    public String getDescription() {
        return allowableValueDescription;
    }
    public String getPropertyDescription() {
        return propertyDescription;
    }
    public String getPropertyDisplayName() {
        return propertyDisplayName;
    }
    public List<ShopifyResource> getResources() {
        return resources;
    }
    public ShopifyResource getResource(final String value) {
        return getResources().stream()
                .filter(s -> s.getValue().equals(value))
                .findFirst()
                .get();
    }
    public AllowableValue[] getResourcesAsAllowableValues() {
        return getResources().stream().map(ShopifyResource::getAllowableValue).toArray(AllowableValue[]::new);
    }
}
