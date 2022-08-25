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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.components.AllowableValue;

public class ResourceDirectory {

    private ResourceDirectory() {
    }

    private static final Map<ResourceType, List<ShopifyResource>> resourceMap;

    static {
        resourceMap = new EnumMap<>(ResourceType.class);
        resourceMap.put(ResourceType.CUSTOMERS, getCustomerResources());
        resourceMap.put(ResourceType.DISCOUNTS, getDiscountResources());
        resourceMap.put(ResourceType.INVENTORY, getInventoryResources());
        resourceMap.put(ResourceType.ONLINE_STORE, getOnlineStoreResources());
        resourceMap.put(ResourceType.ORDERS, getOrderResources());
        resourceMap.put(ResourceType.PRODUCT, getProductResources());
        resourceMap.put(ResourceType.SALES_CHANNELS, getSalesChannelResources());
        resourceMap.put(ResourceType.STORE_PROPERTIES, getStorePropertyResources());
    }

    private static List<ShopifyResource> getCustomerResources() {
        final ShopifyResource customer = ShopifyResource.newInstance(
                "customers",
                "Customers",
                "The Customer resource stores information about a shop's customers, such as their contact details," +
                        " their order history, and whether they've agreed to receive email marketing.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource customerSavedSearch = ShopifyResource.newInstance(
                "customer_saved_searches",
                "Customer Saved Searches",
                "A customer saved search is a search query that represents a group of customers defined by the shop owner.",
                IncrementalLoadingParameter.NONE
        );
        return Collections.unmodifiableList(Arrays.asList(customer, customerSavedSearch));
    }

    private static List<ShopifyResource> getDiscountResources() {
        final ShopifyResource priceRule = ShopifyResource.newInstance(
                "price_rules",
                "Price Rules",
                "The PriceRule resource can be used to get discounts using conditions",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        return Collections.singletonList(priceRule);
    }

    private static List<ShopifyResource> getInventoryResources() {
        final ShopifyResource location = ShopifyResource.newInstance(
                "locations",
                "Locations",
                "A location represents a geographical location where your stores, pop-up stores, headquarters, and warehouses exist.",
                IncrementalLoadingParameter.NONE
        );
        return Collections.singletonList(location);
    }

    private static List<ShopifyResource> getOnlineStoreResources() {
        final ShopifyResource blog = ShopifyResource.newInstance(
                "blogs",
                "Blogs",
                "Shopify shops come with a built-in blogging engine, allowing a shop to have one or more blogs.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource comment = ShopifyResource.newInstance(
                "comments",
                "Comments",
                "A comment is a reader's response to an article in a blog.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource page = ShopifyResource.newInstance(
                "pages",
                "Pages",
                "Shopify stores come with a tool for creating basic HTML web pages.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource redirect = ShopifyResource.newInstance(
                "redirects",
                "Redirects",
                "A redirect causes a visitor on a specific path on the shop's site to be automatically sent to a different location.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource scriptTag = ShopifyResource.newInstance(
                "script_tags",
                "Script Tags",
                "The ScriptTag resource represents remote JavaScript code that is loaded into the pages of a shop's storefront or the order status page of checkout.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource theme = ShopifyResource.newInstance(
                "themes",
                "Themes",
                "A theme controls the look and feel of a Shopify online store.",
                IncrementalLoadingParameter.NONE
        );
        return Collections.unmodifiableList(Arrays.asList(blog, comment, page, redirect, scriptTag, theme));
    }

    private static List<ShopifyResource> getOrderResources() {
        final ShopifyResource abandonedCheckouts = ShopifyResource.newInstance(
                "checkouts",
                "Abandoned Checkouts",
                "A checkout is considered abandoned after the customer has added contact information, but before the customer has completed their purchase.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource draftOrders = ShopifyResource.newInstance(
                "draft_orders",
                "Draft Orders",
                "Merchants can use draft orders to create orders on behalf of their customers.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource orders = ShopifyResource.newInstance(
                "orders",
                "Orders",
                "An order is a customer's request to purchase one or more products from a shop.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        return Collections.unmodifiableList(Arrays.asList(abandonedCheckouts, draftOrders, orders));
    }

    private static List<ShopifyResource> getProductResources() {
        final ShopifyResource collect = ShopifyResource.newInstance(
                "collects",
                "Collects",
                "Collects are meant for managing the relationship between products and custom collections.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource customCollection = ShopifyResource.newInstance(
                "custom_collections",
                "Custom Collections",
                "A custom collection is a grouping of products that a merchant can create to make their store easier to browse. ",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource product = ShopifyResource.newInstance(
                "products",
                "Products",
                "Get products in a merchant's store ",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource smartCollection = ShopifyResource.newInstance(
                "smart_collections",
                "Smart Collections",
                "A smart collection is a grouping of products defined by rules that are set by the merchant.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        return Collections.unmodifiableList(Arrays.asList(collect, customCollection, product, smartCollection));
    }

    private static List<ShopifyResource> getSalesChannelResources() {
        final ShopifyResource collectionListing = ShopifyResource.newInstance(
                "collection_listings",
                "Collection Listings",
                "A CollectionListing resource represents a product collection that a merchant has made available to your sales channel.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource productListing = ShopifyResource.newInstance(
                "product_listings",
                "Product Listings",
                "A ProductListing resource represents a Product which is available to your sales channel.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        return Collections.unmodifiableList(Arrays.asList(collectionListing, productListing));
    }

    private static List<ShopifyResource> getStorePropertyResources() {
        final ShopifyResource country = ShopifyResource.newInstance(
                "countries",
                "Countries",
                "The Country resource represents the tax rates applied to orders from the different countries where a shop sells its products.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource currency = ShopifyResource.newInstance(
                "currencies",
                "Currencies",
                "Merchants who use Shopify Payments can allow customers to pay in their local currency on the online store.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource policy = ShopifyResource.newInstance(
                "policies",
                "Policies",
                "Policy resource can be used to access the policies that a merchant has configured for their shop, such as their refund and privacy policies.",
                IncrementalLoadingParameter.NONE
        );
        final ShopifyResource shippingZone = ShopifyResource.newInstance(
                "shipping_zones",
                "Shipping Zones",
                "ShippingZone resource can be used to view shipping zones and their countries, provinces, and shipping rates.",
                IncrementalLoadingParameter.UPDATED_AT_MIN
        );
        final ShopifyResource shop = ShopifyResource.newInstance(
                "shop",
                "Shop",
                "The Shop resource is a collection of general business and store management settings and information about the store.",
                IncrementalLoadingParameter.NONE
        );
        return Collections.unmodifiableList(Arrays.asList(country, currency, policy, shippingZone, shop));
    }

    public static AllowableValue[] getCategories() {
        return resourceMap.keySet().stream().map(ResourceType::getAllowableValue).toArray(AllowableValue[]::new);
    }

    public static List<ShopifyResource> getResources(final ResourceType key) {
        return resourceMap.get(key);
    }

    public static AllowableValue[] getResourcesAsAllowableValues(final ResourceType key) {
        return getResources(key).stream().map(ShopifyResource::getAllowableValue).toArray(AllowableValue[]::new);
    }

    public static ShopifyResource getResourceTypeDto(final ResourceType key, final String value) {
        return getResources(key).stream()
                .filter(s -> s.getValue().equals(value))
                .findFirst()
                .get();
    }
}
