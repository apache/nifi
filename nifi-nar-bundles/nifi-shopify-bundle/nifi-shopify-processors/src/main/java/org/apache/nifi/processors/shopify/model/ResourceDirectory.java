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
import java.util.List;

public class ResourceDirectory {

    public static final List<ShopifyResource> CUSTOMER_RESOURCES =
            Collections.singletonList(ShopifyResource.newInstance(
                    "customers",
                    "Customers",
                    "The Customer resource stores information about a shop's customers, such as their contact details,"
                            + " their order history, and whether they've agreed to receive email marketing.",
                    IncrementalLoadingParameter.UPDATED_AT
            ));
    public static final List<ShopifyResource> DISCOUNT_RESOURCES =
            Collections.singletonList(ShopifyResource.newInstance(
                    "price_rules",
                    "Price Rules",
                    "The PriceRule resource can be used to get discounts using conditions",
                    IncrementalLoadingParameter.UPDATED_AT
            ));
    public static final List<ShopifyResource> INVENTORY_RESOURCES =
            Collections.singletonList(ShopifyResource.newInstance(
                    "locations",
                    "Locations",
                    "A location represents a geographical location where your stores, pop-up stores, headquarters and warehouses exist.",
                    IncrementalLoadingParameter.NONE
            ));
    public static final List<ShopifyResource> ONLINE_STORE_RESOURCES =
            Collections.unmodifiableList(Arrays.asList(ShopifyResource.newInstance(
                    "blogs",
                    "Blogs",
                    "Shopify shops come with a built-in blogging engine, allowing a shop to have one or more blogs.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "comments",
                    "Comments",
                    "A comment is a reader's response to an article in a blog.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "pages",
                    "Pages",
                    "Shopify stores come with a tool for creating basic HTML web pages.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "redirects",
                    "Redirects",
                    "A redirect causes a visitor on a specific path on the shop's site to be automatically sent to a different location.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "script_tags",
                    "Script Tags",
                    "The ScriptTag resource represents remote JavaScript code that is loaded into the pages of a shop's storefront or the order status page of checkout.",
                    IncrementalLoadingParameter.UPDATED_AT
            ), ShopifyResource.newInstance(
                    "themes",
                    "Themes",
                    "A theme controls the look and feel of a Shopify online store.",
                    IncrementalLoadingParameter.NONE
            )));
    public static final List<ShopifyResource> ORDER_RESOURCES =
            Collections.unmodifiableList(Arrays.asList(ShopifyResource.newInstance(
                    "checkouts",
                    "Abandoned Checkouts",
                    "A checkout is considered abandoned after the customer has added contact information, but before the customer has completed their purchase.",
                    IncrementalLoadingParameter.UPDATED_AT
            ), ShopifyResource.newInstance(
                    "draft_orders",
                    "Draft Orders",
                    "Merchants can use draft orders to create orders on behalf of their customers.",
                    IncrementalLoadingParameter.UPDATED_AT
            ), ShopifyResource.newInstance(
                    "orders",
                    "Orders",
                    "An order is a customer's request to purchase one or more products from a shop.",
                    IncrementalLoadingParameter.UPDATED_AT
            )));
    public static final List<ShopifyResource> PRODUCT_RESOURCES =
            Collections.unmodifiableList(Arrays.asList(ShopifyResource.newInstance(
                    "collects",
                    "Collects",
                    "Collects are meant for managing the relationship between products and custom collections.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "custom_collections",
                    "Custom Collections",
                    "A custom collection is a grouping of products that a merchant can create to make their store easier to browse. ",
                    IncrementalLoadingParameter.UPDATED_AT
            ), ShopifyResource.newInstance(
                    "products",
                    "Products",
                    "Get products in a merchant's store ",
                    IncrementalLoadingParameter.UPDATED_AT
            ), ShopifyResource.newInstance(
                    "smart_collections",
                    "Smart Collections",
                    "A smart collection is a grouping of products defined by rules that are set by the merchant.",
                    IncrementalLoadingParameter.UPDATED_AT
            )));
    public static final List<ShopifyResource> SALES_CHANNEL_RESOURCES =
            Collections.unmodifiableList(Arrays.asList(ShopifyResource.newInstance(
                    "collection_listings",
                    "Collection Listings",
                    "A CollectionListing resource represents a product collection that a merchant has made available to your sales channel.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "product_listings",
                    "Product Listings",
                    "A ProductListing resource represents a Product which is available to your sales channel.",
                    IncrementalLoadingParameter.UPDATED_AT
            )));
    public static final List<ShopifyResource> STORE_PROPERTY_RESOURCES =
            Collections.unmodifiableList(Arrays.asList(ShopifyResource.newInstance(
                    "countries",
                    "Countries",
                    "The Country resource represents the tax rates applied to orders from the different countries where a shop sells its products.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "currencies",
                    "Currencies",
                    "Merchants who use Shopify Payments can allow customers to pay in their local currency on the online store.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "policies",
                    "Policies",
                    "Policy resource can be used to access the policies that a merchant has configured for their shop, such as their refund and privacy policies.",
                    IncrementalLoadingParameter.NONE
            ), ShopifyResource.newInstance(
                    "shipping_zones",
                    "Shipping Zones",
                    "ShippingZone resource can be used to view shipping zones and their countries, provinces, and shipping rates.",
                    IncrementalLoadingParameter.UPDATED_AT
            )));
}
