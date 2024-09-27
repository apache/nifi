<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# GetShopify

## Setting Up a Custom App

Follow the [Shopify tutorial](https://help.shopify.com/en/manual/apps/custom-apps) to enable and create private apps,
set API Scopes and generate API tokens.

## Incremental Loading

Some resources can be processed incrementally by NiFi. This means that only resources created or modified after the last
run time of the processor are displayed. The processor state can be reset in the context menu. The following list shows
which date-time fields are incremented for which resources.

* Customers
    * Customers: updated\_at\_min
* Discounts
    * Price Rules: updated\_at\_min
* Inventory
    * Inventory Levels: updated\_at\_min
* Online Store
    * Script Tags: updated\_at\_min
* Orders
    * Abandoned Checkouts: updated\_at\_min
    * Draft Orders: updated\_at\_min
    * Orders: updated\_at\_min
* Product
    * Custom Collections: updated\_at\_min
    * Products: updated\_at\_min
    * Smart Collections: updated\_at\_min
* Sales Channels
    * Product Listings: updated\_at\_min
* Store Properties
    * Shipping Zones: updated\_at\_min