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
package org.apache.nifi.processors.hubspot;

import org.apache.nifi.components.DescribedValue;

import static org.apache.nifi.processors.hubspot.IncrementalFieldType.HS_LAST_MODIFIED_DATE;
import static org.apache.nifi.processors.hubspot.IncrementalFieldType.LAST_MODIFIED_DATE;

public enum HubSpotObjectType implements DescribedValue {

    COMPANIES(
            "/crm/v3/objects/companies",
            "Companies",
            "In HubSpot, the companies object is a standard CRM object. Individual company records can be used to store information about businesses" +
                    " and organizations within company properties.",
            HS_LAST_MODIFIED_DATE
    ),
    CONTACTS(
            "/crm/v3/objects/contacts",
            "Contacts",
            "In HubSpot, contacts store information about individuals. From marketing automation to smart content, the lead-specific data found in" +
                    " contact records helps users leverage much of HubSpot's functionality.",
            LAST_MODIFIED_DATE
    ),
    DEALS(
            "/crm/v3/objects/deals",
            "Deals",
            "In HubSpot, a deal represents an ongoing transaction that a sales team is pursuing with a contact or company. It’s tracked through" +
                    " pipeline stages until won or lost.",
            HS_LAST_MODIFIED_DATE
    ),
    LINE_ITEMS(
            "/crm/v3/objects/line_items",
            "Line Items",
            "In HubSpot, line items can be thought of as a subset of products. When a product is attached to a deal, it becomes a line item. Line items can" +
                    " be created that are unique to an individual quote, but they will not be added to the product library.",
            HS_LAST_MODIFIED_DATE
    ),
    PRODUCTS(
            "/crm/v3/objects/products",
            "Products",
            "In HubSpot, products represent the goods or services to be sold. Building a product library allows the user to quickly add products to deals," +
                    " generate quotes, and report on product performance.",
            HS_LAST_MODIFIED_DATE
    ),
    TICKETS(
            "/crm/v3/objects/tickets",
            "Tickets",
            "In HubSpot, a ticket represents a customer request for help or support.",
            HS_LAST_MODIFIED_DATE
    ),
    QUOTES(
            "/crm/v3/objects/quotes",
            "Quotes",
            "In HubSpot, quotes are used to share pricing information with potential buyers.",
            HS_LAST_MODIFIED_DATE
    ),

    CALLS(
            "/crm/v3/objects/calls",
            "Calls",
            "Get calls on CRM records and on the calls index page.",
            HS_LAST_MODIFIED_DATE
    ),
    EMAILS(
            "/crm/v3/objects/emails",
            "Emails",
            "Get emails on CRM records.",
            HS_LAST_MODIFIED_DATE
    ),
    MEETINGS(
            "/crm/v3/objects/meetings",
            "Meetings",
            "Get meetings on CRM records.",
            HS_LAST_MODIFIED_DATE
    ),
    NOTES(
            "/crm/v3/objects/notes",
            "Notes",
            "Get notes on CRM records.",
            HS_LAST_MODIFIED_DATE
    ),
    TASKS(
            "/crm/v3/objects/tasks",
            "Tasks",
            "Get tasks on CRM records.",
            HS_LAST_MODIFIED_DATE
    );

    private final String value;
    private final String displayName;
    private final String description;
    private final IncrementalFieldType lastModifiedDateType;

    HubSpotObjectType(String value, String displayName, String description, IncrementalFieldType lastModifiedDateType) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
        this.lastModifiedDateType = lastModifiedDateType;
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

    public IncrementalFieldType getLastModifiedDateType() {
        return lastModifiedDateType;
    }
}
