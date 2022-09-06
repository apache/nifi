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

public enum HubSpotObjectType implements DescribedValue {

    COMPANIES(
            "/crm/v3/objects/companies",
            "Companies",
            "In HubSpot, the companies object is a standard CRM object. Individual company records can be used to store information about businesses" +
                    " and organizations within company properties."
    ),
    CONTACTS(
            "/crm/v3/objects/contacts",
            "Contacts",
            "In HubSpot, contacts store information about individuals. From marketing automation to smart content, the lead-specific data found in" +
                    " contact records helps users leverage much of HubSpot's functionality."
    ),
    DEALS(
            "/crm/v3/objects/deals",
            "Deals",
            "In HubSpot, a deal represents an ongoing transaction that a sales team is pursuing with a contact or company. It’s tracked through" +
                    " pipeline stages until won or lost."
    ),
    FEEDBACK_SUBMISSIONS(
            "/crm/v3/objects/feedback_submissions",
            "Feedback Submissions",
            "In HubSpot, feedback submissions are an object which stores information submitted to a feedback survey. This includes Net Promoter Score (NPS)," +
                    " Customer Satisfaction (CSAT), Customer Effort Score (CES) and Custom Surveys."
    ),
    LINE_ITEMS(
            "/crm/v3/objects/line_items",
            "Line Items",
            "In HubSpot, line items can be thought of as a subset of products. When a product is attached to a deal, it becomes a line item. Line items can" +
                    " be created that are unique to an individual quote, but they will not be added to the product library."
    ),
    PRODUCTS(
            "/crm/v3/objects/products",
            "Products",
            "In HubSpot, products represent the goods or services to be sold. Building a product library allows the user to quickly add products to deals," +
                    " generate quotes, and report on product performance."
    ),
    TICKETS(
            "/crm/v3/objects/tickets",
            "Tickets",
            "In HubSpot, a ticket represents a customer request for help or support."
    ),
    QUOTES(
            "/crm/v3/objects/quotes",
            "Quotes",
            "In HubSpot, quotes are used to share pricing information with potential buyers."
    ),

    CALLS(
            "/crm/v3/objects/calls",
            "Calls",
            "Get calls on CRM records and on the calls index page."
    ),
    EMAILS(
            "/crm/v3/objects/emails",
            "Emails",
            "Get emails on CRM records."
    ),
    MEETINGS(
            "/crm/v3/objects/meetings",
            "Meetings",
            "Get meetings on CRM records."
    ),
    NOTES(
            "/crm/v3/objects/notes",
            "Notes",
            "Get notes on CRM records."
    ),
    TASKS(
            "/crm/v3/objects/tasks",
            "Tasks",
            "Get tasks on CRM records."
    ),

    OWNERS(
            "/crm/v3/owners/",
            "Owners",
            "HubSpot uses owners to assign specific users to contacts, companies, deals, tickets, or engagements. Any HubSpot user with access to contacts" +
                    " can be assigned as an owner, and multiple owners can be assigned to an object by creating a custom property for this purpose."
    );


    private final String value;
    private final String displayName;
    private final String description;

    HubSpotObjectType(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
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
}
