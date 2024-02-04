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

package org.apache.nifi.processors.zendesk;

import org.apache.nifi.components.DescribedValue;

import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.apache.nifi.processors.zendesk.ZendeskExportMethod.CURSOR;
import static org.apache.nifi.processors.zendesk.ZendeskExportMethod.TIME;

public enum ZendeskResource implements DescribedValue {
    TICKETS("/api/v2/incremental/tickets", "Tickets", "tickets", unmodifiableList(asList(TIME, CURSOR)),
        "Tickets are the means through which end users (customers) communicate with agents in Zendesk Support."),
    TICKET_EVENTS("/api/v2/incremental/ticket_events", "Ticket Events", "ticket_events", unmodifiableList(asList(TIME)),
        "Stream of changes that occurred on tickets. Each event is tied to an update on a ticket and contains all the fields that were updated in that change."),
    TICKET_METRIC_EVENTS("/api/v2/incremental/ticket_metric_events", "Ticket Metric Events", "ticket_metric_events", unmodifiableList(asList(TIME)),
        "Ticket metric events API can be used to track reply times, agent work times, and requester wait times."),
    USERS("/api/v2/incremental/users", "Users", "users", unmodifiableList(asList(TIME, CURSOR)),
        "Zendesk Support has three types of users: end users (customers), agents, and administrators."),
    ORGANIZATIONS("/api/v2/incremental/organizations", "Organizations", "organizations", unmodifiableList(asList(TIME)),
        "Just as agents can be segmented into groups in Zendesk Support, customers (end-users) can be segmented into organizations."),
    ARTICLES("/api/v2/help_center/incremental/articles", "Articles", "articles", unmodifiableList(asList(TIME)),
        "Articles are content items such as help topics or tech notes contained in sections."),
    NPS_RESPONSES("/api/v2/nps/incremental/responses", "NPS - Responses", "responses", unmodifiableList(asList(TIME)),
        "When a recipient responds to an NPS survey, their rating, comment, and last survey date are captured."),
    NPS_RECIPIENTS("/api/v2/nps/incremental/recipients", "NPS - Recipients", "recipients", unmodifiableList(asList(TIME)),
        "Every NPS survey is delivered to one or multiple recipients. For most businesses that use Zendesk Support, the recipients are customers. Agents and admins will never receive surveys.");

    private final String value;
    private final String displayName;
    private final String responseFieldName;
    private final List<ZendeskExportMethod> supportedExportMethods;
    private final String description;

    ZendeskResource(String value, String displayName, String responseFieldName, List<ZendeskExportMethod> supportedExportMethods, String description) {
        this.value = value;
        this.displayName = displayName;
        this.responseFieldName = responseFieldName;
        this.supportedExportMethods = supportedExportMethods;
        this.description = description;
    }

    public static ZendeskResource forName(String resourceName) {
        return Stream.of(values()).filter(r -> r.getValue().equalsIgnoreCase(resourceName)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Invalid Zendesk resource: " + resourceName));
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

    public String getResponseFieldName() {
        return responseFieldName;
    }

    public List<ZendeskExportMethod> getSupportedExportMethods() {
        return supportedExportMethods;
    }

    public boolean supportsExportMethod(ZendeskExportMethod exportMethod) {
        return supportedExportMethods.contains(exportMethod);
    }

    public String apiPath(ZendeskExportMethod exportMethod) {
        return format(exportMethod.getExportApiPathTemplate(), value);
    }
}
