/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.azure.cosmos.document;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureCosmosDBUtils {
    public static final String CONSISTENCY_STRONG = "STRONG";
    public static final String CONSISTENCY_BOUNDED_STALENESS= "BOUNDED_STALENESS";
    public static final String CONSISTENCY_SESSION = "SESSION";
    public static final String CONSISTENCY_CONSISTENT_PREFIX = "CONSISTENT_PREFIX";
    public static final String CONSISTENCY_EVENTUAL = "EVENTUAL";

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-uri")
        .displayName("Azure Cosmos DB URI")
        .description("Azure Cosmos DB URI, typically in the form of https://{databaseaccount}.documents.azure.com:443/."
            + " Note this host URL is for Azure Cosmos DB with CORE SQL API"
            + " from Azure Portal (Overview->URI)" )
        .required(false)
        .addValidator(StandardValidators.URI_VALIDATOR)
        .build();

    public static final PropertyDescriptor DB_ACCESS_KEY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-db-key")
        .displayName("Azure Cosmos DB Access Key")
        .description("Azure Cosmos DB Access Key from Azure Portal (Settings->Keys)")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor CONSISTENCY = new PropertyDescriptor.Builder()
        .name("azure-cosmos-consistency-level")
        .displayName("Azure Cosmos DB Consistency Level")
        .description("Choose from five well-defined consistency levels on the consistency spectrum. "
            + "Each level prvoides availability and performance tradeoffs and are backed by SLAs. "
            + "The linearizability of the strong consistency model adds a price of higher write latency (in steady state) "
            + "and reduced availability (during failures). "
            + "On the other hand, eventual consistency offers higher availability and better performance, "
            + "but makes it hard to program applications.")
        .required(false)
        .defaultValue(CONSISTENCY_SESSION)
        .allowableValues(CONSISTENCY_STRONG, CONSISTENCY_BOUNDED_STALENESS, CONSISTENCY_SESSION,
                CONSISTENCY_CONSISTENT_PREFIX, CONSISTENCY_EVENTUAL)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
}