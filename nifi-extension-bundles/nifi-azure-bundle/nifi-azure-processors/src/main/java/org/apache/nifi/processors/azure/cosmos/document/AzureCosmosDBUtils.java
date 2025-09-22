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

import com.azure.cosmos.ConsistencyLevel;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureCosmosDBUtils {
    public static final String CONSISTENCY_STRONG = "STRONG";
    public static final String CONSISTENCY_BOUNDED_STALENESS = "BOUNDED_STALENESS";
    public static final String CONSISTENCY_SESSION = "SESSION";
    public static final String CONSISTENCY_CONSISTENT_PREFIX = "CONSISTENT_PREFIX";
    public static final String CONSISTENCY_EVENTUAL = "EVENTUAL";
    public static final String OLD_URI_DESCRIPTOR_NAME = "azure-cosmos-db-uri";
    public static final String OLD_DB_ACCESS_KEY_DESCRIPTOR_NAME = "azure-cosmos-db-key";
    public static final String OLD_CONSISTENCY_DESCRIPTOR_NAME = "azure-cosmos-db-consistency-level";

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("Cosmos DB URI")
        .description("Cosmos DB URI, typically in the form of https://{databaseaccount}.documents.azure.com:443/"
            + " Note this host URL is for Cosmos DB with Core SQL API"
            + " from Azure Portal (Overview->URI)")
        .required(false)
        .addValidator(StandardValidators.URI_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor DB_ACCESS_KEY = new PropertyDescriptor.Builder()
        .name("Cosmos DB Access Key")
        .description("Cosmos DB Access Key from Azure Portal (Settings->Keys). "
            + "Choose a read-write key to enable database or container creation at run time")
        .required(false)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .sensitive(true)
        .build();

    public static final PropertyDescriptor CONSISTENCY = new PropertyDescriptor.Builder()
        .name("Cosmos DB Consistency Level")
        .description("Choose from five consistency levels on the consistency spectrum. "
            + "Refer to Cosmos DB documentation for their differences")
        .required(false)
        .defaultValue(CONSISTENCY_SESSION)
        .allowableValues(CONSISTENCY_STRONG, CONSISTENCY_BOUNDED_STALENESS, CONSISTENCY_SESSION,
                CONSISTENCY_CONSISTENT_PREFIX, CONSISTENCY_EVENTUAL)
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .build();

    public static ConsistencyLevel determineConsistencyLevel(final String consistency) {
        return switch (consistency) {
            case CONSISTENCY_STRONG -> ConsistencyLevel.STRONG;
            case CONSISTENCY_CONSISTENT_PREFIX -> ConsistencyLevel.CONSISTENT_PREFIX;
            case CONSISTENCY_BOUNDED_STALENESS -> ConsistencyLevel.BOUNDED_STALENESS;
            case CONSISTENCY_EVENTUAL -> ConsistencyLevel.EVENTUAL;
            case CONSISTENCY_SESSION -> ConsistencyLevel.SESSION;
            default -> ConsistencyLevel.SESSION;
        };
    }
}
