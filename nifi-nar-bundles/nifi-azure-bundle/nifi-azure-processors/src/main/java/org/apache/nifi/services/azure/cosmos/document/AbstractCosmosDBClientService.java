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

package org.apache.nifi.services.azure.cosmos.document;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processors.azure.cosmos.document.AzureCosmosDBUtils;
import org.apache.nifi.services.azure.cosmos.AzureCosmosDBConnectionService;

@Tags({"azure", "cosmos", "document", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to Cosmos DB (Core SQL API) " +
                " and provides access to that connection to other Cosmos DB-related components."
)
public abstract class AbstractCosmosDBClientService
        extends AbstractControllerService
        implements AzureCosmosDBConnectionService {

    protected CosmosClient cosmosClient;

    @OnStopped
    public final void onStopped() {
        if (this.cosmosClient != null) {
            try {
                cosmosClient.close();
            } catch(CosmosException e) {
                getLogger().error("Closing CosmosClient Failed: " + e.getMessage(), e);
            } finally {
                this.cosmosClient = null;
            }
        }
    }

    protected void createCosmosClient(final String uri, final String accessKey, final String selectedConsistency){
        final ConsistencyLevel cLevel;

        switch(selectedConsistency) {
            case AzureCosmosDBUtils.CONSISTENCY_STRONG:
                cLevel =  ConsistencyLevel.STRONG;
                break;
            case AzureCosmosDBUtils.CONSISTENCY_CONSISTENT_PREFIX:
                cLevel = ConsistencyLevel.CONSISTENT_PREFIX;
                break;
            case AzureCosmosDBUtils.CONSISTENCY_BOUNDED_STALENESS:
                cLevel = ConsistencyLevel.BOUNDED_STALENESS;
                break;
            case AzureCosmosDBUtils.CONSISTENCY_EVENTUAL:
                cLevel = ConsistencyLevel.EVENTUAL;
                break;
            default:
                cLevel = ConsistencyLevel.SESSION;
        }
        this.cosmosClient = new CosmosClientBuilder()
                .endpoint(uri)
                .key(accessKey)
                .consistencyLevel(cLevel)
                .buildClient();
    }

    @Override
    public CosmosClient getCosmosClient() {
        return this.cosmosClient;
    }

    public void setCosmosClient(CosmosClient client) {
        this.cosmosClient = client;
    }
}

