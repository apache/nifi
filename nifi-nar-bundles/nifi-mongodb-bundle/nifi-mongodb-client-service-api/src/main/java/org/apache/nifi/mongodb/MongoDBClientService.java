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

package org.apache.nifi.mongodb;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.bson.Document;

public interface MongoDBClientService extends ControllerService {
     String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
     String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
     String WRITE_CONCERN_FSYNCED = "FSYNCED";
     String WRITE_CONCERN_JOURNALED = "JOURNALED";
     String WRITE_CONCERN_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";
     String WRITE_CONCERN_MAJORITY = "MAJORITY";

     PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("mongo-uri")
            .displayName("Mongo URI")
            .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();
     PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
     PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

     PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("mongo-write-concern")
            .displayName("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_FSYNCED, WRITE_CONCERN_JOURNALED,
                    WRITE_CONCERN_REPLICA_ACKNOWLEDGED, WRITE_CONCERN_MAJORITY)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
            .build();


    default Document convertJson(String query) {
        return Document.parse(query);
    }
    WriteConcern getWriteConcern(final ConfigurationContext context);
    MongoDatabase getDatabase(String name);
    String getURI();
}
