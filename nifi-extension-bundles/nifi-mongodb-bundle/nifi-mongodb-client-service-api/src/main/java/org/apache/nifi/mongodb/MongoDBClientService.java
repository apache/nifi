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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextProvider;
import org.bson.Document;

public interface MongoDBClientService extends ControllerService, VerifiableControllerService {
     String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
     String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
     String WRITE_CONCERN_FSYNCED = "FSYNCED";
     String WRITE_CONCERN_JOURNALED = "JOURNALED";
     String WRITE_CONCERN_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";
     String WRITE_CONCERN_MAJORITY = "MAJORITY";
     String WRITE_CONCERN_W1 = "W1";
     String WRITE_CONCERN_W2 = "W2";
     String WRITE_CONCERN_W3 = "W3";

     AllowableValue WRITE_CONCERN_ACKNOWLEDGED_VALUE = new AllowableValue(
            WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_ACKNOWLEDGED,
            "Write operations that use this write concern will wait for acknowledgement, " +
            "using the default write concern configured on the server");
     AllowableValue WRITE_CONCERN_UNACKNOWLEDGED_VALUE = new AllowableValue(
            WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED,
            "Write operations that use this write concern will return as soon as the message is written to the socket. " +
            "Exceptions are raised for network issues, but not server errors");
     AllowableValue WRITE_CONCERN_FSYNCED_VALUE = new AllowableValue(
            WRITE_CONCERN_FSYNCED, WRITE_CONCERN_FSYNCED,
            "Deprecated.  Use of \"" + WRITE_CONCERN_JOURNALED + "\" is preferred");
     AllowableValue WRITE_CONCERN_JOURNALED_VALUE = new AllowableValue(
            WRITE_CONCERN_JOURNALED, WRITE_CONCERN_JOURNALED,
            "Write operations wait for the server to group commit to the journal file on disk");
     AllowableValue WRITE_CONCERN_REPLICA_ACKNOWLEDGED_VALUE = new AllowableValue(
            WRITE_CONCERN_REPLICA_ACKNOWLEDGED, WRITE_CONCERN_REPLICA_ACKNOWLEDGED,
            "Deprecated.  Use of \"" + WRITE_CONCERN_W2 + "\" is preferred");
     AllowableValue WRITE_CONCERN_MAJORITY_VALUE = new AllowableValue(
            WRITE_CONCERN_MAJORITY, WRITE_CONCERN_MAJORITY,
            "Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation");
     AllowableValue WRITE_CONCERN_W1_VALUE = new AllowableValue(
            WRITE_CONCERN_W1, WRITE_CONCERN_W1,
            "Write operations that use this write concern will wait for acknowledgement from a single member");
     AllowableValue WRITE_CONCERN_W2_VALUE = new AllowableValue(
            WRITE_CONCERN_W2, WRITE_CONCERN_W2,
            "Write operations that use this write concern will wait for acknowledgement from two members");
     AllowableValue WRITE_CONCERN_W3_VALUE = new AllowableValue(
            WRITE_CONCERN_W3, WRITE_CONCERN_W3,
            "Write operations that use this write concern will wait for acknowledgement from three members");

     PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("mongo-uri")
            .displayName("Mongo URI")
            .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();
     PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .displayName("Database User")
            .description("Database user name")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
     PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .displayName("Password")
            .description("The password for the database user")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();
     PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();
     PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

     PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("mongo-write-concern")
            .displayName("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED_VALUE, WRITE_CONCERN_UNACKNOWLEDGED_VALUE, WRITE_CONCERN_FSYNCED_VALUE,
                             WRITE_CONCERN_JOURNALED_VALUE, WRITE_CONCERN_REPLICA_ACKNOWLEDGED_VALUE, WRITE_CONCERN_MAJORITY_VALUE,
                             WRITE_CONCERN_W1_VALUE, WRITE_CONCERN_W2_VALUE, WRITE_CONCERN_W3_VALUE)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED_VALUE.getValue())
            .build();


    default Document convertJson(String query) {
        return Document.parse(query);
    }
    MongoDatabase getDatabase(String name);
    String getURI();
    WriteConcern getWriteConcern();
}
