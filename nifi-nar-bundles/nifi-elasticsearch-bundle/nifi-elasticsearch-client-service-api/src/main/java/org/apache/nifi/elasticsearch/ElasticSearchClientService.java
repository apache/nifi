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
package org.apache.nifi.elasticsearch;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Tags({"elasticsearch", "client"})
@CapabilityDescription("A controller service for accessing an Elasticsearch client.")
public interface ElasticSearchClientService extends ControllerService {
    PropertyDescriptor HTTP_HOSTS = new PropertyDescriptor.Builder()
            .name("el-cs-http-hosts")
            .displayName("HTTP Hosts")
            .description("A comma-separated list of HTTP hosts that host Elasticsearch query nodes.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-cs-ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections. This service only applies if the Elasticsearch endpoint(s) have been secured with TLS/SSL.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .addValidator(Validator.VALID)
            .build();
    PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("el-cs-username")
            .displayName("Username")
            .description("The username to use with XPack security.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("el-cs-password")
            .displayName("Password")
            .description("The password to use with XPack security.")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-connect-timeout")
            .displayName("Connect timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when trying to connect.")
            .required(true)
            .defaultValue("5000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor SOCKET_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-socket-timeout")
            .displayName("Read timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when waiting for a response.")
            .required(true)
            .defaultValue("60000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor RETRY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el-cs-retry-timeout")
            .displayName("Retry timeout")
            .description("Controls the amount of time, in milliseconds, before a timeout occurs when retrying the operation.")
            .required(true)
            .defaultValue("60000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("el-cs-charset")
            .displayName("Charset")
            .description("The charset to use for interpreting the response from Elasticsearch.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null/empty, will not be written out");

    AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
            "Fields that are missing (present in the schema but not in the record), or that have a value of null/empty, will be written out as a null/empty value");

    PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("el-cs-suppress-nulls")
            .displayName("Suppress Null/Empty Values")
            .description("Specifies how the writer should handle null and empty fields (including objects and arrays)")
            .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS)
            .defaultValue(ALWAYS_SUPPRESS.getValue())
            .required(true)
            .build();

    /**
     * Index a document.
     *
     * @param operation A document to index.
     * @return IndexOperationResponse if successful
     * @throws IOException thrown when there is an error.
     */
    IndexOperationResponse add(IndexOperationRequest operation);

    /**
     * Bulk process multiple documents.
     *
     * @param operations A list of index operations.
     * @return IndexOperationResponse if successful.
     * @throws IOException thrown when there is an error.
     */
    IndexOperationResponse bulk(List<IndexOperationRequest> operations);

    /**
     * Count the documents that match the criteria.
     *
     * @param query A query in the JSON DSL syntax
     * @param index The index to target.
     * @param type The type to target.
     * @return
     */
    Long count(String query, String index, String type);

    /**
     * Delete a document by its ID from an index.
     *
     * @param index The index to target.
     * @param type The type to target. Optional.
     * @param id The document ID to remove from the selected index.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteById(String index, String type, String id);


    /**
     * Delete multiple documents by ID from an index.
     * @param index The index to target.
     * @param type The type to target. Optional.
     * @param ids A list of document IDs to remove from the selected index.
     * @return A DeleteOperationResponse object if successful.
     * @throws IOException thrown when there is an error.
     */
    DeleteOperationResponse deleteById(String index, String type, List<String> ids);

    /**
     * Delete documents by query.
     *
     * @param query A valid JSON query to be used for finding documents to delete.
     * @param index The index to target.
     * @param type The type to target within the index. Optional.
     * @return A DeleteOperationResponse object if successful.
     */
    DeleteOperationResponse deleteByQuery(String query, String index, String type);

    /**
     * Get a document by ID.
     *
     * @param index The index that holds the document.
     * @param type The document type. Optional.
     * @param id The document ID
     * @return Map if successful, null if not found.
     * @throws IOException thrown when there is an error.
     */
    Map<String, Object> get(String index, String type, String id);

    /**
     * Perform a search using the JSON DSL.
     *
     * @param query A JSON string reprensenting the query.
     * @param index The index to target. Optional.
     * @param type The type to target. Optional. Will not be used in future versions of Elasticsearch.
     * @return A SearchResponse object if successful.
     */
    SearchResponse search(String query, String index, String type);

    /**
     * Build a transit URL to use with the provenance reporter.
     * @param index Index targeted. Optional.
     * @param type Type targeted. Optional
     * @return a URL describing the Elasticsearch cluster.
     */
    String getTransitUrl(String index, String type);
}
