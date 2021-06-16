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

package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public interface ElasticsearchRestProcessor {
    String ATTR_RECORD_COUNT = "record.count";

    PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("el-rest-fetch-index")
            .displayName("Index")
            .description("The name of the index to use.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("el-rest-type")
            .displayName("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching).")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("el-rest-query")
            .displayName("Query")
            .description("A query in JSON syntax, not Lucene syntax. Ex: {\"query\":{\"match\":{\"somefield\":\"somevalue\"}}}. " +
                    "If this parameter is not set, the query will be read from the flowfile content.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();
    PropertyDescriptor QUERY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("el-query-attribute")
            .displayName("Query Attribute")
            .description("If set, the executed query will be set on each result flowfile in the specified attribute.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .required(false)
            .build();

    PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("el-rest-client-service")
            .displayName("Client Service")
            .description("An Elasticsearch client service to use for running queries.")
            .identifiesControllerService(ElasticSearchClientService.class)
            .required(true)
            .build();

    PropertyDescriptor LOG_ERROR_RESPONSES = new PropertyDescriptor.Builder()
            .name("put-es-record-log-error-responses")
            .displayName("Log Error Responses")
            .description("If this is enabled, errors will be logged to the NiFi logs at the error log level. Otherwise, they will " +
                    "only be logged if debug logging is enabled on NiFi as a whole. The purpose of this option is to give the user " +
                    "the ability to debug failed operations without having to turn on debug logging.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All flowfiles that fail for reasons unrelated to server availability go to this relationship.")
            .build();
    Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("All flowfiles that fail due to server/cluster availability go to this relationship.")
            .build();
    Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All flowfiles that succeed in being transferred into Elasticsearch go here.")
            .build();
    Relationship REL_FAILED_RECORDS = new Relationship.Builder()
            .name("errors").description("If an output record write is set, any record that failed to process the way it was " +
                    "configured will be sent to this relationship as part of a failed record record set.")
            .autoTerminateDefault(true).build();

    default String getQuery(FlowFile input, ProcessContext context, ProcessSession session) throws IOException {
        String retVal = null;
        if (context.getProperty(QUERY).isSet()) {
            retVal = context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue();
        } else if (input != null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            session.exportTo(input, out);
            out.close();

            retVal = new String(out.toByteArray());
        }

        return retVal;
    }
}
