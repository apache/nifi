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

package org.apache.nifi.processors.mongodb;

import com.mongodb.client.MongoCollection;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMongoQueryProcessor extends AbstractMongoProcessor {
    public static final String DB_NAME = "mongo.database.name";
    public static final String COL_NAME = "mongo.collection.name";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have the results of a successful query execution go here.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All input FlowFiles that are part of a failed query execution go here.")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("All input FlowFiles that are part of a successful query execution go here.")
            .build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("Query")
            .description("The selection criteria to do the lookup. If the field is left blank, it will look for input from" +
                    " an incoming connection from another processor to provide the query as a valid JSON document inside of " +
                    "the FlowFile's body. If this field is left blank and a timer is enabled instead of an incoming connection, " +
                    "that will result in a full collection fetch using a \"{}\" query.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
            .name("Projection")
            .description("The fields to be returned from the documents in the result set; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("Sort")
            .description("The fields by which to sort; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    public static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("Limit")
            .description("The maximum number of elements to return")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of elements to be returned from the server in one batch")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many results to put into a FlowFile at once. The whole body will be treated as a JSON array of results.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    protected Document getQuery(ProcessContext context, ProcessSession session, FlowFile input) {
        Document query = null;
        if (context.getProperty(QUERY).isSet()) {
            query = Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue());
        } else if (!context.getProperty(QUERY).isSet() && input == null) {
            query = Document.parse("{}");
        } else {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                session.exportTo(input, out);
                out.close();
                query = Document.parse(new String(out.toByteArray()));
            } catch (Exception ex) {
                getLogger().error("Error reading FlowFile : ", ex);
                if (input != null) { //Likely culprit is a bad query
                    session.transfer(input, REL_FAILURE);
                    session.commit();
                } else {
                    throw new ProcessException(ex);
                }
            }
        }

        return query;
    }

    protected Map<String, String> getAttributes(ProcessContext context, FlowFile input, Document query, MongoCollection collection) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        if (context.getProperty(QUERY_ATTRIBUTE).isSet()) {
            final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue();
            attributes.put(queryAttr, query.toJson());
        }

        attributes.put(DB_NAME, collection.getNamespace().getDatabaseName());
        attributes.put(COL_NAME, collection.getNamespace().getCollectionName());

        return attributes;
    }
}
