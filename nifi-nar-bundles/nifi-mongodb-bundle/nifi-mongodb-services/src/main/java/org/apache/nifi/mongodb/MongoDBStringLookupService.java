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

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.bson.Document;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MongoDBStringLookupService extends AbstractControllerService implements StringLookupService, MongoDBLookupServiceConfiguration {
    public static final String QUERY = "query";
    public static final Set<String> KEY = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(QUERY)));

    public static final PropertyDescriptor DB_NAME;
    public static final PropertyDescriptor COL_NAME;

    static {
        DB_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(DATABASE_NAME)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        COL_NAME = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(COLLECTION_NAME)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
    }

    public static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
        CONTROLLER_SERVICE, DB_NAME, COL_NAME
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    private volatile MongoDBControllerService clientService;
    private volatile MongoCollection collection;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        clientService = context.getProperty(CONTROLLER_SERVICE).asControllerService(MongoDBControllerService.class);
        String db = context.getProperty(DB_NAME).evaluateAttributeExpressions().getValue();
        String col = context.getProperty(COL_NAME).evaluateAttributeExpressions().getValue();

        collection = clientService.getDatabase(db).getCollection(col);
    }

    @OnDisabled
    public void onDisabled() {
        clientService = null;
        collection = null;
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (!coordinates.containsKey(QUERY) || coordinates.get(QUERY) == null) {
            throw new LookupFailureException(String.format("Missing lookup key. You need to define the key %s", QUERY));
        }

        Document doc;
        try {
            doc = Document.parse((String) coordinates.get(QUERY));
        } catch (Exception ex) {
            throw new LookupFailureException(ex);
        }

        try {
            FindIterable<Document> iterable = collection.find(doc).limit(1);
            MongoCursor<Document> cursor = iterable.iterator();
            Document result = cursor.hasNext() ? cursor.next() : null;

            return Optional.ofNullable(result.toJson());
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return KEY;
    }
}
