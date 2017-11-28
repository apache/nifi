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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


@Tags({"mongo", "mongodb", "lookup", "record"})
@CapabilityDescription(
    "Provides a lookup service based around MongoDB. Each key that is specified \n" +
    "will be added to a query as-is. For example, if you specify the two keys, \n" +
    "user and email, the resulting query will be { \"user\": \"tester\", \"email\": \"tester@test.com\" }.\n" +
    "The query is limited to the first result (findOne in the Mongo documentation). If no \"Lookup Value Field\" is specified " +
    "then the entire MongoDB result document minus the _id field will be returned as a record."
)
public class MongoDBLookupService extends MongoDBControllerService implements LookupService<Object> {

    public static final PropertyDescriptor LOOKUP_VALUE_FIELD = new PropertyDescriptor.Builder()
            .name("mongo-lookup-value-field")
            .displayName("Lookup Value Field")
            .description("The field whose value will be returned when the lookup key(s) match a record. If not specified then the entire " +
                    "MongoDB result document minus the _id field will be returned as a record.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    private String lookupValueField;

    private static final List<PropertyDescriptor> lookupDescriptors;

    static {
        lookupDescriptors = new ArrayList<>();
        lookupDescriptors.addAll(descriptors);
        lookupDescriptors.add(LOOKUP_VALUE_FIELD);
    }

    @Override
    public Optional<Object> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        Map<String, Object> clean = new HashMap<>();
        clean.putAll(coordinates);
        Document query = new Document(clean);

        if (coordinates.size() == 0) {
            throw new LookupFailureException("No keys were configured. Mongo query would return random documents.");
        }

        try {
            Document result = this.findOne(query);

            if (lookupValueField != null && !lookupValueField.equals("")) {
                return Optional.ofNullable(result.get(lookupValueField));
            } else {
                final List<RecordField> fields = new ArrayList<>();

                for (String key : result.keySet()) {
                    if (key.equals("_id")) {
                        continue;
                    }
                    fields.add(new RecordField(key, RecordFieldType.STRING.getDataType()));
                }

                final RecordSchema schema = new SimpleRecordSchema(fields);
                return Optional.ofNullable(new MapRecord(schema, result));
            }
        } catch (Exception ex) {
            getLogger().error("Error during lookup {}", new Object[]{ query.toJson() }, ex);
            throw new LookupFailureException(ex);
        }
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, InterruptedException {
        this.lookupValueField = context.getProperty(LOOKUP_VALUE_FIELD).getValue();
        super.onEnabled(context);
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {

        return lookupDescriptors;
    }
}
