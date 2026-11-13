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
package org.apache.nifi.cdc.mongodb.processors;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mongodb.MongoDBClientService;

import java.util.List;
import java.util.Map;

/**
 * Stands in for the client service in the tests that drive the processor with a fake cursor, so no server is
 * needed. The URI carries a password on purpose: the transit URI must not repeat it.
 */
public class FakeMongoDBClientService extends AbstractControllerService implements MongoDBClientService {

    static final String URI = "mongodb://cdc_reader:s3cret@mongo.example:27017";

    @Override
    public MongoDatabase getDatabase(final String name) {
        throw new UnsupportedOperationException("The tests that use this service never reach the server");
    }

    @Override
    public String getURI() {
        return URI;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return WriteConcern.ACKNOWLEDGED;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> variables) {
        return List.of();
    }
}
