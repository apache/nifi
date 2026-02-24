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

package org.apache.nifi.processors.mongodb.gridfs;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.processors.mongodb.AbstractMongoIT;
import org.apache.nifi.util.TestRunner;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class GridFSITTestBase extends AbstractMongoIT {
    static final String DB  = "gridfs_test_database";
    MongoClient client;

    public void setup(final TestRunner runner, final String bucketName) throws Exception {
        setup(runner, bucketName, true);
    }

    public void setup(final TestRunner runner, final String bucketName, final boolean validate) throws Exception {
        final MongoDBClientService clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.setProperty(AbstractGridFSProcessor.CLIENT_SERVICE, "clientService");
        runner.setProperty(clientService, MongoDBControllerService.URI, MONGO_CONTAINER.getConnectionString());
        runner.setProperty(AbstractGridFSProcessor.BUCKET_NAME, bucketName);
        runner.setProperty(AbstractGridFSProcessor.DATABASE_NAME, DB);
        runner.enableControllerService(clientService);
        runner.setValidateExpressionUsage(true);
        if (validate) {
            runner.assertValid();
        }

        client = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }
    public void tearDown() {
        client.getDatabase(DB).drop();
        client.close();
    }

    public boolean fileExists(final String name, final String bucketName) {
        final GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        final MongoCursor it = bucket.find(Document.parse(String.format("{ \"filename\": \"%s\" }", name))).iterator();
        final boolean retVal = it.hasNext();
        it.close();

        return retVal;
    }

    public ObjectId writeTestFile(final String fileName, final String content, final String bucketName, final Map<String, Object> attrs) {
        final GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        final GridFSUploadOptions options = new GridFSUploadOptions().metadata(new Document(attrs));
        final ByteArrayInputStream input = new ByteArrayInputStream(content.getBytes());
        final ObjectId retVal = bucket.uploadFromStream(fileName, input, options);

        return retVal;
    }

    public boolean fileHasProperties(final String name, final String bucketName, final Map<String, String> attrs) {
        final GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        final MongoCursor it = bucket.find(Document.parse(String.format("{ \"filename\": \"%s\" }", name))).iterator();
        boolean retVal = false;

        if (it.hasNext()) {
            final GridFSFile file = (GridFSFile) it.next();
            final Document metadata = file.getMetadata();
            if (metadata != null && metadata.size() == attrs.size()) {
                retVal = true;
                for (final Map.Entry<String, Object> entry : metadata.entrySet()) {
                    final Object val = attrs.get(entry.getKey());
                    if (val == null || !entry.getValue().equals(val)) {
                        retVal = false;
                        break;
                    }
                }
            }
        }

        it.close();

        return retVal;
    }
}
