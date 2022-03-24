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

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.util.TestRunner;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class GridFSITTestBase {
    static final String URI = "mongodb://localhost:27017";
    static final String DB  = "gridfs_test_database";
    MongoClient client;

    public void setup(TestRunner runner, String bucketName) throws Exception {
        setup(runner, bucketName, true);
    }

    public void setup(TestRunner runner, String bucketName, boolean validate) throws Exception {
        MongoDBClientService clientService = new MongoDBControllerService();
        runner.addControllerService("clientService", clientService);
        runner.setProperty(AbstractGridFSProcessor.CLIENT_SERVICE, "clientService");
        runner.setProperty(clientService, MongoDBControllerService.URI, URI);
        runner.setProperty(AbstractGridFSProcessor.BUCKET_NAME, bucketName);
        runner.setProperty(AbstractGridFSProcessor.DATABASE_NAME, DB);
        runner.enableControllerService(clientService);
        runner.setValidateExpressionUsage(true);
        if (validate) {
            runner.assertValid();
        }

        client = new MongoClient("localhost", 27017);
    }
    public void tearDown() {
        client.dropDatabase(DB);
        client.close();
    }

    public boolean fileExists(String name, String bucketName) {
        GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        MongoCursor it = bucket.find(Document.parse(String.format("{ \"filename\": \"%s\" }", name))).iterator();
        boolean retVal = it.hasNext();
        it.close();

        return retVal;
    }

    public ObjectId writeTestFile(String fileName, String content, String bucketName, Map<String, Object> attrs) {
        GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        GridFSUploadOptions options = new GridFSUploadOptions().metadata(new Document(attrs));
        ByteArrayInputStream input = new ByteArrayInputStream(content.getBytes());
        ObjectId retVal = bucket.uploadFromStream(fileName, input, options);

        return retVal;
    }

    public boolean fileHasProperties(String name, String bucketName, Map<String, String> attrs) {
        GridFSBucket bucket = GridFSBuckets.create(client.getDatabase(DB), bucketName);
        MongoCursor it = bucket.find(Document.parse(String.format("{ \"filename\": \"%s\" }", name))).iterator();
        boolean retVal = false;

        if (it.hasNext()) {
            GridFSFile file = (GridFSFile)it.next();
            Document metadata = file.getMetadata();
            if (metadata != null && metadata.size() == attrs.size()) {
                retVal = true;
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    Object val = attrs.get(entry.getKey());
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
