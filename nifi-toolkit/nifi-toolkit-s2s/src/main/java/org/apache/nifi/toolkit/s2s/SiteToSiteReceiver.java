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

package org.apache.nifi.toolkit.s2s;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Class that will print received DataPackets to output
 */
public class SiteToSiteReceiver {
    private final SiteToSiteClient siteToSiteClient;
    private final OutputStream output;

    public SiteToSiteReceiver(SiteToSiteClient siteToSiteClient, OutputStream output) {
        this.siteToSiteClient = siteToSiteClient;
        this.output = output;
    }

    public TransactionCompletion receiveFiles() throws IOException {
        Transaction transaction = siteToSiteClient.createTransaction(TransferDirection.RECEIVE);
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(output);
        jsonGenerator.writeStartArray();
        DataPacket dataPacket;
        while ((dataPacket = transaction.receive()) != null) {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("attributes");
            jsonGenerator.writeStartObject();
            Map<String, String> attributes = dataPacket.getAttributes();
            if (attributes != null) {
                for (Map.Entry<String, String> stringStringEntry : attributes.entrySet()) {
                    jsonGenerator.writeStringField(stringStringEntry.getKey(), stringStringEntry.getValue());
                }
            }
            jsonGenerator.writeEndObject();
            InputStream data = dataPacket.getData();
            if (data != null) {
                jsonGenerator.writeBinaryField("data", IOUtils.toByteArray(data));
            }
            jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
        transaction.confirm();
        return transaction.complete();
    }
}
