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

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransactionCompletion;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class that will send DataPackets read from input
 */
public class SiteToSiteSender {
    private final SiteToSiteClient siteToSiteClient;
    private final InputStream input;

    public SiteToSiteSender(SiteToSiteClient siteToSiteClient, InputStream input) {
        this.siteToSiteClient = siteToSiteClient;
        this.input = input;
    }

    public TransactionCompletion sendFiles() throws IOException {
        Transaction transaction = siteToSiteClient.createTransaction(TransferDirection.SEND);
        try {
            DataPacketDto.getDataPacketStream(input).forEachOrdered(d -> {
                try {
                    transaction.send(d);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException(e.getMessage(), e);
        }
        transaction.confirm();
        return transaction.complete();
    }
}
