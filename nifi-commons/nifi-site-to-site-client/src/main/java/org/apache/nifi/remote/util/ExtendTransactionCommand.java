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
package org.apache.nifi.remote.util;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Site-to-Site Extend Transaction Command executes background requests for transfer transactions
 */
public class ExtendTransactionCommand implements Runnable {
    private static final String CATEGORY = "Site-to-Site";

    private static final Logger logger = LoggerFactory.getLogger(ExtendTransactionCommand.class);

    private final SiteToSiteRestApiClient client;

    private final String transactionUrl;

    private final EventReporter eventReporter;

    ExtendTransactionCommand(final SiteToSiteRestApiClient client, final String transactionUrl, final EventReporter eventReporter) {
        this.client = client;
        this.transactionUrl = transactionUrl;
        this.eventReporter = eventReporter;
    }

    /**
     * Run Command and attempt to extend transaction
     */
    @Override
    public void run() {
        try {
            final TransactionResultEntity entity = client.extendTransaction(transactionUrl);
            logger.debug("Extend Transaction Completed [{}] Code [{}] FlowFiles Sent [{}]", transactionUrl, entity.getResponseCode(), entity.getFlowFileSent());
        } catch (final Throwable e) {
            if (e instanceof IllegalStateException) {
                logger.debug("Extend Transaction Failed [{}] client connection pool shutdown", transactionUrl, e);
            } else {
                logger.warn("Extend Transaction Failed [{}]", transactionUrl, e);
                final String message = String.format("Extend Transaction Failed [%s]: %s", transactionUrl, e.getMessage());
                eventReporter.reportEvent(Severity.WARNING, CATEGORY, message);
                try {
                    client.close();
                } catch (final Exception closeException) {
                    logger.warn("Extend Transaction [{}] Close Client Failed", transactionUrl, closeException);
                }
            }
        }
    }
}
