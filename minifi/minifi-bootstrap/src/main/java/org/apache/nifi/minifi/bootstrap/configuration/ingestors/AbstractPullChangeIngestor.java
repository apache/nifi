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

package org.apache.nifi.minifi.bootstrap.configuration.ingestors;

import org.apache.nifi.minifi.bootstrap.ConfigurationFileHolder;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.interfaces.ChangeIngestor;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractPullChangeIngestor implements Runnable, ChangeIngestor {

    // 5 minute default pulling period
    protected static final String DEFAULT_POLLING_PERIOD = "300000";
    protected static Logger logger;

    protected final AtomicInteger pollingPeriodMS = new AtomicInteger();
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    protected volatile ConfigurationChangeNotifier configurationChangeNotifier;
    protected final AtomicReference<Properties> properties = new AtomicReference<>();

    @Override
    public void initialize(Properties properties, ConfigurationFileHolder configurationFileHolder, ConfigurationChangeNotifier configurationChangeNotifier) {
        this.configurationChangeNotifier = configurationChangeNotifier;
        this.properties.set(properties);
    }

    @Override
    public void start() {
        scheduledThreadPoolExecutor.scheduleAtFixedRate(this, pollingPeriodMS.get(), pollingPeriodMS.get(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
        scheduledThreadPoolExecutor.shutdownNow();
    }

    @Override
    public abstract void run();
}
