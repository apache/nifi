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

package org.apache.nifi.processors.snowflake;

import org.apache.nifi.key.service.StandardPrivateKeyService;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.snowflake.service.SnowflakeComputingConnectionPool;
import org.apache.nifi.snowflake.service.StandardSnowflakeIngestManagerProviderService;
import org.apache.nifi.snowflake.service.util.ConnectionUrlFormat;
import org.apache.nifi.util.TestRunner;

import java.nio.file.Path;
import java.nio.file.Paths;

public interface SnowflakeConfigAware {

    Path filePath = Paths.get("???");

    String organizationName = "???";
    String accountName = "???";
    String userName = "???";
    String password = "???";
    String database = "???";
    String schema = "PUBLIC";
    String pipeName = "???";
    String internalStageName = "???";
    String privateKeyFile = "???";
    String privateKeyPassphrase = "???";

    default PrivateKeyService createPrivateKeyService(TestRunner runner) throws InitializationException {
        final StandardPrivateKeyService privateKeyService = new StandardPrivateKeyService();

        runner.addControllerService("privateKeyService", privateKeyService);

        runner.setProperty(privateKeyService,
                StandardPrivateKeyService.KEY_FILE,
                privateKeyFile);
        runner.setProperty(privateKeyService,
                StandardPrivateKeyService.KEY_PASSWORD,
                privateKeyPassphrase);

        runner.enableControllerService(privateKeyService);
        return privateKeyService;
    }

    default SnowflakeConnectionProviderService createConnectionProviderService(TestRunner runner)
        throws InitializationException {
        final SnowflakeConnectionProviderService connectionProviderService = new SnowflakeComputingConnectionPool();

        runner.addControllerService("connectionProviderService", connectionProviderService);

        runner.setProperty(connectionProviderService,
                SnowflakeComputingConnectionPool.CONNECTION_URL_FORMAT,
                ConnectionUrlFormat.ACCOUNT_NAME);
        runner.setProperty(connectionProviderService,
                SnowflakeComputingConnectionPool.SNOWFLAKE_ORGANIZATION_NAME,
                organizationName);
        runner.setProperty(connectionProviderService,
                SnowflakeComputingConnectionPool.SNOWFLAKE_ACCOUNT_NAME,
                accountName);
        runner.setProperty(connectionProviderService,
                SnowflakeComputingConnectionPool.SNOWFLAKE_USER,
                userName);
        runner.setProperty(connectionProviderService,
                SnowflakeComputingConnectionPool.SNOWFLAKE_PASSWORD,
                password);
        runner.setProperty(connectionProviderService,
                SnowflakeProperties.DATABASE,
                database);
        runner.setProperty(connectionProviderService,
                SnowflakeProperties.SCHEMA,
                schema);

        runner.enableControllerService(connectionProviderService);
        return connectionProviderService;
    }

    default SnowflakeIngestManagerProviderService createIngestManagerProviderService(TestRunner runner)
            throws InitializationException {
        final SnowflakeIngestManagerProviderService ingestManagerProviderService =
                new StandardSnowflakeIngestManagerProviderService();
        final PrivateKeyService privateKeyService = createPrivateKeyService(runner);

        runner.addControllerService("ingestManagerProviderService", ingestManagerProviderService);

        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.ORGANIZATION_NAME,
                organizationName);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.ACCOUNT_NAME,
                accountName);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.USER_NAME,
                userName);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.DATABASE,
                database);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.SCHEMA,
                schema);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.PIPE,
                pipeName);
        runner.setProperty(ingestManagerProviderService,
                StandardSnowflakeIngestManagerProviderService.PRIVATE_KEY_SERVICE,
                privateKeyService.getIdentifier());

        runner.enableControllerService(ingestManagerProviderService);
        return ingestManagerProviderService;
    }
}
