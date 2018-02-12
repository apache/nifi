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
package org.apache.nifi.toolkit.cli.impl.command.composite;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.pg.PGImport;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.CreateRegistryClient;
import org.apache.nifi.toolkit.cli.impl.command.nifi.registry.GetRegistryClientId;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.CreateBucket;
import org.apache.nifi.toolkit.cli.impl.command.registry.bucket.ListBuckets;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.CreateFlow;
import org.apache.nifi.toolkit.cli.impl.command.registry.flow.ImportFlowVersion;
import org.apache.nifi.toolkit.cli.impl.result.BucketsResult;
import org.apache.nifi.toolkit.cli.impl.result.RegistryClientIDResult;
import org.apache.nifi.toolkit.cli.impl.result.StringResult;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * Command to demonstrate a quick import capability.
 */
public class QuickImport extends AbstractCompositeCommand<StringResult> {

    public static final String BUCKET_NAME = "Quick Import";
    public static final String BUCKET_DESC = "Created to demonstrate quickly importing a flow with NiFi CLI.";

    public static final String FLOW_NAME = "Quick Import - ";
    public static final String FLOW_DESC = "Automatically imported on ";

    public static final String REG_CLIENT_NAME = "Quick Import";
    public static final String REG_CLIENT_DESC = "Automatically created on ";

    private final ListBuckets listBuckets;
    private final CreateBucket createBucket;
    private final CreateFlow createFlow;
    private final ImportFlowVersion importFlowVersion;
    private final GetRegistryClientId getRegistryClientId;
    private final CreateRegistryClient createRegistryClient;
    private final PGImport pgImport;

    public QuickImport() {
        super("quick-import", StringResult.class);
        this.listBuckets = new ListBuckets();
        this.createBucket = new CreateBucket();
        this.createFlow = new CreateFlow();
        this.importFlowVersion = new ImportFlowVersion();
        this.getRegistryClientId = new GetRegistryClientId();
        this.createRegistryClient = new CreateRegistryClient();
        this.pgImport = new PGImport();
    }

    @Override
    public String getDescription() {
        return "Imports a flow from a file or a public URL into a pre-defined bucket named '" + BUCKET_NAME + "'. This command will " +
                "create the bucket if it doesn't exist, and will create a new flow for each execution. The flow will then be imported " +
                "to the given NiFi instance.";
    }

    @Override
    protected void doInitialize(Context context) {
        // add additional options
        addOption(CommandOption.INPUT_SOURCE.createOption());

        // initialize sub-commands since we are managing their lifecycle ourselves here
        listBuckets.initialize(context);
        createBucket.initialize(context);
        createFlow.initialize(context);
        importFlowVersion.initialize(context);
        getRegistryClientId.initialize(context);
        createRegistryClient.initialize(context);
        pgImport.initialize(context);
    }

    @Override
    public StringResult doExecute(final CommandLine cli, final NiFiClient nifiClient, final Properties nifiProps,
                                  final NiFiRegistryClient registryClient, final Properties registryProps)
            throws IOException, NiFiRegistryException, ParseException, NiFiClientException {

        final boolean isInteractive = getContext().isInteractive();

        // determine the registry client in NiFi to use, or create one
        // do this first so that we don't get through creating buckets, flows, etc, and then fail on the reg client
        final String registryClientBaseUrl = registryProps.getProperty(CommandOption.URL.getLongName());
        final String registryClientId = getRegistryClientId(nifiClient, registryClientBaseUrl, isInteractive);

        // get or create the quick import bucket
        final String quickImportBucketId = getQuickImportBucketId(registryClient, isInteractive);

        // create a new flow in the quick-import bucket
        final String quickImportFlowId = createQuickImportFlow(registryClient, quickImportBucketId, isInteractive);

        // import the versioned flow snapshot into newly created quick-import flow
        final String inputSource = cli.getOptionValue(CommandOption.INPUT_SOURCE.getLongName());
        if (StringUtils.isBlank(inputSource)) {
            throw new MissingOptionException("Missing required option --" + CommandOption.INPUT_SOURCE.getLongName());
        }

        final String quickImportFlowVersion = importFlowVersion(registryClient, quickImportFlowId, isInteractive, inputSource);

        // pg-import to nifi
        final Properties pgImportProps = new Properties();
        pgImportProps.setProperty(CommandOption.REGISTRY_CLIENT_ID.getLongName(), registryClientId);
        pgImportProps.setProperty(CommandOption.BUCKET_ID.getLongName(), quickImportBucketId);
        pgImportProps.setProperty(CommandOption.FLOW_ID.getLongName(), quickImportFlowId);
        pgImportProps.setProperty(CommandOption.FLOW_VERSION.getLongName(), quickImportFlowVersion);

        final StringResult createdPgResult = pgImport.doExecute(nifiClient, pgImportProps);

        if (isInteractive) {
            println();
            println("Imported process group to NiFi...");
            println();
        }

        return createdPgResult;
    }

    private String importFlowVersion(final NiFiRegistryClient registryClient, final String quickImportFlowId, final boolean isInteractive, final String inputSource)
            throws ParseException, IOException, NiFiRegistryException {
        final Properties importVersionProps = new Properties();
        importVersionProps.setProperty(CommandOption.FLOW_ID.getLongName(), quickImportFlowId);
        importVersionProps.setProperty(CommandOption.INPUT_SOURCE.getLongName(), inputSource);

        final StringResult createdVersion = importFlowVersion.doExecute(registryClient, importVersionProps);
        final String quickImportFlowVersion = createdVersion.getResult();

        if (isInteractive) {
            println();
            println("Imported flow version...");
        }
        return quickImportFlowVersion;
    }

    private String createQuickImportFlow(final NiFiRegistryClient registryClient, final String quickImportBucketId, final boolean isInteractive)
            throws ParseException, IOException, NiFiRegistryException {
        final String flowName = FLOW_NAME + System.currentTimeMillis();
        final String flowDescription = FLOW_DESC + (new Date()).toString();

        final Properties createFlowProps = new Properties();
        createFlowProps.setProperty(CommandOption.FLOW_NAME.getLongName(), flowName);
        createFlowProps.setProperty(CommandOption.FLOW_DESC.getLongName(), flowDescription);
        createFlowProps.setProperty(CommandOption.BUCKET_ID.getLongName(), quickImportBucketId);

        final StringResult createdFlow = createFlow.doExecute(registryClient, createFlowProps);
        final String quickImportFlowId = createdFlow.getResult();

        if (isInteractive) {
            println();
            println("Created new flow '" + flowName + "'...");
        }
        return quickImportFlowId;
    }

    private String getQuickImportBucketId(final NiFiRegistryClient registryClient, final boolean isInteractive)
            throws IOException, NiFiRegistryException, MissingOptionException {

        final BucketsResult bucketsResult = listBuckets.doExecute(registryClient, new Properties());

        final Bucket quickImportBucket = bucketsResult.getResult().stream()
                .filter(b -> BUCKET_NAME.equals(b.getName()))
                .findFirst().orElse(null);

        // if it doesn't exist, then create the quick import bucket
        String quickImportBucketId = null;
        if (quickImportBucket != null) {
            quickImportBucketId = quickImportBucket.getIdentifier();
            if (isInteractive) {
                println();
                println("Found existing bucket '" + BUCKET_NAME + "'...");
            }
        } else {
            final Properties createBucketProps = new Properties();
            createBucketProps.setProperty(CommandOption.BUCKET_NAME.getLongName(), BUCKET_NAME);
            createBucketProps.setProperty(CommandOption.BUCKET_DESC.getLongName(), BUCKET_DESC);

            final StringResult createdBucketId = createBucket.doExecute(registryClient, createBucketProps);
            quickImportBucketId = createdBucketId.getResult();
            if (isInteractive) {
                println();
                println("Created new bucket '" + BUCKET_NAME + "'...");
            }
        }
        return quickImportBucketId;
    }

    private String getRegistryClientId(final NiFiClient nifiClient, final String registryClientBaseUrl, final boolean isInteractive)
            throws NiFiClientException, IOException, MissingOptionException {

        final Properties getRegClientProps = new Properties();
        getRegClientProps.setProperty(CommandOption.REGISTRY_CLIENT_URL.getLongName(), registryClientBaseUrl);

        String registryClientId;
        try {
            final RegistryClientIDResult registryClientResult = getRegistryClientId.doExecute(nifiClient, getRegClientProps);
            registryClientId = registryClientResult.getResult().getId();
            if (isInteractive) {
                println();
                println("Found existing registry client '" + registryClientResult.getResult().getName() + "'...");
            }
        } catch (Exception e) {
            registryClientId = null;
        }

        if (registryClientId == null) {
            final Properties createRegClientProps = new Properties();
            createRegClientProps.setProperty(CommandOption.REGISTRY_CLIENT_NAME.getLongName(), REG_CLIENT_NAME);
            createRegClientProps.setProperty(CommandOption.REGISTRY_CLIENT_DESC.getLongName(), REG_CLIENT_DESC + new Date().toString());
            createRegClientProps.setProperty(CommandOption.REGISTRY_CLIENT_URL.getLongName(), registryClientBaseUrl);

            final StringResult createdRegClient = createRegistryClient.doExecute(nifiClient, createRegClientProps);
            registryClientId = createdRegClient.getResult();

            if (isInteractive) {
                println();
                println("Created new registry client '" + REG_CLIENT_NAME + "'...");
            }
        }

        return registryClientId;
    }

}
