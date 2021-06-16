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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Value;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.KeystoreType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.util.FormatUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SiteToSiteCliMain {
    public static final String URL_OPTION = "url";
    public static final String URL_OPTION_DEFAULT = "http://localhost:8080/nifi";
    public static final String DIRECTION_OPTION = "direction";
    public static final String DIRECTION_OPTION_DEFAULT = TransferDirection.SEND.toString();
    public static final String PORT_NAME_OPTION = "portName";
    public static final String PORT_IDENTIFIER_OPTION = "portIdentifier";
    public static final String TIMEOUT_OPTION = "timeout";
    public static final String PENALIZATION_OPTION = "penalization";
    public static final String KEYSTORE_OPTION = "keyStore";
    public static final String KEY_STORE_TYPE_OPTION = "keyStoreType";
    public static final String KEY_STORE_PASSWORD_OPTION = "keyStorePassword";
    public static final String TRUST_STORE_OPTION = "trustStore";
    public static final String TRUST_STORE_TYPE_OPTION = "trustStoreType";
    public static final String TRUST_STORE_PASSWORD_OPTION = "trustStorePassword";
    public static final String PEER_PERSISTENCE_FILE_OPTION = "peerPersistenceFile";
    public static final String COMPRESSION_OPTION = "compression";
    public static final String TRANSPORT_PROTOCOL_OPTION = "transportProtocol";
    public static final String TRANSPORT_PROTOCOL_OPTION_DEFAULT = SiteToSiteTransportProtocol.RAW.toString();
    public static final String BATCH_COUNT_OPTION = "batchCount";
    public static final String BATCH_SIZE_OPTION = "batchSize";
    public static final String BATCH_DURATION_OPTION = "batchDuration";
    public static final String HELP_OPTION = "help";
    public static final String PROXY_HOST_OPTION = "proxyHost";
    public static final String PROXY_PORT_OPTION = "proxyPort";
    public static final String PROXY_USERNAME_OPTION = "proxyUsername";
    public static final String PROXY_PASSWORD_OPTION = "proxyPassword";
    public static final String PROXY_PORT_OPTION_DEFAULT = "80";
    public static final String KEYSTORE_TYPE_OPTION_DEFAULT = KeystoreType.JKS.toString();

    /**
     * Prints the usage to System.out
     *
     * @param errorMessage optional error message
     * @param options      the options object to print usage for
     */
    public static void printUsage(String errorMessage, Options options) {
        if (errorMessage != null) {
            System.out.println(errorMessage);
            System.out.println();
            System.out.println();
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        objectMapper.setDefaultPropertyInclusion(Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        System.out.println("s2s is a command line tool that can either read a list of DataPackets from stdin to send over site-to-site or write the received DataPackets to stdout");
        System.out.println();
        System.out.println("The s2s cli input/output format is a JSON list of DataPackets.  They can have the following formats:");
        try {
            System.out.println();
            objectMapper.writeValue(System.out, Arrays.asList(new DataPacketDto("hello nifi".getBytes(StandardCharsets.UTF_8)).putAttribute("key", "value")));
            System.out.println();
            System.out.println("Where data is the base64 encoded value of the FlowFile content (always used for received data) or");
            System.out.println();
            objectMapper.writeValue(System.out, Arrays.asList(new DataPacketDto(new HashMap<>(), new File("EXAMPLE").getAbsolutePath()).putAttribute("key", "value")));
            System.out.println();
            System.out.println("Where dataFile is a file to read the FlowFile content from");
            System.out.println();
            System.out.println();
            System.out.println("Example usage to send a FlowFile with the contents of \"hey nifi\" to a local unsecured NiFi over http with an input port named input:");
            System.out.print("echo '");
            DataPacketDto dataPacketDto = new DataPacketDto("hey nifi".getBytes(StandardCharsets.UTF_8));
            dataPacketDto.setAttributes(null);
            objectMapper.writeValue(System.out, Arrays.asList(dataPacketDto));
            System.out.println("' | bin/s2s.sh -n input -p http");
            System.out.println();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.printHelp("s2s", options);
        System.out.flush();
    }

    /**
     * Parses command line options into a CliParse object
     *
     * @param options an empty options object (so callers can print usage if the parse fails
     * @param args    the string array of arguments
     * @return a CliParse object containing the constructed SiteToSiteClient.Builder and a TransferDirection
     * @throws ParseException if there is an error parsing the command line
     */
    public static CliParse parseCli(Options options, String[] args) throws ParseException {
        options.addOption("u", URL_OPTION, true, "NiFI URL to connect to (default: " + URL_OPTION_DEFAULT + ")");
        options.addOption("d", DIRECTION_OPTION, true, "Direction (valid directions: "
                + Arrays.stream(TransferDirection.values()).map(Object::toString).collect(Collectors.joining(", ")) + ") (default: " + DIRECTION_OPTION_DEFAULT + ")");
        options.addOption("n", PORT_NAME_OPTION, true, "Port name");
        options.addOption("i", PORT_IDENTIFIER_OPTION, true, "Port id");
        options.addOption(null, TIMEOUT_OPTION, true, "Timeout");
        options.addOption(null, PENALIZATION_OPTION, true, "Penalization period");
        options.addOption(null, KEYSTORE_OPTION, true, "Keystore");
        options.addOption(null, KEY_STORE_TYPE_OPTION, true, "Keystore type (default: " + KEYSTORE_TYPE_OPTION_DEFAULT + ")");
        options.addOption(null, KEY_STORE_PASSWORD_OPTION, true, "Keystore password");
        options.addOption(null, TRUST_STORE_OPTION, true, "Truststore");
        options.addOption(null, TRUST_STORE_TYPE_OPTION, true, "Truststore type (default: " + KEYSTORE_TYPE_OPTION_DEFAULT + ")");
        options.addOption(null, TRUST_STORE_PASSWORD_OPTION, true, "Truststore password");
        options.addOption("c", COMPRESSION_OPTION, false, "Use compression");
        options.addOption(null, PEER_PERSISTENCE_FILE_OPTION, true, "File to write peer information to so it can be recovered on restart");
        options.addOption("p", TRANSPORT_PROTOCOL_OPTION, true, "Site to site transport protocol (default: " + TRANSPORT_PROTOCOL_OPTION_DEFAULT + ")");
        options.addOption(null, BATCH_COUNT_OPTION, true, "Number of flow files in a batch");
        options.addOption(null, BATCH_SIZE_OPTION, true, "Size of flow files in a batch");
        options.addOption(null, BATCH_DURATION_OPTION, true, "Duration of a batch");
        options.addOption(null, PROXY_HOST_OPTION, true, "Proxy hostname");
        options.addOption(null, PROXY_PORT_OPTION, true, "Proxy port");
        options.addOption(null, PROXY_USERNAME_OPTION, true, "Proxy username");
        options.addOption(null, PROXY_PASSWORD_OPTION, true, "Proxy password");
        options.addOption("h", HELP_OPTION, false, "Show help message and exit");
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine;
        commandLine = parser.parse(options, args);
        if (commandLine.hasOption(HELP_OPTION)) {
            printUsage(null, options);
            System.exit(1);
        }
        SiteToSiteClient.Builder builder = new SiteToSiteClient.Builder();
        builder.url(commandLine.getOptionValue(URL_OPTION, URL_OPTION_DEFAULT));
        if (commandLine.hasOption(PORT_NAME_OPTION)) {
            builder.portName(commandLine.getOptionValue(PORT_NAME_OPTION));
        }
        if (commandLine.hasOption(PORT_IDENTIFIER_OPTION)) {
            builder.portIdentifier(commandLine.getOptionValue(PORT_IDENTIFIER_OPTION));
        }
        if (commandLine.hasOption(TIMEOUT_OPTION)) {
            builder.timeout(FormatUtils.getTimeDuration(commandLine.getOptionValue(TIMEOUT_OPTION), TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        }
        if (commandLine.hasOption(PENALIZATION_OPTION)) {
            builder.nodePenalizationPeriod(FormatUtils.getTimeDuration(commandLine.getOptionValue(PENALIZATION_OPTION), TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        }
        if (commandLine.hasOption(KEYSTORE_OPTION)) {
            builder.keystoreFilename(commandLine.getOptionValue(KEYSTORE_OPTION));
            builder.keystoreType(KeystoreType.valueOf(commandLine.getOptionValue(KEY_STORE_TYPE_OPTION, KEYSTORE_TYPE_OPTION_DEFAULT).toUpperCase()));

            if (commandLine.hasOption(KEY_STORE_PASSWORD_OPTION)) {
                builder.keystorePass(commandLine.getOptionValue(KEY_STORE_PASSWORD_OPTION));
            } else {
                throw new ParseException("Must specify keystore password");
            }
        }
        if (commandLine.hasOption(TRUST_STORE_OPTION)) {
            builder.truststoreFilename(commandLine.getOptionValue(TRUST_STORE_OPTION));
            builder.truststoreType(KeystoreType.valueOf(commandLine.getOptionValue(TRUST_STORE_TYPE_OPTION, KEYSTORE_TYPE_OPTION_DEFAULT).toUpperCase()));

            if (commandLine.hasOption(TRUST_STORE_PASSWORD_OPTION)) {
                builder.truststorePass(commandLine.getOptionValue(TRUST_STORE_PASSWORD_OPTION));
            } else {
                throw new ParseException("Must specify truststore password");
            }
        }
        if (commandLine.hasOption(COMPRESSION_OPTION)) {
            builder.useCompression(true);
        } else {
            builder.useCompression(false);
        }
        if (commandLine.hasOption(PEER_PERSISTENCE_FILE_OPTION)) {
            builder.peerPersistenceFile(new File(commandLine.getOptionValue(PEER_PERSISTENCE_FILE_OPTION)));
        }
        if (commandLine.hasOption(BATCH_COUNT_OPTION)) {
            builder.requestBatchCount(Integer.parseInt(commandLine.getOptionValue(BATCH_COUNT_OPTION)));
        }
        if (commandLine.hasOption(BATCH_SIZE_OPTION)) {
            builder.requestBatchSize(Long.parseLong(commandLine.getOptionValue(BATCH_SIZE_OPTION)));
        }
        if (commandLine.hasOption(BATCH_DURATION_OPTION)) {
            builder.requestBatchDuration(FormatUtils.getTimeDuration(commandLine.getOptionValue(BATCH_DURATION_OPTION), TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        }
        if (commandLine.hasOption(PROXY_HOST_OPTION)) {
            builder.httpProxy(new HttpProxy(commandLine.getOptionValue(PROXY_HOST_OPTION), Integer.parseInt(commandLine.getOptionValue(PROXY_PORT_OPTION, PROXY_PORT_OPTION_DEFAULT)),
                    commandLine.getOptionValue(PROXY_USERNAME_OPTION), commandLine.getOptionValue(PROXY_PASSWORD_OPTION)));
        }
        builder.transportProtocol(SiteToSiteTransportProtocol.valueOf(commandLine.getOptionValue(TRANSPORT_PROTOCOL_OPTION, TRANSPORT_PROTOCOL_OPTION_DEFAULT).toUpperCase()));
        TransferDirection transferDirection = TransferDirection.valueOf(commandLine.getOptionValue(DIRECTION_OPTION, DIRECTION_OPTION_DEFAULT));
        return new CliParse() {
            @Override
            public SiteToSiteClient.Builder getBuilder() {
                return builder;
            }

            @Override
            public TransferDirection getTransferDirection() {
                return transferDirection;
            }
        };
    }

    public static void main(String[] args) {
        // Make IO redirection useful
        PrintStream output = System.out;
        System.setOut(System.err);
        Options options = new Options();
        try {
            CliParse cliParse = parseCli(options, args);
            try (SiteToSiteClient siteToSiteClient = cliParse.getBuilder().build()) {
                if (cliParse.getTransferDirection() == TransferDirection.SEND) {
                    new SiteToSiteSender(siteToSiteClient, System.in).sendFiles();
                } else {
                    new SiteToSiteReceiver(siteToSiteClient, output).receiveFiles();
                }
            }
        } catch (Exception e) {
            printUsage(e.getMessage(), options);
            e.printStackTrace();
        }
    }

    /**
     * Combines a SiteToSiteClient.Builder and TransferDirection into a return value for parseCli
     */
    public interface CliParse {
        SiteToSiteClient.Builder getBuilder();

        TransferDirection getTransferDirection();
    }
}
