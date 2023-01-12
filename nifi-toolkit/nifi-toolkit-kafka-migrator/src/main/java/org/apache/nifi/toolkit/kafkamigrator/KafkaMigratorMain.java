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
package org.apache.nifi.toolkit.kafkamigrator;

import org.apache.nifi.toolkit.kafkamigrator.service.KafkaMigrationService;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.transform.StandardTransformProvider;
import org.w3c.dom.Document;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class KafkaMigratorMain {

    private static void printUsage() {
        System.out.println("This application replaces Kafka processors from version 0.8, 0.9, 0.10 and 0.11 to version 2.0 processors" +
                " in a flow.xml.gz file.");
        System.out.println("\n");
        System.out.println("Usage: kafka-migrator.sh <path to input flow.xml.gz> <path to output flow.xml.gz>" +
                " <use transaction true or false>\n<optional: coma separated kafka brokers in <host>:<port> format. " +
                "Required for version 0.8 processors only>");
    }

    public static void main(final String[] args) throws Exception {
        if (helpRequested(args)) {
            printUsage();
            return;
        }

        final String input = args[0];
        final String output = args[1];
        if (input.equalsIgnoreCase(output)) {
            System.out.println("Input and output files should be different.");
            return;
        }
        final String transaction = args[2];
        if (!(transaction.equalsIgnoreCase("true") || transaction.equalsIgnoreCase("false"))) {
            System.out.println("Transaction argument should be either true or false.");
            return;
        }
        String kafkaBrokers = "";
        if (args.length == 4) {
            if (args[3].matches(".+:\\d+")) {
                kafkaBrokers = args[3];
            } else {
                System.out.println("Kafka Brokers must be in a <host>:<port> format, can be separated by coma. " +
                        "For example: hostname:1234, host:5678");
                return;
            }
        }

        final Map<String, String> arguments = new HashMap<>();
        arguments.put("transaction", transaction);
        arguments.put("kafkaBrokers", kafkaBrokers);

        final InputStream fileStream = Files.newInputStream(Paths.get(input));
        final OutputStream outputStream = Files.newOutputStream(Paths.get(output));
        final InputStream gzipStream = new GZIPInputStream(fileStream);
        final OutputStream gzipOutStream = new GZIPOutputStream(outputStream);

        System.out.println("Using flow=" + input);

        try {
            final DocumentProvider documentProvider = new StandardDocumentProvider();
            final Document document = documentProvider.parse(gzipStream);

            final KafkaMigrationService migrationService = new KafkaMigrationService();
            System.out.println("Replacing processors.");
            migrationService.replaceKafkaProcessors(document, arguments);

            final StreamResult streamResult = new StreamResult(gzipOutStream);
            final StandardTransformProvider transformProvider = new StandardTransformProvider();
            transformProvider.setIndent(true);
            transformProvider.transform(new DOMSource(document), streamResult);
            System.out.println("Replacing completed.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while attempting to parse flow.xml.gz.  Cause: " + e.getCause());
        } finally {
            gzipOutStream.close();
            outputStream.close();
            gzipStream.close();
            fileStream.close();
        }
    }

    private static boolean helpRequested(String[] args) {
        return args.length < 3 || args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help");
    }
}
