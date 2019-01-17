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
package org.apache.nifi.fn.runtimes;

import org.apache.nifi.fn.bootstrap.InMemoryFlowFile;
import org.apache.nifi.fn.bootstrap.RunnableFlow;
import org.apache.nifi.fn.bootstrap.RunnableFlowFactory;
import org.apache.nifi.fn.core.FnFlow;
import org.apache.nifi.fn.runtimes.openwhisk.NiFiFnOpenWhiskAction;
import org.apache.nifi.fn.runtimes.yarn.YARNServiceUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Program {

    public static final String RUN_FROM_REGISTRY = "RunFromRegistry";
    public static final String RUN_YARN_SERVICE_FROM_REGISTRY = "RunYARNServiceFromRegistry";
    public static final String RUN_OPENWHISK_ACTION_SERVER = "RunOpenwhiskActionServer";


    public static void launch(final String[] args, final ClassLoader systemClassLoader, final File narWorkingDirectory) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        } else if (args[0].equals(RUN_FROM_REGISTRY) && (args[1].equalsIgnoreCase("Once") || args[1].equalsIgnoreCase("Continuous")) && args.length >= 4) {
            runLocal(args, systemClassLoader, narWorkingDirectory);
        } else if (args[0].equals(RUN_YARN_SERVICE_FROM_REGISTRY) && args.length >= 7) {
            runOnYarn(args);
        } else if (args[0].equals(RUN_OPENWHISK_ACTION_SERVER) && args.length == 2) {
            runOnOpenWhisk(args);
        } else {
            System.out.println("Invalid input: " + String.join(",", args));
            printUsage();
            System.exit(1);
        }
    }

    private static void runOnOpenWhisk(final String[] args) throws IOException {
        NiFiFnOpenWhiskAction action = new NiFiFnOpenWhiskAction(Integer.parseInt(args[1]));
        action.start();
    }

    private static void runOnYarn(final String[] args) throws IOException {
        String YARNUrl = args[1];
        String imageName = args[2];
        String serviceName = args[3];
        int numberOfContainers = Integer.parseInt(args[4]);
        List<String> launchCommand = Arrays.asList(RUN_FROM_REGISTRY, "Continuous");

        if (args[5].equals("--file")) {
            launchCommand.add("--json");
            launchCommand.add(new String(Files.readAllBytes(Paths.get(args[6]))));
        } else if (args[5].equals("--json")) {
            launchCommand.add("--json");
            launchCommand.add(args[6]);
        }

        if (args.length >= 9) {
            for (int i = 5; i < args.length; i++) {
                launchCommand.add(args[i]);
            }
        } else {
            System.out.println("Invalid input: " + String.join(",", args));
            printUsage();
            System.exit(1);
        }

        StringBuilder message = new StringBuilder();
        YARNServiceUtil yarnServiceUtil = new YARNServiceUtil(YARNUrl, imageName);
        yarnServiceUtil.launchYARNService(serviceName, numberOfContainers, launchCommand.toArray(new String[0]), message);
        System.out.println(message);
    }

    private static void runLocal(final String[] args, final ClassLoader systemClassLoader, final File narWorkingDirectory) throws Exception {
        final boolean once = args[1].equalsIgnoreCase("Once");

        final RunnableFlow flow;
        if (args[2].equals("--file")) {
            flow = RunnableFlowFactory.fromJsonFile(args[3], systemClassLoader, narWorkingDirectory);
        } else if (args[2].equals("--json")) {
            flow = RunnableFlowFactory.fromJson(args[3]);
        } else if (args.length >= 5) {
            flow = RunnableFlowFactory.fromCommandLineArgs(args);
        } else {
            System.out.println("Invalid input: " + String.join(",", args));
            printUsage();
            System.exit(1);
            return;
        }

        // Run Flow
        final Queue<InMemoryFlowFile> outputFlowFiles = new LinkedList<>();
        final boolean successful;
        if (once) {
            successful = flow.runOnce(outputFlowFiles);
        } else {
            successful = flow.run(outputFlowFiles);  //Run forever
        }

        if (successful) {
            System.out.println("Flow Succeeded");
            outputFlowFiles.forEach(f -> System.out.println(f.toStringFull()));
        } else {
            System.out.println("Flow Failed");
            outputFlowFiles.forEach(f -> System.out.println(f.toStringFull()));
            System.exit(1);
        }
    }


    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("    1) " + RUN_FROM_REGISTRY + " [Once|Continuous] <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       " + RUN_FROM_REGISTRY + " [Once|Continuous] --json <JSON>");
        System.out.println("       " + RUN_FROM_REGISTRY + " [Once|Continuous] --file <File Name>");
        System.out.println();
        System.out.println("    2) " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> \\");
        System.out.println("                                               <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --json <JSON>");
        System.out.println("       " + RUN_YARN_SERVICE_FROM_REGISTRY + "        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --file <File Name>");
        System.out.println();
        System.out.println("    3) " + RUN_OPENWHISK_ACTION_SERVER + "          <Port>");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("    1) " + RUN_FROM_REGISTRY + " Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\" \"absolute.path-/tmp/nififn/input/;" +
            "filename-test2.txt\"");
        System.out.println("    2) " + RUN_FROM_REGISTRY + " Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"f25c9204-6c95-3aa9-b0a8-c556f5f61849\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\"");
        System.out.println("    3) " + RUN_YARN_SERVICE_FROM_REGISTRY + " http://127.0.0.1:8088 nifi-fn:latest kafka-to-solr 3 --file kafka-to-solr.json");
        System.out.println("    4) " + RUN_OPENWHISK_ACTION_SERVER + " 8080");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("    1) <Input Variables> will be split on ';' and '-' then injected into the flow using the variable registry interface.");
        System.out.println("    2) <Failure Output Ports> will be split on ';'. FlowFiles routed to matching output ports will immediately fail the flow.");
        System.out.println("    3) <Input FlowFile> will be split on ';' and '-' then injected into the flow using the \"" + FnFlow.CONTENT + "\" field as the FlowFile content.");
        System.out.println("    4) Multiple <Input FlowFile> arguments can be provided.");
        System.out.println("    5) The configuration file must be in JSON format. ");
        System.out.println("    6) When providing configurations via JSON, the following attributes must be provided: " + FnFlow.REGISTRY + ", " + FnFlow.BUCKETID + ", " + FnFlow.FLOWID + ".");
        System.out.println("          All other attributes will be passed to the flow using the variable registry interface");
        System.out.println();
    }
}
