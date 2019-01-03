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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.fn.core.FnFlow;
import org.apache.nifi.fn.core.FnFlowFile;
import org.apache.nifi.fn.runtimes.OpenWhisk.NiFiFnOpenWhiskAction;
import org.apache.nifi.fn.runtimes.YARN.YARNServiceUtil;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Program {

    public static final String RUN_FROM_REGISTRY = "RunFromRegistry";
    public static final String RUN_YARN_SERVICE_FROM_REGISTRY = "RunYARNServiceFromRegistry";
    public static final String RUN_OPENWHISK_ACTION_SERVER = "RunOpenwhiskActionServer";

    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException, ProcessorInstantiationException, NiFiRegistryException, IOException, InterruptedException, InitializationException {
        if(args.length == 0) {
            printUsage();
            System.exit(1);
        } else if(args[0].equals(RUN_FROM_REGISTRY) && (args[1].equals("Once") || args[1].equals("Continuous")) && args.length >= 4) {

            boolean once = args[1].equals("Once");
            FnFlow flow;
            if(args[2].equals("--file")){
                String json = new String(Files.readAllBytes(Paths.get(args[3])));
                JsonObject config = new JsonParser().parse(json).getAsJsonObject();
                flow = FnFlow.createAndEnqueueFromJSON(config);
            } else if (args[2].equals("--json")){
                JsonObject config = new JsonParser().parse(args[3]).getAsJsonObject();
                flow = FnFlow.createAndEnqueueFromJSON(config);
            } else if(args.length >= 5) {
                //Initialize flow
                String registryUrl = args[2];
                String bucketID = args[3];
                String flowID = args[4];
                Map<VariableDescriptor, String> inputVariables = new HashMap<>();

                String[] variables = args[5].split(";");
                for (String v : variables) {
                    String[] tokens = v.split("-");
                    inputVariables.put(new VariableDescriptor(tokens[0]), tokens[1]);
                }

                String[] failureOutputPorts = args.length >= 7 ? args[6].split(";") : new String[]{};
                flow = new FnFlow(registryUrl, bucketID, flowID, () -> inputVariables, Arrays.asList(failureOutputPorts), true);

                //Enqueue all provided flow files
                if (7 < args.length) {
                    int i = 7;
                    while (i < args.length) {
                        Map<String, String> attributes = new HashMap<>();
                        byte[] content = {};

                        String[] attributesArr = args[i].split(";");
                        for (String v : attributesArr) {
                            String[] tokens = v.split("-");
                            if (tokens[0].equals(FnFlow.CONTENT))
                                content = tokens[1].getBytes();
                            else
                                attributes.put(tokens[0], tokens[1]);
                        }
                        flow.enqueueFlowFile(content, attributes);
                        i++;
                    }
                }
            } else {
                System.out.println("Invalid input: "+String.join(",",args));
                printUsage();
                System.exit(1);
                return;
            }

            //Run Flow
            Queue<FnFlowFile> outputFlowFiles = new LinkedList<>();
            //run flow once or forever
            boolean successful;
            if (once)
                successful = flow.runOnce(outputFlowFiles);
            else
                successful = flow.run(outputFlowFiles);//Run forever

            if(!successful) {
                System.out.println("Flow Failed");
                outputFlowFiles.forEach(f->System.out.println(f.toStringFull()));
                System.exit(1);
            } else {
                System.out.println("Flow Succeeded");
                outputFlowFiles.forEach(f->System.out.println(f.toStringFull()));
            }

        }else if(args[0].equals(RUN_YARN_SERVICE_FROM_REGISTRY) && args.length >= 7){
            String YARNUrl = args[1];
            String imageName = args[2];
            String serviceName = args[3];
            int numberOfContainers = Integer.parseInt(args[4]);
            List<String> launchCommand = Arrays.asList(RUN_FROM_REGISTRY,"Continuous");

            if(args[5].equals("--file")){
                launchCommand.add("--json");
                launchCommand.add(new String(Files.readAllBytes(Paths.get(args[6]))));
            } else if (args[5].equals("--json")){
                launchCommand.add("--json");
                launchCommand.add(args[6]);
            } if(args.length >= 9) {
                for(int i = 5; i < args.length; i++){
                    launchCommand.add(args[i]);
                }
            } else {
                System.out.println("Invalid input: "+String.join(",",args));
                printUsage();
                System.exit(1);
            }

            StringBuilder message = new StringBuilder();
            YARNServiceUtil yarnServiceUtil = new YARNServiceUtil(YARNUrl, imageName);
            yarnServiceUtil.launchYARNService(serviceName, numberOfContainers, launchCommand.toArray(new String[0]), message);
            System.out.println(message);
        } else if (args[0].equals(RUN_OPENWHISK_ACTION_SERVER) && args.length == 2){
            NiFiFnOpenWhiskAction action = new NiFiFnOpenWhiskAction(Integer.parseInt(args[1]));
            action.start();
        }else{
            System.out.println("Invalid input: "+String.join(",",args));
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage(){
        System.out.println("Usage:");
        System.out.println("    1) "+RUN_FROM_REGISTRY+" [Once|Continuous] <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       "+RUN_FROM_REGISTRY+" [Once|Continuous] --json <JSON>");
        System.out.println("       "+RUN_FROM_REGISTRY+" [Once|Continuous] --file <File Name>");
        System.out.println();
        System.out.println("    2) "+RUN_YARN_SERVICE_FROM_REGISTRY+"        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> \\");
        System.out.println("                                               <NiFi registry URL> <Bucket ID> <Flow ID> <Input Variables> [<Failure Output Ports>] [<Input FlowFile>]");
        System.out.println("       "+RUN_YARN_SERVICE_FROM_REGISTRY+"        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --json <JSON>");
        System.out.println("       "+RUN_YARN_SERVICE_FROM_REGISTRY+"        <YARN RM URL> <Docker Image Name> <Service Name> <# of Containers> --file <File Name>");
        System.out.println();
        System.out.println("    3) "+RUN_OPENWHISK_ACTION_SERVER+"          <Port>");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("    1) "+RUN_FROM_REGISTRY+" Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\" \"absolute.path-/tmp/nififn/input/;filename-test2.txt\"");
        System.out.println("    2) "+RUN_FROM_REGISTRY+" Once http://172.0.0.1:61080 e53b8a0d-5c85-4fcd-912a-1c549a586c83 6cf8277a-c402-4957-8623-0fa9890dd45d \\");
        System.out.println("             \"DestinationDirectory-/tmp/nififn/output2/\" \"f25c9204-6c95-3aa9-b0a8-c556f5f61849\" \"absolute.path-/tmp/nififn/input/;filename-test.txt\"");
        System.out.println("    3) "+RUN_YARN_SERVICE_FROM_REGISTRY+" http://127.0.0.1:8088 nifi-fn:latest kafka-to-solr 3 --file kafka-to-solr.json");
        System.out.println("    4) "+RUN_OPENWHISK_ACTION_SERVER+" 8080");
        System.out.println();
        System.out.println("Notes:");
        System.out.println("    1) <Input Variables> will be split on ';' and '-' then injected into the flow using the variable registry interface.");
        System.out.println("    2) <Failure Output Ports> will be split on ';'. FlowFiles routed to matching output ports will immediately fail the flow.");
        System.out.println("    3) <Input FlowFile> will be split on ';' and '-' then injected into the flow using the \""+FnFlow.CONTENT+"\" field as the FlowFile content.");
        System.out.println("    4) Multiple <Input FlowFile> arguments can be provided.");
        System.out.println("    5) The configuration file must be in JSON format. ");
        System.out.println("    6) When providing configurations via JSON, the following attributes must be provided: "+ FnFlow.REGISTRY+", "+ FnFlow.BUCKETID+", "+ FnFlow.FLOWID+".");
        System.out.println("          All other attributes will be passed to the flow using the variable registry interface");
        System.out.println();
    }
}
