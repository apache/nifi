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
package org.apache.nifi.fn.bootstrap;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.fn.core.FnFlow;
import org.apache.nifi.fn.core.RegistryUtil;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.reporting.InitializationException;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RunnableFlowFactory {

    public static RunnableFlow fromJson(final String json) throws NiFiRegistryException, InitializationException,
                IOException, ProcessorInstantiationException {
        final JsonObject config = new JsonParser().parse(json).getAsJsonObject();
        return FnFlow.createAndEnqueueFromJSON(config);
    }

    public static RunnableFlow fromJsonFile(final String filename, final ClassLoader systemClassLoader, final File narWorkingDir) throws IOException,
                NiFiRegistryException, ProcessorInstantiationException, InitializationException {
        final String json = new String(Files.readAllBytes(Paths.get(filename)));
        final JsonObject config = new JsonParser().parse(json).getAsJsonObject();
        return FnFlow.createAndEnqueueFromJSON(config, systemClassLoader, narWorkingDir);
    }

    public static RunnableFlow fromCommandLineArgs(final String[] args) throws  InitializationException, IOException, ProcessorInstantiationException, NiFiRegistryException {
        //Initialize flow
        final String registryUrl = args[2];
        final String bucketID = args[3];
        final String flowID = args[4];
        final Map<VariableDescriptor, String> inputVariables = new HashMap<>();

        if (args.length >= 6) {
            final String[] variables = args[5].split(";");
            for (final String v : variables) {
                String[] tokens = v.split("-");
                inputVariables.put(new VariableDescriptor(tokens[0]), tokens[1]);
            }
        }

        final String[] failureOutputPorts = args.length >= 7 ? args[6].split(";") : new String[]{};
        final SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new NiFiRegistryException("Could not get Default SSL Context", e);
        }

        final VersionedFlowSnapshot snapshot = new RegistryUtil(registryUrl, sslContext).getFlowByID(bucketID, flowID);
        final VersionedProcessGroup versionedFlow = snapshot.getFlowContents();

        final ExtensionManager extensionManager = ExtensionDiscovery.discover(new File("./work"), ClassLoader.getSystemClassLoader());
        final FnFlow flow = new FnFlow(versionedFlow, extensionManager, () -> inputVariables, Arrays.asList(failureOutputPorts), true, sslContext);

        // Enqueue all provided flow files
        if (7 < args.length) {
            int i = 7;
            while (i++ < args.length) {
                final Map<String, String> attributes = new HashMap<>();
                byte[] content = {};

                final String[] attributesArr = args[i].split(";");
                for (final String v : attributesArr) {
                    final String[] tokens = v.split("-");
                    if (tokens[0].equals(FnFlow.CONTENT)) {
                        content = tokens[1].getBytes();
                    } else {
                        attributes.put(tokens[0], tokens[1]);
                    }
                }

                flow.enqueueFlowFile(content, attributes);
            }
        }

        return flow;
    }
}
