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
package org.apache.nifi.stateless.core;


//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//import org.apache.nifi.bundle.Bundle;
//import org.apache.nifi.bundle.BundleCoordinate;
//import org.apache.nifi.controller.exception.ProcessorInstantiationException;
//import org.apache.nifi.controller.serialization.FlowEncodingVersion;
//import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
//import org.apache.nifi.encrypt.StringEncryptor;
//import org.apache.nifi.nar.ExtensionManager;
//import org.apache.nifi.nar.NarUnpacker;
//import org.apache.nifi.parameter.Parameter;
//import org.apache.nifi.parameter.ParameterContext;
//import org.apache.nifi.registry.VariableDescriptor;
//import org.apache.nifi.reporting.InitializationException;
//import org.apache.nifi.stateless.bootstrap.ExtensionDiscovery;
//import org.apache.nifi.util.NiFiProperties;
//import org.apache.nifi.web.api.dto.ProcessGroupDTO;
//import org.glassfish.jersey.internal.jsr166.Flow;
//import org.w3c.dom.Document;
//import org.w3c.dom.Element;
//
//import javax.net.ssl.SSLContext;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.net.URLClassLoader;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.*;
//
//import static org.mockito.ArgumentMatchers.anyString;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;

public class InitFromFlowXmlIT {

//    private NiFiProperties getNiFiProperties() {
//        final NiFiProperties nifiProperties = mock(NiFiProperties.class);
//        when(nifiProperties.getProperty(StringEncryptor.NF_SENSITIVE_PROPS_ALGORITHM)).thenReturn("PBEWITHMD5AND256BITAES-CBC-OPENSSL");
//        when(nifiProperties.getProperty(StringEncryptor.NF_SENSITIVE_PROPS_PROVIDER)).thenReturn("BC");
//        when(nifiProperties.getProperty(anyString(), anyString())).then(invocation -> invocation.getArgument(1));
//        return nifiProperties;
//    }
//
//    private static URL[] toURLs(final File[] files) throws MalformedURLException {
//        final List<URL> urls = new ArrayList<>();
//        for (final File file : files) {
//            urls.add(file.toURI().toURL());
//        }
//
//        return urls.toArray(new URL[0]);
//    }

//    @org.junit.Test
//    public void testScenario1_Test() throws IOException, ProcessorInstantiationException, InitializationException {

//        final File narWorkingDir = new File("../../nifi-docker/dockermaven-stateless/target/work");
//        final File statelessLibDir = new File("../../nifi-stateless/nifi-stateless-core/target/nifi-stateless-lib");
//
//        final File statelessCoreWorkingDirectory;
//        try {
//            statelessCoreWorkingDirectory = Objects.requireNonNull(narWorkingDir.listFiles(file -> file.getName().startsWith("nifi-stateless")))[0];
//        } catch(Exception ex) {
//            throw new FileNotFoundException("Could not find core stateless dependencies in the working directory <" + narWorkingDir + ">");
//        }
//
//        final File bundledDependenciesDir = new File(statelessCoreWorkingDirectory, NarUnpacker.BUNDLED_DEPENDENCIES_DIRECTORY);
//        final File[] statelessCoreFiles = bundledDependenciesDir.listFiles();
//        if (statelessCoreFiles == null) {
//            throw new IOException("Could not obtain listing of NiFi-Stateless NAR's bundled dependencies in working directory <" + bundledDependenciesDir + ">");
//        }
//        final URL[] statelessCoreUrls = toURLs(statelessCoreFiles);
//
//        final File[] jarFiles = statelessLibDir.listFiles(file -> file.getName().endsWith(".jar"));
//        if (jarFiles == null) {
//            throw new IOException("Could not obtain listing of NiFi-Stateless Lib directory <" + statelessLibDir + ">");
//        }
//
//        final URL[] jarUrls = toURLs(jarFiles);
//
//        final URLClassLoader rootClassLoader = new URLClassLoader(jarUrls);
//        final URLClassLoader statelessCoreClassLoader = new URLClassLoader(statelessCoreUrls, rootClassLoader);
//        Thread.currentThread().setContextClassLoader(statelessCoreClassLoader);
//
//        final List<String> failurePorts = new ArrayList<>();
//        final Set<Parameter> parameters = new HashSet<>();
//        final Map<VariableDescriptor, String> inputVariables = new HashMap<>();
//        final ParameterContext parameterContext = new StatelessParameterContext(parameters);
//
//        final NiFiProperties props = getNiFiProperties();
//        final StringEncryptor encryptor = NiFiPropertiesUtil.createEncryptorFromProperties(props);
//
//        final JsonObject args = new JsonParser().parse("{\"failurePorts\": [\"9058a28c-016c-1000-7ffc-a2fc292f066e\"]}").getAsJsonObject();
//        final SSLContext sslContext = StatelessFlow.getSSLContext(args);
//
//        // parse flow.xml.gz
//        final Path flowXmlPath = Paths.get("src/test/resources/flow.xml.gz");
//        final Document flow = FlowXmlUtil.readFlowFromDisk(flowXmlPath);
//
//        // grab the root PG Element
//        final Element rootElement = flow.getDocumentElement();
//        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(rootElement);
//        final Element rootGroupElement = (Element) rootElement.getElementsByTagName("rootGroup").item(0);
//        final ProcessGroupDTO rootProcessGroup = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, encryptor, encodingVersion);
//
//        // load nar extensions
//        final ExtensionManager extensionManager = ExtensionDiscovery.discover(narWorkingDir, Thread.currentThread().getContextClassLoader());
//
//        // debug
////        for (Class c : extensionManager.getTypes(new BundleCoordinate("org.apache.nifi", "nifi-standard-nar", "1.10.0-SNAPSHOT"))) {
////            System.out.println(c.toString());
////        }
//
//        // test StatelessFlow initialization
//        new StatelessFlow(rootProcessGroup, extensionManager, () -> inputVariables, failurePorts, false, sslContext, parameterContext);
//    }
}
