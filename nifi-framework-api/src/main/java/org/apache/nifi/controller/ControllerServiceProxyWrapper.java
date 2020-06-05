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

package org.apache.nifi.controller;

/**
 * The purpose of this interface is to help handle the following scenario:
 *
 * A Controller Service method returns a value which is wrapped in a proxy by the framework.
 * Another method of the Controller Service receives the same proxy as an argument. (The Controller Service gets back the object.)
 *  This method expects the argument to be of the concrete type of the real return value.
 *  Since the proxy only preserves the interface of the real return value but not the concrete type, the method fails.
 *
 * To fix this, this interface is added to the proxy (along with the original interface) and the framework will get the real value via {@link #getWrapped()}
 *  so the Controller Service will receive the real object.
 *
 * E.g.:
 *
 * <pre><code>public interface IConnectionProviderService {
 *     IConnection getConnection();
 *     void closeConnection(IConnection);
 * }
 *
 * public class ConnectionProviderServiceImpl {
 *     IConnection getConnection() {
 *         return new SimpleConnection();
 *     }
 *
 *     void closeConnection(IConnection) {
 *         if (connection instanceof SimpleConnection) {
 *             ...
 *         } else {
 *             throw new InvalidArgumentException();
 *         }
 *     }
 * }
 *
 * public class ConnectionUserProcessor {
 *     IConnectionProviderService service; #Set to ConnectionProviderServiceImpl
 *
 *     void onTrigger() {
 *         IConnection connection = service.getConnection();
 *
 *         # 'connection' at this point is a proxy of a 'SimpleConnection' object
 *         # So '(connection instanceof IConnection)' is true, but
 *         # '(connection instanceof SimpleConnection)' is false
 *
 *         ...
 *
 *         service.closeConnection(connection); # !! This would have thrown InvalidArgumentException
 *     }
 * }
 * </code></pre>
 *
 * But why wrap the return value in a proxy in the first place? It is needed to handle the following scenario:
 *
 * A Controller Service method returns an object to a Processor.
 * A method is called on the returned object int the Processor.
 * This method tries to load a class that is in the same package as the return object.
 * Since it tries to use the Processor classloader, it fails.
 *
 * E.g.:
 * <pre><code>package root.interface;
 *
 * public interface IReportService {
 *     IReport getReport();
 * }
 * public interface IReport {
 *     void submit();
 * }
 *
 *
 * package root.service;
 *
 * public class ReportServiceImpl {
 *     IReport getReport() {
 *         return new Report();
 *     }
 * }
 * public class ReportImpl {
 *     void submit() {
 *         Class.forName("roo.service.OtherClass");
 *         ...
 *     }
 * }
 * public class OtherClass {}
 *
 *
 * package root.processor;
 *
 * public class ReportProcessor {
 *     IReportService service; #Set to ReportServiceImpl
 *
 *     void onTrigger() {
 *         IReport report = service.getReport();
 *         ...
 *         report.submit(); # !! This would have thrown ClassNotFoundException
 *     }
 * }
 * </code></pre>
 *
 * So in general there is a barrier between the Controller Service and the Processor (or another Controller Service) due to the fact that they have
 * their own classloaders.
 * When an object crosses the barrier, it needs to be proxied to be able to use its original classloader.
 * Also when it crosses the barrier back again, it needs to be unproxied to preserve specific type information.
 *
 * Note that wrapping may not be necessary if the class is loaded by a classloader that is parent to both the Controller Service and the Processor,
 * as in this case the 'barrier' not really there for instances of that class.
 *
 * @param <T> the type of the wrapped/proxied object
 */
public interface ControllerServiceProxyWrapper<T> {
    /**
     * @return the object that is being wrapped/proxied.
     */
    T getWrapped();
}
