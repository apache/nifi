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
package org.apache.nifi.annotation.behavior;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *  Marker annotation a component can use to indicate that the framework should create a new ClassLoader
 *  for each instance of the component, copying all resources from the component's NARClassLoader to a
 *  new ClassLoader which will only be used by a given instance of the component.
 *
 *  If cloneAncestorResources is set to true, the instance ClassLoader will include ancestor resources up to the
 *  first ClassLoader containing a controller service API referenced by the component, or up to the Jetty NAR.
 *
 *  Example #1 - PutHDFS has this flag set to true and does not reference any controller services, so it will include
 *  resources from nifi-hadoop-nar, nifi-hadoop-libraries-nar, and nifi-standard-services-api-nar, stopping at nifi-jetty-nar.
 *
 *  Example #2 - If PutHDFS referenced an SSLContext and has this flag set to true, then it would include
 *  resources from nifi-hadoop-nar, nifi-hadoop-libraries-nar, and stop before nifi-standard-services-api-nar.
 *
 *  Example #3 - HBaseClientService_1_1_2 does not have this flag set so it defaults to false, and therefore includes
 *  only resources from the nifi-hbase-client-service-1_1_2-nar.
 *
 *  NOTE: When this annotation is used it is important to note that each added instance of the component will increase
 *  the overall memory footprint more than that of a component without this annotation.
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface RequiresInstanceClassLoading {

    boolean cloneAncestorResources() default false;

}
