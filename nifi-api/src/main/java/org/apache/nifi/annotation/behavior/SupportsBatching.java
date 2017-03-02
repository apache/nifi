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
 * Marker annotation a Processor implementation can use to indicate that users
 * should be able to supply a Batch Duration for the Processor. If a Processor
 * uses this annotation, it is allowing the Framework to batch
 * {@link org.apache.nifi.processor.ProcessSession ProcessSession}s' commits, as well as
 * allowing the Framework to return the same ProcessSession multiple times from
 * subsequent calls to
 * {@link org.apache.nifi.processor.ProcessSessionFactory ProcessSessionFactory}.
 * {@link org.apache.nifi.processor.ProcessSessionFactory#createSession() createSession()}.
 *
 * When this Annotation is used, it is important to note that calls to
 * {@link org.apache.nifi.processor.ProcessSession#commit() ProcessSession.commit()} may
 * not provide a guarantee that the data has been safely stored in NiFi's
 * Content Repository or FlowFile Repository. Therefore, it is not appropriate,
 * for instance, to use this annotation if the Processor will call
 * ProcessSession.commit() to ensure data is persisted before deleting the data
 * from a remote source.
 *
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface SupportsBatching {

}
