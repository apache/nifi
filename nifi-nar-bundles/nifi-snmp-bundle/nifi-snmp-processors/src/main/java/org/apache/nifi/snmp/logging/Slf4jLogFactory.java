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
package org.apache.nifi.snmp.logging;

import org.slf4j.LoggerFactory;
import org.snmp4j.log.LogAdapter;
import org.snmp4j.log.LogFactory;

import java.util.Iterator;

public class Slf4jLogFactory extends LogFactory {

    public Slf4jLogFactory() {
    }

    @Override
    protected LogAdapter createLogger(Class c) {
        return new Slf4jLogAdapter(LoggerFactory.getLogger(c.getName()));
    }

    @Override
    protected LogAdapter createLogger(String className) {
        return new Slf4jLogAdapter(LoggerFactory.getLogger(className));
    }

    @Override
    public LogAdapter getRootLogger() {
        return new Slf4jLogAdapter(LoggerFactory.getLogger(""));
    }

    @Override
    public Iterator loggers() {
        return null;
    }
}
