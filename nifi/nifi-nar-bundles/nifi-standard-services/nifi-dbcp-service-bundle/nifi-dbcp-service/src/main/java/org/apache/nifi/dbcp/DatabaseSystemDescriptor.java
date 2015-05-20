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
package org.apache.nifi.dbcp;

import java.text.MessageFormat;

import org.apache.nifi.components.AllowableValue;

/**
 * An immutable object for holding information about a database system.
 *
 */
public class DatabaseSystemDescriptor extends AllowableValue {

    public final String driverClassName;
    public final Integer defaultPort;
    public final String urlTemplate;
    public final boolean internalDriverJar;

    public DatabaseSystemDescriptor(String value, String description, String driverClassName, Integer defaultPort, String urlTemplate, boolean internalDriverJar) {
        super(value, value, description);

        if (defaultPort==null)
            throw new IllegalArgumentException("defaultPort cannot be null");

        this.driverClassName = driverClassName;
        this.defaultPort = defaultPort;
        this.urlTemplate = urlTemplate;
        this.internalDriverJar = internalDriverJar;
    }

    public String buildUrl(String host, Integer port, String dbname) {
        return MessageFormat.format(urlTemplate, host, port.toString(), dbname);
    }

}
