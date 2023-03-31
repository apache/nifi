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

package org.apache.nifi.minifi.c2.api.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class C2Properties extends Properties {
    public static final String MINIFI_C2_SERVER_SECURE = "minifi.c2.server.secure";
    public static final String MINIFI_C2_SERVER_KEYSTORE_TYPE = "minifi.c2.server.keystoreType";
    public static final String MINIFI_C2_SERVER_KEYSTORE = "minifi.c2.server.keystore";
    public static final String MINIFI_C2_SERVER_KEYSTORE_PASSWD = "minifi.c2.server.keystorePasswd";
    public static final String MINIFI_C2_SERVER_KEY_PASSWD = "minifi.c2.server.keyPasswd";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE = "minifi.c2.server.truststore";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE_TYPE = "minifi.c2.server.truststoreType";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE_PASSWD = "minifi.c2.server.truststorePasswd";

    private static final C2Properties properties = initProperties();

    private static C2Properties initProperties() {
        C2Properties properties = new C2Properties();
        try (InputStream inputStream = C2Properties.class.getClassLoader().getResourceAsStream("c2.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load c2.properties", e);
        }
        return properties;
    }

    public static C2Properties getInstance() {
        return properties;
    }

    public boolean isSecure() {
        return Boolean.parseBoolean(getProperty(MINIFI_C2_SERVER_SECURE, "false"));
    }
}
