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
package org.apache.nifi.security.util;

import java.io.PrintWriter;
import java.io.Writer;

/**
 * Types of security stores and their related Java system properties.
 */
public enum SecurityStoreTypes {

    TRUSTSTORE(
            "javax.net.ssl.trustStore",
            "javax.net.ssl.trustStorePassword",
            "javax.net.ssl.trustStoreType"),
    KEYSTORE(
            "javax.net.ssl.keyStore",
            "javax.net.ssl.keyStorePassword",
            "javax.net.ssl.keyStoreType");

    /**
     * Logs the keystore and truststore Java system property values to the given
     * writer. It logPasswords is true, then the keystore and truststore
     * password property values are logged.
     *
     * @param writer a writer to log to
     *
     * @param logPasswords true if passwords should be logged; false otherwise
     */
    public static void logProperties(final Writer writer,
            final boolean logPasswords) {
        if (writer == null) {
            return;
        }

        PrintWriter pw = new PrintWriter(writer);

        // keystore properties
        pw.println(
                KEYSTORE.getStoreProperty() + " = " + System.getProperty(KEYSTORE.getStoreProperty()));

        if (logPasswords) {
            pw.println(
                    KEYSTORE.getStorePasswordProperty() + " = "
                    + System.getProperty(KEYSTORE.getStoreProperty()));
        }

        pw.println(
                KEYSTORE.getStoreTypeProperty() + " = "
                + System.getProperty(KEYSTORE.getStoreTypeProperty()));

        // truststore properties
        pw.println(
                TRUSTSTORE.getStoreProperty() + " = "
                + System.getProperty(TRUSTSTORE.getStoreProperty()));

        if (logPasswords) {
            pw.println(
                    TRUSTSTORE.getStorePasswordProperty() + " = "
                    + System.getProperty(TRUSTSTORE.getStoreProperty()));
        }

        pw.println(
                TRUSTSTORE.getStoreTypeProperty() + " = "
                + System.getProperty(TRUSTSTORE.getStoreTypeProperty()));
        pw.flush();
    }

    /**
     * the Java system property for setting the keystore (or truststore) path
     */
    private String storeProperty = "";

    /**
     * the Java system property for setting the keystore (or truststore)
     * password
     */
    private String storePasswordProperty = "";

    /**
     * the Java system property for setting the keystore (or truststore) type
     */
    private String storeTypeProperty = "";

    /**
     * Creates an instance.
     *
     * @param storeProperty the Java system property for setting the keystore (
     * or truststore) path
     * @param storePasswordProperty the Java system property for setting the
     * keystore (or truststore) password
     * @param storeTypeProperty the Java system property for setting the
     * keystore (or truststore) type
     */
    SecurityStoreTypes(final String storeProperty,
            final String storePasswordProperty,
            final String storeTypeProperty) {
        this.storeProperty = storeProperty;
        this.storePasswordProperty = storePasswordProperty;
        this.storeTypeProperty = storeTypeProperty;
    }

    /**
     * Returns the keystore (or truststore) property.
     *
     * @return the keystore (or truststore) property
     */
    public String getStoreProperty() {
        return storeProperty;
    }

    /**
     * Returns the keystore (or truststore) password property.
     *
     * @return the keystore (or truststore) password property
     */
    public String getStorePasswordProperty() {
        return storePasswordProperty;
    }

    /**
     * Returns the keystore (or truststore) type property.
     *
     * @return the keystore (or truststore) type property
     */
    public String getStoreTypeProperty() {
        return storeTypeProperty;
    }
}
