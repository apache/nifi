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
package org.apache.nifi.toolkit.encryptconfig

import org.apache.log4j.LogManager
import org.apache.log4j.PropertyConfigurator
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class EncryptConfigLogger {
    private static final Logger logger = LoggerFactory.getLogger(EncryptConfigLogger.class)

    /**
     * Configures the logger.
     *
     * The nifi-toolkit module uses log4j, which will be configured to append all
     * log output to the system STDERR. The log level can be specified using the verboseEnabled
     * argument. A value of <code>true</code> will set the log level to DEBUG, a value of
     * <code>false</code> will set the log level to INFO.
     *
     * @param verboseEnabled flag to indicate if verbose mode is enabled, which sets the log level to DEBUG
     */
    static configureLogger(boolean verboseEnabled) {

        Properties log4jProps = null
        URL log4jPropsPath = this.getClass().getResource("log4j.properties")
        if (log4jPropsPath) {
            try {
                log4jPropsPath.withReader { reader ->
                    log4jProps = new Properties()
                    log4jProps.load(reader)
                }
            } catch (IOException e) {
                // do nothing, we will fallback to hardcoded defaults below
            }
        }

        if (!log4jProps) {
            log4jProps = defaultProperties()
        }

        if (verboseEnabled) {
            // Override the log level for this package. For this to work as intended, this class must belong
            // to the same package (or a parent package) of all the encrypt-config classes
            log4jProps.put("log4j.logger." + EncryptConfigLogger.class.package.name, "DEBUG")
        }

        LogManager.resetConfiguration()
        PropertyConfigurator.configure(log4jProps)

        if (verboseEnabled) {
            logger.debug("Verbose mode is enabled (goes to stderr by default).")
        }
    }

    /**
     * A copy of the settings in /src/main/resources/log4j.properties, in case that is not on the classpath at runtime
     * @return Properties containing the default properties for Log4j
     */
    static Properties defaultProperties() {
        Properties defaultProperties = new Properties()
        defaultProperties.setProperty("log4j.rootLogger", "INFO,console")
        defaultProperties.setProperty("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
        defaultProperties.setProperty("log4j.appender.console.Target", "System.err")
        defaultProperties.setProperty("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
        defaultProperties.setProperty("log4j.appender.console.layout.ConversionPattern", "%d{yyyy-mm-dd HH:mm:ss} %p %c{1}: %m%n")
        return defaultProperties
    }
}
