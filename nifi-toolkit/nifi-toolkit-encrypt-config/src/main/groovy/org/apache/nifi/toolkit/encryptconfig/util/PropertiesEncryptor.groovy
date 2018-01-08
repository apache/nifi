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
package org.apache.nifi.toolkit.encryptconfig.util

import groovy.io.GroovyPrintWriter
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.PropertiesConfigurationLayout
import org.apache.commons.configuration2.builder.fluent.Configurations
import org.apache.nifi.properties.SensitivePropertyProvider
import org.apache.nifi.util.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.regex.Pattern

class PropertiesEncryptor {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesEncryptor.class)

    private static final String SUPPORTED_PROPERTY_FILE_REGEX = /^\s*nifi\.[-.\w\s]+\s*=/
    protected static final String PROPERTY_PART_DELIMINATOR = "."
    protected static final String PROTECTION_ID_PROPERTY_SUFFIX = "protected"

    protected SensitivePropertyProvider encryptionProvider
    protected SensitivePropertyProvider decryptionProvider

    PropertiesEncryptor(SensitivePropertyProvider encryptionProvider, SensitivePropertyProvider decryptionProvider) {
        this.encryptionProvider = encryptionProvider
        this.decryptionProvider = decryptionProvider
    }

    static boolean supportsFile(String filePath) {
        try {
            File file = new File(filePath)
            if (!ToolUtilities.canRead(file)) {
                return false
            }
            Pattern p = Pattern.compile(SUPPORTED_PROPERTY_FILE_REGEX);
            return file.readLines().any { it =~ SUPPORTED_PROPERTY_FILE_REGEX }
        } catch (Throwable ignored) {
            return false
        }
    }

    static Properties loadFile(String filePath) throws IOException {

        Properties rawProperties
        File inputPropertiesFile = new File(filePath)

        if (ToolUtilities.canRead(inputPropertiesFile)) {
            rawProperties = new Properties()
            inputPropertiesFile.withReader { reader ->
                rawProperties.load(reader)
            }
        } else {
            throw new IOException("The file at ${filePath} must exist and be readable by the user running this tool")
        }

        return rawProperties

    }

    Properties decrypt(final Properties properties) {

        Set<String> propertiesToSkip = getProtectionIdPropertyKeys(properties)
        Map<String, String> propertiesToDecrypt = getProtectedPropertyKeys(properties)

        if (propertiesToDecrypt.isEmpty()) {
            return properties
        }

        if (decryptionProvider == null) {
            throw new IllegalStateException("Decryption capability not supported without provider. " +
                    "Usually this means a decryption password / key was not provided to the tool.")
        }

        String supportedDecryptionScheme = decryptionProvider.getIdentifierKey()
        if (supportedDecryptionScheme) {
            propertiesToDecrypt.entrySet().each { entry ->
                if (!supportedDecryptionScheme.equals(entry.getValue())) {
                    throw new IllegalStateException("Decryption capability not supported by this tool. " +
                            "This tool supports ${supportedDecryptionScheme}, but this properties file contains " +
                            "${entry.getKey()} protected by ${entry.getValue()}")
                }
            }
        }

        Properties unprotectedProperties = new Properties()

        for (String propertyName : properties.stringPropertyNames()) {
            String propertyValue = properties.getProperty(propertyName)
            if (propertiesToSkip.contains(propertyName)) {
                continue
            }
            if (propertiesToDecrypt.keySet().contains(propertyName)) {
                String decryptedPropertyValue = decryptionProvider.unprotect(propertyValue)
                unprotectedProperties.setProperty(propertyName, decryptedPropertyValue)
            } else {
                unprotectedProperties.setProperty(propertyName, propertyValue)
            }
        }

        return unprotectedProperties
    }

    Properties encrypt(Properties properties) {
        return encrypt(properties, properties.stringPropertyNames())
    }

    Properties encrypt(final Properties properties, final Set<String> propertiesToEncrypt) {

        if (encryptionProvider == null) {
            throw new IllegalStateException("Input properties is encrypted, but decryption capability is not enabled. " +
                    "Usually this means a decryption password / key was not provided to the tool.")
        }

        logger.debug("Encrypting ${propertiesToEncrypt.size()} properties")

        Properties protectedProperties = new Properties();
        for (String propertyName : properties.stringPropertyNames()) {
            String propertyValue = properties.getProperty(propertyName)
            // empty properties are not encrypted
            if (!StringUtils.isEmpty(propertyValue) && propertiesToEncrypt.contains(propertyName)) {
                String encryptedPropertyValue = encryptionProvider.protect(propertyValue)
                protectedProperties.setProperty(propertyName, encryptedPropertyValue)
                protectedProperties.setProperty(protectionPropertyForProperty(propertyName), encryptionProvider.getIdentifierKey())
            } else {
                protectedProperties.setProperty(propertyName, propertyValue)
            }
        }

        return protectedProperties
    }

    void write(Properties updatedProperties, String outputFilePath, String inputFilePath) {
        if (!outputFilePath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty file path")
        }
        File outputPropertiesFile = new File(outputFilePath)

        if (ToolUtilities.isSafeToWrite(outputPropertiesFile)) {
            String serializedProperties = serializePropertiesAndPreserveFormatIfPossible(updatedProperties, inputFilePath)
            outputPropertiesFile.text = serializedProperties
        } else {
            throw new IOException("The file at ${outputFilePath} must be writable by the user running this tool")
        }
    }

    private String serializePropertiesAndPreserveFormatIfPossible(Properties updatedProperties, String inputFilePath) {
        List<String> linesToPersist
        File inputPropertiesFile = new File(inputFilePath)
        if (ToolUtilities.canRead(inputPropertiesFile)) {
            // Instead of just writing the Properties instance to a properties file,
            // this method attempts to maintain the structure of the original file and preserves comments
            linesToPersist = serializePropertiesAndPreserveFormat(updatedProperties, inputPropertiesFile)
        } else {
            linesToPersist = serializeProperties(updatedProperties)
        }
        return linesToPersist.join("\n")
    }

    private List<String> serializePropertiesAndPreserveFormat(Properties properties, File originalPropertiesFile) {
        Configurations configurations = new Configurations()
        try {
            PropertiesConfiguration originalPropertiesConfiguration = configurations.properties(originalPropertiesFile)
            def keysToAdd = properties.keySet().findAll { !originalPropertiesConfiguration.containsKey(it.toString()) }
            def keysToUpdate = properties.keySet().findAll {
                !keysToAdd.contains(it) &&
                        properties.getProperty(it.toString()) != originalPropertiesConfiguration.getProperty(it.toString())
            }
            def keysToRemove = originalPropertiesConfiguration.getKeys().findAll {!properties.containsKey(it) }

            keysToUpdate.forEach {
                originalPropertiesConfiguration.setProperty(it.toString(), properties.getProperty(it.toString()))
            }
            keysToRemove.forEach {
                originalPropertiesConfiguration.clearProperty(it.toString())
            }
            boolean isFirst = true
            keysToAdd.sort().forEach {
                originalPropertiesConfiguration.setProperty(it.toString(), properties.getProperty(it.toString()))
                if (isFirst) {
                    originalPropertiesConfiguration.getLayout().setBlancLinesBefore(it.toString(), 1)
                    originalPropertiesConfiguration.getLayout().setComment(it.toString(), "protection properties")
                    isFirst = false
                }
            }

            OutputStream out = new ByteArrayOutputStream()
            Writer writer = new GroovyPrintWriter(out)

            PropertiesConfigurationLayout layout = originalPropertiesConfiguration.getLayout()
            layout.setGlobalSeparator("=")
            layout.save(originalPropertiesConfiguration, writer)

            writer.flush()
            List<String> lines = out.toString().split("\n")

            return lines
        } catch(Exception e) {
            throw new RuntimeException("Error serializing properties.", e)
        }
    }

    private List<String> serializeProperties(final Properties properties) {
        OutputStream out = new ByteArrayOutputStream()
        Writer writer = new GroovyPrintWriter(out)

        properties.store(writer, null)
        writer.flush()
        List<String> lines = out.toString().split("\n")

        return lines
    }

    /**
     * Returns a Map of the keys identifying properties that are currently protected
     * and the protection identifier for each. The protection
     *
     * @return the Map of protected property keys and the protection identifier for each
     */
    private static Map<String, String> getProtectedPropertyKeys(Properties properties) {
        Map<String, String> protectedProperties = new HashMap<>();
        properties.stringPropertyNames().forEach({ key ->
            String protectionKey = protectionPropertyForProperty(key)
            String protectionIdentifier = properties.getProperty(protectionKey)
            if (protectionIdentifier) {
                protectedProperties.put(key, protectionIdentifier)
            }
        })
        return protectedProperties
    }

    private static Set<String> getProtectionIdPropertyKeys(Properties properties) {
        Set<String> protectedProperties = properties.stringPropertyNames().findAll { key ->
            key.endsWith(PROPERTY_PART_DELIMINATOR + PROTECTION_ID_PROPERTY_SUFFIX)
        }
        return protectedProperties;
    }

    private static String protectionPropertyForProperty(String propertyName) {
        return propertyName + PROPERTY_PART_DELIMINATOR + PROTECTION_ID_PROPERTY_SUFFIX
    }

    private static String propertyForProtectionProperty(String protectionPropertyName) {
        String[] propertyNameParts = protectionPropertyName.split(Pattern.quote(PROPERTY_PART_DELIMINATOR))
        if (propertyNameParts.length >= 2 && PROTECTION_ID_PROPERTY_SUFFIX.equals(propertyNameParts[-1])) {
            return propertyNameParts[(0..-2)].join(PROPERTY_PART_DELIMINATOR)
        }
        return null
    }



}
