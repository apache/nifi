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
package org.apache.nifi.authorization;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.generated.Authorization;
import org.apache.nifi.authorization.generated.Resource;
import org.apache.nifi.authorization.generated.Resources;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.util.file.monitor.MD5SumMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides identity checks and grants authorities.
 */
public class FileAuthorizer implements Authorizer {

    private static final Logger logger = LoggerFactory.getLogger(FileAuthorizer.class);
    private static final String READ_CODE = "R";
    private static final String WRITE_CODE = "W";
    private static final String USERS_XSD = "/authorizations.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, FileAuthorizer.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private NiFiProperties properties;
    private File authorizationsFile;
    private File restoreAuthorizationsFile;
    private SynchronousFileWatcher fileWatcher;
    private ScheduledExecutorService fileWatcherExecutorService;

    private final AtomicReference<Map<String, Map<String, Set<RequestAction>>>> authorizations = new AtomicReference<>();

    @Override
    public void initialize(final AuthorizerInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(final AuthorizerConfigurationContext configurationContext) throws ProviderCreationException {
        try {
            final PropertyValue authorizationsPath = configurationContext.getProperty("Authorizations File");
            if (StringUtils.isBlank(authorizationsPath.getValue())) {
                throw new ProviderCreationException("The authorizations file must be specified.");
            }

            // get the authorizations file and ensure it exists
            authorizationsFile = new File(authorizationsPath.getValue());
            if (!authorizationsFile.exists()) {
                throw new ProviderCreationException("The authorizations file must exist.");
            }

            final File authorizationsFileDirectory = authorizationsFile.getAbsoluteFile().getParentFile();

            // the restore directory is optional and may be null
            final File restoreDirectory = properties.getRestoreDirectory();
            if (restoreDirectory != null) {
                // sanity check that restore directory is a directory, creating it if necessary
                FileUtils.ensureDirectoryExistAndCanAccess(restoreDirectory);

                // check that restore directory is not the same as the primary directory
                if (authorizationsFileDirectory.getAbsolutePath().equals(restoreDirectory.getAbsolutePath())) {
                    throw new ProviderCreationException(String.format("Authorizations file directory '%s' is the same as restore directory '%s' ",
                            authorizationsFileDirectory.getAbsolutePath(), restoreDirectory.getAbsolutePath()));
                }

                // the restore copy will have same file name, but reside in a different directory
                restoreAuthorizationsFile = new File(restoreDirectory, authorizationsFile.getName());

                try {
                    // sync the primary copy with the restore copy
                    FileUtils.syncWithRestore(authorizationsFile, restoreAuthorizationsFile, logger);
                } catch (final IOException | IllegalStateException ioe) {
                    throw new ProviderCreationException(ioe);
                }
            }

            final PropertyValue rawReloadInterval = configurationContext.getProperty("Reload Interval");

            long reloadInterval;
            try {
                reloadInterval = rawReloadInterval.asTimePeriod(TimeUnit.MILLISECONDS);
            } catch (final Exception iae) {
                logger.info(String.format("Unable to interpret reload interval '%s'. Using default of 30 seconds.", rawReloadInterval));
                reloadInterval = 30000L;
            }

            // reload the authorizations
            reload();

            // watch the file for modifications
            fileWatcher = new SynchronousFileWatcher(authorizationsFile.toPath(), new MD5SumMonitor());
            fileWatcherExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(final Runnable r) {
                    return new Thread(r, "Authorization File Reload Thread");
                }
            });
            fileWatcherExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (fileWatcher.checkAndReset()) {
                            reload();
                        }
                    } catch (final Exception e) {
                        logger.warn("Unable to reload Authorizations file do to: " + e, e);
                    }
                }
            }, reloadInterval, reloadInterval, TimeUnit.MILLISECONDS);
        } catch (IOException | ProviderCreationException | SAXException | JAXBException | IllegalStateException e) {
            throw new ProviderCreationException(e);
        }

    }

    @Override
    public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
        // get the current authorizations
        final Map<String, Map<String, Set<RequestAction>>> currentAuthorizations = authorizations.get();

        // get the requested resource
        final org.apache.nifi.authorization.Resource requestedResource = request.getResource();

        // get the authorizations for the requested resources
        final Map<String, Set<RequestAction>> resourceAuthorizations = currentAuthorizations.get(requestedResource.getIdentifier());

        // ensure the resource has authorizations
        if (resourceAuthorizations == null) {
            return AuthorizationResult.resourceNotFound();
        }

        // get the user authorizations
        final Set<RequestAction> userAuthorizations = resourceAuthorizations.get(request.getIdentity());

        // ensure the user has authorizations
        if (userAuthorizations == null) {
            return AuthorizationResult.denied();
        }

        // ensure the appropriate response
        if (userAuthorizations.contains(request.getAction())) {
            return AuthorizationResult.approved();
        } else {
            return AuthorizationResult.denied();
        }
    }

    /**
     * Reloads the authorized users file.
     *
     * @throws SAXException             Unable to reload the authorized users file
     * @throws JAXBException            Unable to reload the authorized users file
     * @throws IOException              Unable to sync file with restore
     * @throws IllegalStateException    Unable to sync file with restore
     */
    private void reload() throws SAXException, JAXBException, IOException, IllegalStateException {
        // find the schema
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(FileAuthorizer.class.getResource(USERS_XSD));

        // attempt to unmarshal
        final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
        unmarshaller.setSchema(schema);
        final JAXBElement<Resources> element = unmarshaller.unmarshal(new StreamSource(authorizationsFile), Resources.class);
        final Resources resources = element.getValue();

        // new authorizations
        final Map<String, Map<String, Set<RequestAction>>> newAuthorizations = new HashMap<>();

        // load the new authorizations
        for (final Resource authorizedResource : resources.getResource()) {
            final String identifier = authorizedResource.getIdentifier();

            // ensure the entry exists
            if (!newAuthorizations.containsKey(identifier)) {
                newAuthorizations.put(identifier, new HashMap<String, Set<RequestAction>>());
            }

            // go through each authorization
            for (final Authorization authorization : authorizedResource.getAuthorization()) {
                final String identity = authorization.getIdentity();

                // get the authorizations for this resource
                final Map<String, Set<RequestAction>> resourceAuthorizations = newAuthorizations.get(identifier);

                // ensure the entry exists
                if (!resourceAuthorizations.containsKey(identity)) {
                    resourceAuthorizations.put(identity, EnumSet.noneOf(RequestAction.class));
                }

                final Set<RequestAction> authorizedActions = resourceAuthorizations.get(identity);
                final String authorizationCode = authorization.getAction();

                // updated the actions for this identity
                if (authorizationCode.contains(READ_CODE)) {
                    authorizedActions.add(RequestAction.READ);
                }
                if (authorizationCode.contains(WRITE_CODE)) {
                    authorizedActions.add(RequestAction.WRITE);
                }
            }
        }

        // set the new authorizations
        authorizations.set(newAuthorizations);

        // if we've copied a the authorizations file to a restore directory synchronize it
        if (restoreAuthorizationsFile != null) {
            FileUtils.copyFile(authorizationsFile, restoreAuthorizationsFile, false, false, logger);
        }

        logger.info(String.format("Authorizations file loaded at %s", new Date().toString()));
    }

    @AuthorizerContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public void preDestruction() {
        if (fileWatcherExecutorService != null) {
            fileWatcherExecutorService.shutdown();
        }
    }
}
