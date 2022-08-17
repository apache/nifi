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
package org.apache.nifi.web.server.util;

import org.eclipse.jetty.util.Scanner;
import org.eclipse.jetty.util.annotation.ManagedAttribute;
import org.eclipse.jetty.util.annotation.ManagedOperation;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * <p>The {@link TrustStoreScanner} is used to monitor the TrustStore file used by the {@link SslContextFactory}.
 * It will reload the {@link SslContextFactory} if it detects that the TrustStore file has been modified.</p>
 * <p>
 * Though it would have been more ideal to simply extend KeyStoreScanner and override the keystore resource
 * with the truststore resource, KeyStoreScanner's constructor was written in a way that doesn't make this possible.
 */
public class TrustStoreScanner extends ContainerLifeCycle implements Scanner.DiscreteListener {
    private static final Logger LOG = Log.getLogger(TrustStoreScanner.class);

    private final SslContextFactory sslContextFactory;
    private final File truststoreFile;
    private final Scanner _scanner;

    public TrustStoreScanner(SslContextFactory sslContextFactory) {
        this.sslContextFactory = sslContextFactory;
        try {
            Resource truststoreResource = sslContextFactory.getTrustStoreResource();
            File monitoredFile = truststoreResource.getFile();
            if (monitoredFile == null || !monitoredFile.exists()) {
                throw new IllegalArgumentException("truststore file does not exist");
            }
            if (monitoredFile.isDirectory()) {
                throw new IllegalArgumentException("expected truststore file not directory");
            }

            if (truststoreResource.getAlias() != null) {
                // this resource has an alias, use the alias, as that's what's returned in the Scanner
                monitoredFile = new File(truststoreResource.getAlias());
            }

            truststoreFile = monitoredFile;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Monitored Truststore File: {}", monitoredFile);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("could not obtain truststore file", e);
        }

        File parentFile = truststoreFile.getParentFile();
        if (!parentFile.exists() || !parentFile.isDirectory()) {
            throw new IllegalArgumentException("error obtaining truststore dir");
        }

        _scanner = new Scanner();
        _scanner.setScanDirs(Collections.singletonList(parentFile));
        _scanner.setScanInterval(1);
        _scanner.setReportDirs(false);
        _scanner.setReportExistingFilesOnStartup(false);
        _scanner.setScanDepth(1);
        _scanner.addListener(this);
        addBean(_scanner);
    }

    @Override
    public void fileAdded(String filename) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("added {}", filename);
        }

        if (truststoreFile.toPath().toString().equals(filename)) {
            reload();
        }
    }

    @Override
    public void fileChanged(String filename) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("changed {}", filename);
        }

        if (truststoreFile.toPath().toString().equals(filename)) {
            reload();
        }
    }

    @Override
    public void fileRemoved(String filename) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("removed {}", filename);
        }

        if (truststoreFile.toPath().toString().equals(filename)) {
            reload();
        }
    }

    @ManagedOperation(value = "Scan for changes in the SSL Truststore", impact = "ACTION")
    public void scan() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("scanning");
        }

        _scanner.scan();
        _scanner.scan();
    }

    @ManagedOperation(value = "Reload the SSL Truststore", impact = "ACTION")
    public void reload() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("reloading truststore file {}", truststoreFile);
        }

        try {
            sslContextFactory.reload(scf -> {
            });
        } catch (Throwable t) {
            LOG.warn("Truststore Reload Failed", t);
        }
    }

    @ManagedAttribute("scanning interval to detect changes which need reloaded")
    public int getScanInterval() {
        return _scanner.getScanInterval();
    }

    public void setScanInterval(int scanInterval) {
        _scanner.setScanInterval(scanInterval);
    }
}