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
package org.apache.nifi.toolkit.cli.impl.session;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.api.SessionException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Set;

public class PersistentSession implements Session {

    private final File persistenceFile;

    private final Session wrappedSession;

    public PersistentSession(final File persistenceFile, final Session wrappedSession) {
        this.persistenceFile = persistenceFile;
        this.wrappedSession = wrappedSession;
        Validate.notNull(persistenceFile);
        Validate.notNull(wrappedSession);
    }

    @Override
    public String getNiFiClientID() {
        return wrappedSession.getNiFiClientID();
    }

    @Override
    public synchronized void set(final String variable, final String value) throws SessionException {
        wrappedSession.set(variable, value);
        saveSession();
    }

    @Override
    public synchronized String get(final String variable) throws SessionException {
        return wrappedSession.get(variable);
    }

    @Override
    public synchronized void remove(final String variable) throws SessionException {
        wrappedSession.remove(variable);
        saveSession();
    }

    @Override
    public synchronized void clear() throws SessionException {
        wrappedSession.clear();
        saveSession();
    }

    @Override
    public synchronized Set<String> keys() throws SessionException {
        return wrappedSession.keys();
    }

    @Override
    public synchronized void printVariables(final PrintStream output) throws SessionException {
        wrappedSession.printVariables(output);
    }

    private void saveSession() throws SessionException {
        try (final OutputStream out = new FileOutputStream(persistenceFile)) {
            final Properties properties = new Properties();
            for (String variable : wrappedSession.keys()) {
                String value = wrappedSession.get(variable);
                properties.setProperty(variable, value);
            }
            properties.store(out, null);
            out.flush();
        } catch (Exception e) {
            throw new SessionException("Error saving session: " + e.getMessage(), e);
        }
    }

    public synchronized void loadSession() throws SessionException {
        wrappedSession.clear();
        try (final InputStream in = new FileInputStream(persistenceFile)) {
            final Properties properties = new Properties();
            properties.load(in);

            for (final String propName : properties.stringPropertyNames()) {
                final String propValue = properties.getProperty(propName);
                wrappedSession.set(propName, propValue);
            }
        } catch (Exception e) {
            throw new SessionException("Error loading session: " + e.getMessage(), e);
        }
    }

}
