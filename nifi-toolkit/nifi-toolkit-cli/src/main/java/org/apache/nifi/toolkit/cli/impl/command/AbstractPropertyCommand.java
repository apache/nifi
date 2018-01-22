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
package org.apache.nifi.toolkit.cli.impl.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Session;
import org.apache.nifi.toolkit.cli.impl.session.SessionVariables;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base class for commands that support loading properties from the session or an argument.
 */
public abstract class AbstractPropertyCommand extends AbstractCommand {

    public AbstractPropertyCommand(String name) {
        super(name);
    }

    @Override
    public void execute(final CommandLine commandLine) throws CommandException {
        try {
            final Properties properties = new Properties();

            // start by loading the properties file if it was specified
            if (commandLine.hasOption(CommandOption.PROPERTIES.getLongName())) {
                final String propertiesFile = commandLine.getOptionValue(CommandOption.PROPERTIES.getLongName());
                if (!StringUtils.isBlank(propertiesFile)) {
                    try (final InputStream in = new FileInputStream(propertiesFile)) {
                        properties.load(in);
                    }
                }
            } else {
                // no properties file was specified so see if there is anything in the session
                final SessionVariables sessionVariable = getPropertiesSessionVariable();
                if (sessionVariable != null) {
                    final Session session = getContext().getSession();
                    final String sessionPropsFiles = session.get(sessionVariable.getVariableName());
                    if (!StringUtils.isBlank(sessionPropsFiles)) {
                        try (final InputStream in = new FileInputStream(sessionPropsFiles)) {
                            properties.load(in);
                        }
                    }
                }
            }

            // add in anything specified on command line, and override anything that was already there
            for (final Option option : commandLine.getOptions()) {
                final String optValue = option.getValue() == null ? "" : option.getValue();
                properties.setProperty(option.getLongOpt(), optValue);
            }

            // delegate to sub-classes
            doExecute(properties);

        } catch (CommandException ce) {
            throw ce;
        } catch (Exception e) {
            throw new CommandException("Error executing command '" + getName() + "' : " + e.getMessage(), e);
        }
    }

    /**
     * @return the SessionVariables that specifies the properties file for this command, or null if not supported
     */
    protected abstract SessionVariables getPropertiesSessionVariable();

    /**
     * Sub-classes implement specific command logic.
     *
     * @param properties the properties which represent the arguments
     * @throws CommandException if an error occurrs
     */
    protected abstract void doExecute(final Properties properties) throws CommandException;

}
