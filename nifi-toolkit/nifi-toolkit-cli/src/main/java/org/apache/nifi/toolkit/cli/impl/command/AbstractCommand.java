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

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.commons.cli.help.TextHelpAppendable;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.Result;
import org.apache.nifi.toolkit.cli.api.ResultType;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/**
 * Base class for all commands.
 */
public abstract class AbstractCommand<R extends Result> implements Command<R> {

    private final String name;
    private final Class<R> resultClass;
    private final Options options;

    private Context context;
    private PrintStream output;

    public AbstractCommand(final String name, final Class<R> resultClass) {
        this.name = Objects.requireNonNull(name);
        this.resultClass = Objects.requireNonNull(resultClass);
        this.options = createBaseOptions();
    }

    protected Options createBaseOptions() {
        final Options options = new Options();

        options.addOption(CommandOption.URL.createOption());
        options.addOption(CommandOption.PROPERTIES.createOption());

        options.addOption(CommandOption.CONNECTION_TIMEOUT.createOption());
        options.addOption(CommandOption.READ_TIMEOUT.createOption());

        options.addOption(CommandOption.KEYSTORE.createOption());
        options.addOption(CommandOption.KEYSTORE_TYPE.createOption());
        options.addOption(CommandOption.KEYSTORE_PASSWORD.createOption());
        options.addOption(CommandOption.KEY_PASSWORD.createOption());

        options.addOption(CommandOption.TRUSTSTORE.createOption());
        options.addOption(CommandOption.TRUSTSTORE_TYPE.createOption());
        options.addOption(CommandOption.TRUSTSTORE_PASSWORD.createOption());

        options.addOption(CommandOption.PROXIED_ENTITY.createOption());

        options.addOption(CommandOption.BASIC_AUTH_USER.createOption());
        options.addOption(CommandOption.BASIC_AUTH_PASSWORD.createOption());

        options.addOption(CommandOption.BEARER_TOKEN.createOption());

        options.addOption(CommandOption.OUTPUT_TYPE.createOption());
        options.addOption(CommandOption.VERBOSE.createOption());
        options.addOption(CommandOption.HELP.createOption());

        return options;
    }

    @Override
    public final void initialize(final Context context) {
        this.context = Objects.requireNonNull(context);
        this.output = Objects.requireNonNull(context.getOutput());
        this.doInitialize(context);
    }

    protected void doInitialize(final Context context) {
        // subclasses can override to do additional things like add options
    }

    protected void addOption(final Option option) {
        this.options.addOption(option);
    }

    protected Context getContext() {
        return this.context;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final Class<R> getResultImplType() {
        return resultClass;
    }

    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public void printUsage(String errorMessage) {
        output.println();

        if (errorMessage != null) {
            output.println("ERROR: " + errorMessage);
            output.println();
        }

        try {
            final PrintWriter printWriter = new PrintWriter(output);
            final TextHelpAppendable appendable = new TextHelpAppendable(printWriter);
            appendable.setMaxWidth(80);

            appendable.appendParagraph(getDescription());

            if (isReferencable()) {
                appendable.appendParagraph("PRODUCES BACK-REFERENCES");
            }

            HelpFormatter.builder()
                    .setHelpAppendable(appendable)
                    .setShowSince(false)
                    .get()
                    .printHelp(getName(), null, getOptions(), null, false);

            printWriter.println();
            printWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to print command usage for " + getName(), e);
        }
    }


    protected void print(final String val) {
        output.print(val);
    }

    protected void println(final String val) {
        output.println(val);
    }

    protected void println() {
        output.println();
    }

    protected ResultType getResultType(final Properties properties) {
        final ResultType resultType;
        if (properties.containsKey(CommandOption.OUTPUT_TYPE.getLongName())) {
            final String outputTypeValue = properties.getProperty(CommandOption.OUTPUT_TYPE.getLongName());
            resultType = ResultType.valueOf(outputTypeValue.toUpperCase().trim());
        } else {
            resultType = ResultType.SIMPLE;
        }
        return resultType;
    }

    protected String getArg(final Properties properties, final CommandOption option) {
        return properties.getProperty(option.getLongName());
    }

    protected String getRequiredArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            throw new MissingOptionException("Missing required option --" + option.getLongName());
        }
        return argValue;
    }

    protected Integer getIntArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            return null;
        }

        try {
            return Integer.valueOf(argValue);
        } catch (Exception e) {
            throw new MissingOptionException("Version must be numeric: " + argValue);
        }
    }

    protected Integer getRequiredIntArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            throw new MissingOptionException("Missing required option --" + option.getLongName());
        }

        try {
            return Integer.valueOf(argValue);
        } catch (Exception e) {
            throw new MissingOptionException("Version must be numeric: " + argValue);
        }
    }

    protected boolean hasArg(final Properties properties, final CommandOption option) {
        return properties.containsKey(option.getLongName());
    }

    protected boolean isVerbose(final Properties properties) {
        return properties.containsKey(CommandOption.VERBOSE.getLongName());
    }

    protected boolean isInteractive() {
        return getContext().isInteractive();
    }

    protected void printlnIfInteractive(final String val) {
        if (isInteractive()) {
            println(val);
        }
    }

    protected String getInputSourceContent(String inputFile) throws IOException {
        String contents;
        try {
            // try a public resource URL
            contents = IOUtils.toString(URI.create(inputFile).toURL(), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException | MalformedURLException e) {
            // assume a local file then
            URI uri = Paths.get(inputFile).toAbsolutePath().toUri();
            contents = IOUtils.toString(uri, StandardCharsets.UTF_8);
        }
        return contents;
    }

}
