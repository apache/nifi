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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Command;
import org.apache.nifi.toolkit.cli.api.Context;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Properties;

/**
 * Base class for all commands.
 */
public abstract class AbstractCommand implements Command {

    protected static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        MAPPER.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        MAPPER.setAnnotationIntrospector(new JaxbAnnotationIntrospector(MAPPER.getTypeFactory()));
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    protected static final ObjectWriter OBJECT_WRITER = MAPPER.writerWithDefaultPrettyPrinter();

    private final String name;
    private final Options options;

    private Context context;
    private PrintStream output;

    public AbstractCommand(final String name) {
        this.name = name;
        Validate.notNull(this.name);

        this.options = new Options();

        this.options.addOption(CommandOption.URL.createOption());
        this.options.addOption(CommandOption.PROPERTIES.createOption());

        this.options.addOption(CommandOption.KEYSTORE.createOption());
        this.options.addOption(CommandOption.KEYSTORE_TYPE.createOption());
        this.options.addOption(CommandOption.KEYSTORE_PASSWORD.createOption());
        this.options.addOption(CommandOption.KEY_PASSWORD.createOption());

        this.options.addOption(CommandOption.TRUSTSTORE.createOption());
        this.options.addOption(CommandOption.TRUSTSTORE_TYPE.createOption());
        this.options.addOption(CommandOption.TRUSTSTORE_PASSWORD.createOption());

        this.options.addOption(CommandOption.PROXIED_ENTITY.createOption());

        this.options.addOption(CommandOption.VERBOSE.createOption());
        this.options.addOption(CommandOption.HELP.createOption());
    }

    @Override
    public final void initialize(final Context context) {
        Validate.notNull(context);
        Validate.notNull(context.getOutput());
        this.context = context;
        this.output = context.getOutput();
        this.doInitialize(context);
    }

    protected void doInitialize(final Context context) {
        // sub-classes can override to do additional things like add options
    }

    protected void addOption(final Option option) {
        this.options.addOption(option);
    }

    protected Context getContext() {
        return this.context;
    }

    @Override
    public String getName() {
        return name;
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

        final PrintWriter printWriter = new PrintWriter(output);

        final HelpFormatter hf = new HelpFormatter();
        hf.setWidth(160);
        hf.printHelp(printWriter, hf.getWidth(), getName(), null, getOptions(),
                hf.getLeftPadding(), hf.getDescPadding(), null, false);

        printWriter.println();
        printWriter.flush();
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

    protected void writeResult(final Properties properties, final Object result) throws IOException {
        if (properties.containsKey(CommandOption.OUTPUT_FILE.getLongName())) {
            final String outputFile = properties.getProperty(CommandOption.OUTPUT_FILE.getLongName());
            try (final OutputStream resultOut = new FileOutputStream(outputFile)) {
                OBJECT_WRITER.writeValue(resultOut, result);
            }
        } else {
            OBJECT_WRITER.writeValue(new OutputStream() {
                @Override
                public void write(byte[] b) throws IOException {
                    output.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    output.write(b, off, len);
                }

                @Override
                public void write(int b) throws IOException {
                    output.write(b);
                }

                @Override
                public void close() throws IOException {
                    // DON'T close the output stream here
                    output.flush();
                }
            }, result);
        }

    }

    protected String getArg(final Properties properties, final CommandOption option) {
        return properties.getProperty(option.getLongName());
    }

    protected String getRequiredArg(final Properties properties, final CommandOption option) throws MissingOptionException {
        final String argValue = properties.getProperty(option.getLongName());
        if (StringUtils.isBlank(argValue)) {
            throw new MissingOptionException("Missing required option '" + option.getLongName() + "'");
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
            throw new MissingOptionException("Missing required option '" + option.getLongName() + "'");
        }

        try {
            return Integer.valueOf(argValue);
        } catch (Exception e) {
            throw new MissingOptionException("Version must be numeric: " + argValue);
        }
    }

}
