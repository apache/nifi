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
package org.apache.nifi.toolkit.cli.impl.command.nifi.params;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ParamContextClient;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.util.JacksonUtils;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterEntity;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class ExportParamContext extends AbstractNiFiCommand<ExportParamContext.ExportedParamContextResult> {

    public ExportParamContext() {
        super("export-param-context", ExportedParamContextResult.class);
    }

    @Override
    public String getDescription() {
        return "Exports a given parameter context to a json representation, with the option of writing to a file. ";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PARAM_CONTEXT_ID.createOption());
        addOption(CommandOption.OUTPUT_FILE.createOption());
    }

    @Override
    public ExportedParamContextResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {
        final String paramContextId = getRequiredArg(properties, CommandOption.PARAM_CONTEXT_ID);
        final String outputFilename = getArg(properties, CommandOption.OUTPUT_FILE);

        // retrieve the context by id
        final ParamContextClient paramContextClient = client.getParamContextClient();
        final ParameterContextEntity parameterContextEntity = paramContextClient.getParamContext(paramContextId);

        // clear out values that don't make sense for importing to next environment
        final ParameterContextDTO parameterContext = parameterContextEntity.getComponent();
        parameterContext.setId(null);
        parameterContext.setBoundProcessGroups(null);

        for (final ParameterEntity parameterEntity : parameterContext.getParameters()) {
            final ParameterDTO parameterDTO = parameterEntity.getParameter();
            parameterDTO.setReferencingComponents(null);
            if (parameterDTO.getSensitive()) {
                parameterDTO.setValue(null);
            }
            parameterEntity.setCanWrite(null);
        }

        // sort the entities so that each export is in consistent order
        final Comparator<ParameterEntity> entityComparator = (p1, p2) ->{
            final String p1Name = p1.getParameter().getName();
            final String p2Name = p2.getParameter().getName();
            return p1Name.compareTo(p2Name);
        };

        final Set<ParameterEntity> sortedEntities = new TreeSet<>(entityComparator);
        sortedEntities.addAll(parameterContext.getParameters());
        parameterContext.setParameters(sortedEntities);

        return new ExportedParamContextResult(parameterContext, outputFilename);
    }

    /**
     * Result for writing the exported param context.
     */
    public static class ExportedParamContextResult implements WritableResult<ParameterContextDTO> {

        private final ParameterContextDTO parameterContext;
        private final String outputFilename;

        public ExportedParamContextResult(final ParameterContextDTO parameterContext, final String outputFilename) {
            this.parameterContext = parameterContext;
            this.outputFilename = outputFilename;
            Validate.notNull(this.parameterContext);
        }

        @Override
        public void write(final PrintStream output) throws IOException {
            if (outputFilename != null) {
                try (final OutputStream resultOut = new FileOutputStream(outputFilename)) {
                    JacksonUtils.write(parameterContext, resultOut);
                }
            } else {
                JacksonUtils.write(parameterContext, output);
            }
        }

        @Override
        public ParameterContextDTO getResult() {
            return parameterContext;
        }
    }

}
