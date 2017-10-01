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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;

import static org.apache.nifi.processors.adls.ADLSConstants.REL_FAILURE;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_SUCCESS;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"azure", "hadoop", "ADLS", "get", "ingest", "fetch", "source", "ingress", "restricted"})
@CapabilityDescription("Retrieves a file from ADLS. The content of the incoming FlowFile is replaced by the content of the file in ADLS. "
        + "The file in ADLS is left intact without any changes being made to it.")
@SeeAlso({PutADLSFile.class, ListADLSFile.class})
@Restricted("Provides operator the ability to retrieve any file that NiFi has access to in ADLS or the local filesystem.")
public class FetchADLSFile extends ADLSAbstractProcessor {

    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("adls-fetch-filename")
            .displayName("ADLS filename")
            .description("The name of the ADLS file to retrieve, including the path")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("${path}/${filename}")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .build();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        super.descriptors.add(FILENAME);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        ADLStoreClient adlsClient = getAdlStoreClient(context);

        StopWatch stopWatch = new StopWatch();
        final String filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        ADLFileInputStream readStream = null;
        try {
            stopWatch.start();
            readStream = adlsClient.getReadStream(filename);
            flowFile = session.importFrom(readStream, flowFile);

        } catch (IOException e) {
            getLogger().error("Unable to retrieve the file from ADLS", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } finally {
            if(readStream != null)
                try {
                    readStream.close();
                } catch (IOException e) {
                    //ignore
                }
        }
        stopWatch.stop();
        getLogger().debug("Successfully fetched the file in {} at {} rate",
                new Object[]{
                        stopWatch.calculateDataRate(flowFile.getSize()),
                        stopWatch.getDuration()});
        session.transfer(flowFile, REL_SUCCESS);
    }
}
