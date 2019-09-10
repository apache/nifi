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
package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.persistence.TemplateSerializer;
import org.apache.nifi.toolkit.cli.api.WritableResult;
import org.apache.nifi.web.api.dto.TemplateDTO;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class TemplateResult implements WritableResult<TemplateDTO> {

    private final TemplateDTO templateDTO;

    private final String exportFileName;

    public TemplateResult(final TemplateDTO templateDTO, final String exportFileName) {
        this.templateDTO = templateDTO;
        this.exportFileName = exportFileName;
        Validate.notNull(this.templateDTO);
    }

    @Override
    public TemplateDTO getResult() {
        return templateDTO;
    }

    @Override
    public void write(final PrintStream output) throws IOException {
        final byte[] serializedTemplate = TemplateSerializer.serialize(templateDTO);
        if (exportFileName != null) {
            try (final OutputStream resultOut = new FileOutputStream(exportFileName)) {
                resultOut.write(serializedTemplate);
            }
        } else {
            output.write(serializedTemplate);
        }
    }

}
