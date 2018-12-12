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
package org.apache.nifi.toolkit.cli.impl.client.nifi.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.client.nifi.TemplatesClient;
import org.apache.nifi.web.api.dto.TemplateDTO;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.client.WebTarget;

/**
 * Jersey implementation of TemplatesClient.
 */
public class JerseyTemplatesClient extends AbstractJerseyClient implements TemplatesClient {

    private final WebTarget templatesTarget;

    public JerseyTemplatesClient(final WebTarget baseTarget) {
        this(baseTarget, Collections.emptyMap());
    }

    public JerseyTemplatesClient(final WebTarget baseTarget, final Map<String, String> headers) {
        super(headers);
        this.templatesTarget = baseTarget.path("/templates");
    }

    @Override
    public TemplateDTO getTemplate(final String templateId) throws NiFiClientException, IOException {
        if (StringUtils.isBlank(templateId)) {
            throw new IllegalArgumentException("Template id cannot be null");
        }

        return executeAction("Error retrieving template", () -> {
            final WebTarget target = templatesTarget
                    .path("{id}/download")
                    .resolveTemplate("id", templateId);
            return getRequestBuilder(target).get(TemplateDTO.class);
        });
    }

}
