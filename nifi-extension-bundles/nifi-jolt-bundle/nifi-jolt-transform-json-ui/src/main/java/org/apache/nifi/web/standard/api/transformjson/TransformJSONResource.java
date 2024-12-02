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

package org.apache.nifi.web.standard.api.transformjson;

import com.bazaarvoice.jolt.JoltTransform;
import com.bazaarvoice.jolt.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.StandardEvaluationContext;
import org.apache.nifi.jolt.util.TransformFactory;
import org.apache.nifi.jolt.util.TransformUtils;
import org.apache.nifi.web.standard.api.AbstractStandardResource;
import org.apache.nifi.web.standard.api.transformjson.dto.JoltSpecificationDTO;
import org.apache.nifi.web.standard.api.transformjson.dto.ValidationDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Path("/standard/transformjson")
public class TransformJSONResource extends AbstractStandardResource {

    private static final Logger logger = LoggerFactory.getLogger(TransformJSONResource.class);
    private static final String DEFAULT_CHARSET = "UTF-8";

    private static final String CUSTOM_TRANSFORM_NAME = "jolt-transform-custom";

    protected Object getSpecificationJsonObject(JoltSpecificationDTO specificationDTO, boolean evaluateAttributes) {
        if (!StringUtils.isEmpty(specificationDTO.getSpecification())) {
            final String specification;

            if (evaluateAttributes) {
                PreparedQuery preparedQuery = Query.prepare(specificationDTO.getSpecification());
                Map<String, String> attributes = specificationDTO.getExpressionLanguageAttributes() == null ? Collections.unmodifiableMap(new HashMap<>())
                        : specificationDTO.getExpressionLanguageAttributes();
                specification = preparedQuery.evaluateExpressions(new StandardEvaluationContext(attributes), null);
            } else {
                specification = specificationDTO.getSpecification().replaceAll("\\$\\{", "\\\\\\\\\\$\\{");
            }
            return JsonUtils.jsonToObject(specification, DEFAULT_CHARSET);

        } else {
            return null;
        }
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/validate")
    public Response validateSpec(JoltSpecificationDTO specificationDTO) {
        ValidationDTO validation;

        try {
            getTransformation(specificationDTO, false);
            validation = new ValidationDTO(true, null);
        } catch (final Exception e) {
            logger.warn("Jolt Transform Validation Failed", e);

            final String message;
            if (CUSTOM_TRANSFORM_NAME.equals(specificationDTO.getTransform())) {
                message = "Custom Transform not supported for advanced validation";
            } else {
                message = "Validation Failed: Please review the Jolt Specification formatting";
            }

            validation = new ValidationDTO(false, message);
        }

        return Response.ok(validation).build();
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/execute")
    public Response executeSpec(JoltSpecificationDTO specificationDTO) {
        try {
            JoltTransform transform = getTransformation(specificationDTO, true);
            Object inputJson = JsonUtils.jsonToObject(specificationDTO.getInput());
            return Response.ok(JsonUtils.toJsonString(TransformUtils.transform(transform, inputJson))).build();
        } catch (final Exception e) {
            logger.warn("Jolt Transform Execute Failed", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    private JoltTransform getTransformation(JoltSpecificationDTO specificationDTO, boolean evaluateAttributes) throws Exception {
        final String transformName = specificationDTO.getTransform();

        if (CUSTOM_TRANSFORM_NAME.equals(transformName)) {
            throw new IllegalArgumentException("Custom Transform Classes not supported for dynamic evaluation");
        }

        Object specJson = getSpecificationJsonObject(specificationDTO, evaluateAttributes);
        return TransformFactory.getTransform(getClass().getClassLoader(), specificationDTO.getTransform(), specJson);
    }
}
