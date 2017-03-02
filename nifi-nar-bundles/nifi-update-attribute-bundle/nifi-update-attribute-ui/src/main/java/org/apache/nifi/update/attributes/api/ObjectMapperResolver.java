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
package org.apache.nifi.update.attributes.api;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;

/**
 *
 */
@Provider
public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    public ObjectMapperResolver() throws Exception {
        mapper = new ObjectMapper();

        final AnnotationIntrospector jaxbIntrospector = new JaxbAnnotationIntrospector();
        final SerializationConfig serializationConfig = mapper.getSerializationConfig();
        final DeserializationConfig deserializationConfig = mapper.getDeserializationConfig();

        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        mapper.setDeserializationConfig(deserializationConfig.withAnnotationIntrospector(jaxbIntrospector));
    }

    @Override
    public ObjectMapper getContext(Class<?> objectType) {
        return mapper;
    }
}
