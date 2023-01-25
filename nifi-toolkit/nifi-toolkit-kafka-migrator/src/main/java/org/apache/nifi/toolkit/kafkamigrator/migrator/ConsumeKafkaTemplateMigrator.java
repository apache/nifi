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
package org.apache.nifi.toolkit.kafkamigrator.migrator;

import org.apache.nifi.toolkit.kafkamigrator.MigratorConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

public class ConsumeKafkaTemplateMigrator extends AbstractKafkaMigrator {

    public ConsumeKafkaTemplateMigrator(final MigratorConfiguration configuration) {
        super(configuration);
    }

    @Override
    public void configureProperties(final Node node) throws XPathExpressionException {
        final Element propertyElement = (Element) XPATH.evaluate("config/properties", node, XPathConstants.NODE);
        super.configureProperties(propertyElement);
    }

    @Override
    public void configureComponentSpecificSteps(final Node node) throws XPathExpressionException {
        final Element propertyElement = (Element) XPATH.evaluate("config/properties", node, XPathConstants.NODE);
        super.configureComponentSpecificSteps(propertyElement);
    }

    @Override
    public void migrate(final Element className, final Node processor) throws XPathExpressionException {
        configureProperties(processor);
        configureComponentSpecificSteps(processor);
        configureDescriptors(processor);
        replaceClassName(className);
        replaceArtifact(processor);
    }
}
