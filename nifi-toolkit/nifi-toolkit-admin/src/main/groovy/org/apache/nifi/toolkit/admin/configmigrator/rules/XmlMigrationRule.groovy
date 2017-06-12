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
package org.apache.nifi.toolkit.admin.configmigrator.rules


/**
 * XmlMigrationRule assists classes in migrating existing xml configurations to newer versions
 * by converting incoming content to Xml nodes which can then be navigated and edited as required.
 */

abstract class XmlMigrationRule extends GenericMigrationRule{

    @Override
    byte[] migrate(byte[] oldContent, byte[] upgradeContent) {
        def oldBais = new ByteArrayInputStream(oldContent)
        def newBais = new ByteArrayInputStream(upgradeContent)
        migrateXml(new XmlParser().parse(oldBais), new XmlParser().parse(newBais))
    }

    abstract byte[] migrateXml(Node oldXmlContent, Node newXmlContent)

    protected byte[] convertToByteArray(Node xml){
        def writer = new StringWriter()
        def nodePrinter = new XmlNodePrinter(new PrintWriter(writer))
        nodePrinter.setPreserveWhitespace(true)
        nodePrinter.print(xml)
        return writer.toString().bytes
    }


}
