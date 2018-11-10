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



package org.apache.nifi.testharness.util;

import org.apache.nifi.testharness.api.FlowFileEditorCallback;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileInputStream;

public final class XmlUtils {

    public static void editXml(File inputFile, File outputFile, FlowFileEditorCallback editCallback) {

      try {
          Document document = getFileAsDocument(inputFile);

          document = editCallback.edit(document);

          // save the result
          TransformerFactory transformerFactory = TransformerFactory.newInstance();
          Transformer transformer = transformerFactory.newTransformer();
          transformer.transform(new DOMSource(document), new StreamResult(outputFile));

      } catch (Exception e) {
          throw new RuntimeException("Failed to change XML document: " + e.getMessage(), e);
      }
  }

  public static Document getFileAsDocument(File xmlFile) {
      try(FileInputStream inputStream = new FileInputStream(xmlFile)) {

          DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

          return documentBuilder.parse(new InputSource(inputStream));

      } catch (Exception e) {
          throw new RuntimeException("Failed to parse XML file: " + xmlFile, e);
      }
  }

}