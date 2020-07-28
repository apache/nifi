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
package org.apache.nifi.toolkit.flowanalyzer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FlowAnalyzerDriver {
    final static String CONST_BYTES_GB_CONV = "1000000000";
    final static String CONST_BYTES_MB_CONV = "1000000";
    final static String CONST_BYTES_KB_CONV = "1000";
    final static int DIVIDE_SCALE           = 9;
    final static String CONST_XMLNODE_CONNECTION = "connection";

    private static void printUsage() {
        System.out.println("This application seeks to produce a report to analyze the flow.xml.gz file");
        System.out.println(
                "Currently the reports supported by this application are Total Storage for all queues " +
                "backpressure, average storage of all queues backpressure, and min and max of all queues " +
                 "backpressure over the entire flow.");
        System.out.println("\n\n\n");
        System.out.println("Usage: flow-analyzer.sh <path to flow.xml.gz>");
    }

    public static void main(String[] args) throws Exception {
        BigDecimal totalDataSize = new BigDecimal("0.0");
        BigDecimal max = new BigDecimal("0.0");
        BigDecimal min = new BigDecimal("0.0");
        BigDecimal avg = new BigDecimal("0.0");
        long maxQueueSize       = 0L;
        long minQueueSize       = 0L;
        long totalQueueSize     = 0L;

        int numberOfConnections = 0;

        if (helpRequested(args)) {
            printUsage();
            return;
        }

        String input = args[0];
        if (!input.contains("xml.gz"))
            input = input + "/flow.xml.gz";

        InputStream fileStream = new FileInputStream(input);
        InputStream gzipStream = new GZIPInputStream(fileStream);

        System.out.println("Using flow=" + input);

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder;
        try {
            documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(gzipStream);
            NodeList connectionNode = document.getElementsByTagName(CONST_XMLNODE_CONNECTION);

            for (int x = 0; x < connectionNode.getLength(); x++) {
                Node nNode = connectionNode.item(x);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element maxWorkQueueSize = (Element) nNode;
                    String maxDataSize = maxWorkQueueSize.getElementsByTagName("maxWorkQueueDataSize").item(0)
                            .getTextContent();
                    BigDecimal byteValue = (convertSizeToByteValue(maxDataSize)) != null
                            ? convertSizeToByteValue(maxDataSize) : new BigDecimal("0.0");
                    numberOfConnections++;
                    avg = avg.add(byteValue);
                    String dataQueueSize = maxWorkQueueSize.getElementsByTagName("maxWorkQueueSize").item(0)
                            .getTextContent();
                    Long dataQueueSizeL  = new Long(dataQueueSize);
                    totalQueueSize = dataQueueSizeL + totalQueueSize;
                    if(dataQueueSizeL > maxQueueSize)
                        maxQueueSize = dataQueueSizeL;
                    if(dataQueueSizeL < minQueueSize || minQueueSize == 0)
                        minQueueSize = dataQueueSizeL;
                    if (max.compareTo(byteValue) < 0)
                        max = byteValue;

                    if (byteValue.compareTo(min) < 0 || min.compareTo(new BigDecimal("0.0")) == 0)
                        min = byteValue;

                    totalDataSize = totalDataSize.add(byteValue);
                }

            }

            System.out.println("Total Bytes Utilized by System=" + convertBytesToGB(totalDataSize).toPlainString()
                    + " GB\nMax Back Pressure Size=" + convertBytesToGB(max).toPlainString()
                    + " GB\nMin Back Pressure Size=" + convertBytesToGB(min).toPlainString()
                    + " GB\nAverage Back Pressure Size="
                    + convertBytesToGB(avg.divide(new BigDecimal(numberOfConnections), DIVIDE_SCALE, RoundingMode.HALF_UP)) + " GB");
            System.out.println("Max Flowfile Queue Size=" + maxQueueSize + "\nMin Flowfile Queue Size=" + minQueueSize
                    + "\nAvg Flowfile Queue Size=" + new BigDecimal(totalQueueSize).divide(new BigDecimal(numberOfConnections), DIVIDE_SCALE, RoundingMode.HALF_UP));
            gzipStream.close();
            fileStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Exception occurred while attempting to parse flow.xml.gz.  Cause: " + e.getCause());
        }

    }

    private static boolean helpRequested(String[] args) {
        return args.length == 0 || (args.length > 0 && (args[0].equalsIgnoreCase("-h") || args[0].equalsIgnoreCase("--help")));
    }

    /**
     *
     * @param value to convert to bytes
     * @return BigDecimal Byte size
     */
    public static BigDecimal convertSizeToByteValue(String value) {
        BigDecimal size = null;

        if (value.contains("GB")) {
            String numericValue = value.substring(0, value.indexOf("G") - 1);
            size = new BigDecimal(numericValue).multiply(new BigDecimal(CONST_BYTES_GB_CONV));
        }

        if (value.contains("MB")) {
            String numericValue = value.substring(0, value.indexOf("M") - 1);
            size = new BigDecimal(numericValue).multiply(new BigDecimal(CONST_BYTES_MB_CONV));
        }

        if (value.contains("KB")) {
            String numericValue = value.substring(0, value.indexOf("K") - 1);
            size = new BigDecimal(numericValue).multiply(new BigDecimal(CONST_BYTES_KB_CONV));
        }

        return size;
    }

    /**
     * @param bytes to convert to GB
     * @return BigDecimal bytes to GB
     */
    public static BigDecimal convertBytesToGB(BigDecimal bytes) {
        return bytes.divide(new BigDecimal(CONST_BYTES_GB_CONV), DIVIDE_SCALE, RoundingMode.HALF_UP).stripTrailingZeros();
    }

}
