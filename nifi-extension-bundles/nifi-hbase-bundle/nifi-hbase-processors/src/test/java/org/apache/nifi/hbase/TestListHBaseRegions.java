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
package org.apache.nifi.hbase;

import org.apache.nifi.hbase.scan.HBaseRegion;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class TestListHBaseRegions {
    private static final String TABLE_NAME = "nifi";
    private static final String HBASE_CLIENT_SERVICE_NAME = "hBaseClientService";

    private TestRunner runner;
    private ListHBaseRegions proc;

    private MockHBaseClientService hBaseClientService;

    @BeforeEach
    public void setup() throws InitializationException {
        proc = new ListHBaseRegions();
        runner = TestRunners.newTestRunner(proc);

        hBaseClientService = new MockHBaseClientService();
        runner.addControllerService(HBASE_CLIENT_SERVICE_NAME, hBaseClientService);
        runner.enableControllerService(hBaseClientService);

        runner.setProperty(ListHBaseRegions.TABLE_NAME, TABLE_NAME);
        runner.setProperty(ListHBaseRegions.HBASE_CLIENT_SERVICE, HBASE_CLIENT_SERVICE_NAME);
    }

    @Test
    public void testAllFlowFilesToSuccess() throws HBaseClientException {
        runner.setProperty(ListHBaseRegions.ROUTE_DEGENERATE_REGIONS, "false");
        runner.assertValid();

        final String startRowKey1 = "1";
        final String endRowKey1 = "5";
        final String regionName1 = "region-1";
        final long regionId1 = 1L;
        final boolean isDegenerate1 = false;
        final HBaseRegion hBaseRegion1 = new HBaseRegion(
                startRowKey1.getBytes(StandardCharsets.UTF_8),
                endRowKey1.getBytes(StandardCharsets.UTF_8),
                regionName1,
                regionId1,
                isDegenerate1
        );

        // this is a "degenerate" region where startRowKey > endRowKey
        final String startRowKey2 = "10";
        final String endRowKey2 = "6";
        final String regionName2 = "region-2";
        final long regionId2 = 2L;
        final boolean isDegenerate2 = true;
        final HBaseRegion hBaseRegion2 = new HBaseRegion(
                startRowKey2.getBytes(StandardCharsets.UTF_8),
                endRowKey2.getBytes(StandardCharsets.UTF_8),
                regionName2,
                regionId2,
                isDegenerate2
        );

        final List<HBaseRegion> regions = Arrays.asList(hBaseRegion1, hBaseRegion2);
        hBaseClientService.addHBaseRegions(regions);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(ListHBaseRegions.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHBaseRegions.REL_SUCCESS);

        assertEquals(String.valueOf(regionId1), flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));

        assertEquals(String.valueOf(regionId2), flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));
    }

    @Test
    public void testDegenerateRegionsToDegenerateRelationship() throws HBaseClientException {
        runner.setProperty(ListHBaseRegions.ROUTE_DEGENERATE_REGIONS, "true");
        runner.assertValid();

        final String startRowKey1 = "1";
        final String endRowKey1 = "5";
        final String regionName1 = "region-1";
        final long regionId1 = 1L;
        final boolean isDegenerate1 = false;
        final HBaseRegion hBaseRegion1 = new HBaseRegion(
                startRowKey1.getBytes(StandardCharsets.UTF_8),
                endRowKey1.getBytes(StandardCharsets.UTF_8),
                regionName1,
                regionId1,
                isDegenerate1
        );

        // this is a "degenerate" region where startRowKey > endRowKey
        final String startRowKey2 = "10";
        final String endRowKey2 = "6";
        final String regionName2 = "region-2";
        final long regionId2 = 2L;
        final boolean isDegenerate2 = true;
        final HBaseRegion hBaseRegion2 = new HBaseRegion(
                startRowKey2.getBytes(StandardCharsets.UTF_8),
                endRowKey2.getBytes(StandardCharsets.UTF_8),
                regionName2,
                regionId2,
                isDegenerate2
        );

        final List<HBaseRegion> regions = Arrays.asList(hBaseRegion1, hBaseRegion2);
        hBaseClientService.addHBaseRegions(regions);

        runner.run(1);
        runner.assertTransferCount(ListHBaseRegions.REL_SUCCESS, 1);
        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(ListHBaseRegions.REL_SUCCESS);

        assertEquals(String.valueOf(regionId1), successFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName1, successFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey1, successFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey1, successFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));

        runner.assertTransferCount(ListHBaseRegions.REL_DEGENERATE, 1);
        final List<MockFlowFile> degenerateFlowFiles = runner.getFlowFilesForRelationship(ListHBaseRegions.REL_DEGENERATE);

        assertEquals(String.valueOf(regionId2), degenerateFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName2, degenerateFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey2, degenerateFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey2, degenerateFlowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));
    }

    @Test
    public void testShouldNotRouteToDegenerateIfNoDegenerateRegions() throws HBaseClientException {
        runner.setProperty(ListHBaseRegions.ROUTE_DEGENERATE_REGIONS, "false");
        runner.assertValid();

        final String startRowKey1 = "1";
        final String endRowKey1 = "5";
        final String regionName1 = "region-1";
        final long regionId1 = 1L;
        final boolean isDegenerate1 = false;
        final HBaseRegion hBaseRegion1 = new HBaseRegion(
                startRowKey1.getBytes(StandardCharsets.UTF_8),
                endRowKey1.getBytes(StandardCharsets.UTF_8),
                regionName1,
                regionId1,
                isDegenerate1
        );

        final String startRowKey2 = "5";
        final String endRowKey2 = "10";
        final String regionName2 = "region-2";
        final long regionId2 = 2L;
        final boolean isDegenerate2 = false;
        final HBaseRegion hBaseRegion2 = new HBaseRegion(
                startRowKey2.getBytes(StandardCharsets.UTF_8),
                endRowKey2.getBytes(StandardCharsets.UTF_8),
                regionName2,
                regionId2,
                isDegenerate2
        );

        final List<HBaseRegion> regions = Arrays.asList(hBaseRegion1, hBaseRegion2);
        hBaseClientService.addHBaseRegions(regions);

        runner.run(1);
        runner.assertAllFlowFilesTransferred(ListHBaseRegions.REL_SUCCESS, 2);
        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHBaseRegions.REL_SUCCESS);

        assertEquals(String.valueOf(regionId1), flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey1, flowFiles.get(0).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));

        assertEquals(String.valueOf(regionId2), flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_ID_ATTR));
        assertEquals(regionName2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_NAME_ATTR));
        assertEquals(startRowKey2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_START_ROW_ATTR));
        assertEquals(endRowKey2, flowFiles.get(1).getAttribute(ListHBaseRegions.HBASE_REGION_END_ROW_ATTR));
    }
}
