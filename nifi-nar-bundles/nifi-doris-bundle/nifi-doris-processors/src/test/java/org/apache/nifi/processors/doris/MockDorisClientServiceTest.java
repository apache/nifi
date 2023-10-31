package org.apache.nifi.processors.doris;

import org.apache.http.client.methods.HttpPut;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.doris.DorisClientService;
import org.apache.nifi.doris.util.Result;

import java.io.IOException;
import java.util.HashMap;

public class MockDorisClientServiceTest extends AbstractControllerService implements DorisClientService {

    @Override
    public HashMap<String, HttpPut> setClient(String destDatabase, String destTableName, String columns) {
        return null;
    }

    @Override
    public void testConnectivity(ConfigurationContext context) throws IOException {

    }

    @Override
    public void select() {

    }

    @Override
    public Result putJson(String jsonData, String database, String tableName) {
        return null;
    }

}
