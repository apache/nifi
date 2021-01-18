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
package org.apache.nifi.stateless.runtimes.yarn;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.stateless.core.security.StatelessSecurityUtility;

public class YARNServiceUtil {
    private final String YARNUrl;
    private final String imageName;

    public YARNServiceUtil(String YARNUrl, String imageName) {
        this.YARNUrl = YARNUrl;
        this.imageName = imageName;
    }

    public boolean launchYARNService(String name, int containerCount, String[] argLaunchCommand, StringBuilder outMessage) {

        //YARN cannot handle commas in a launch command...
        String[] updatedLaunchCommand = new String[argLaunchCommand.length];
        for(int i = 0; i < argLaunchCommand.length; i++){
            if(argLaunchCommand[i].equals("--json"))
                updatedLaunchCommand[i] = "--yarnjson";
            else
                updatedLaunchCommand[i] = argLaunchCommand[i]
                        .replace(',',';')
                        .replace("}}","} }");
        }

        JsonObject spec = new JsonObject();
        spec.addProperty("name", name.substring(0, Math.min(name.length(), 25))); //truncate name
        spec.addProperty("version", "1.0.0");
        spec.addProperty("description", "Stateless NiFi service");

        JsonObject component = new JsonObject();
        component.addProperty("name", "mc");
        component.addProperty("number_of_containers", containerCount);

        JsonObject artifact = new JsonObject();
        artifact.addProperty("id", this.imageName);
        artifact.addProperty("type", "DOCKER");
        component.add("artifact", artifact);

        component.addProperty("launch_command", String.join(",", updatedLaunchCommand));

        JsonObject resource = new JsonObject();
        resource.addProperty("cpus", 1);
        resource.addProperty("memory", "512");
        component.add("resource", resource);

        JsonObject env = new JsonObject();
        env.addProperty("YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE", "true");
        JsonObject configuration = new JsonObject();
        configuration.add("env", env);
        component.add("configuration", configuration);

        JsonArray components = new JsonArray();
        components.add(component);
        spec.add("components", components);

        HttpPost request = new HttpPost(
            this.YARNUrl + "/app/v1/services?user.name=" + System.getProperty("user.name")
        );
        System.out.println("Running YARN service with the following definition:");
        System.out.println(StatelessSecurityUtility.formatJson(spec));
        System.out.println("Launch Command");
        System.out.println(StatelessSecurityUtility.formatArgs(updatedLaunchCommand, true));

        try {
            request.setEntity(new StringEntity(spec.toString(), " application/json", StandardCharsets.UTF_8.toString()));
            HttpResponse response = HttpClientBuilder.create().build().execute(request);
            outMessage.append(new BasicResponseHandler().handleResponse(response));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            outMessage.append(e.getMessage());
            return false;
        }
    }

}
