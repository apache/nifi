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

import { Injectable } from "@angular/core";
import { delay, Observable, of } from "rxjs";

@Injectable({ providedIn: 'root'})
export class FlowService {

  getFlow(): Observable<any> {
    const flow = {
      "permissions": {
        "canRead": true,
        "canWrite": true
      },
      "processGroupFlow": {
        "id": "1edb8929-018a-1000-814c-5672cf7fc951",
        "uri": "https://localhost:8443/nifi-api/flow/process-groups/1edb8929-018a-1000-814c-5672cf7fc951",
        "breadcrumb": {
          "id": "1edb8929-018a-1000-814c-5672cf7fc951",
          "permissions": {
            "canRead": true,
            "canWrite": true
          },
          "breadcrumb": {
            "id": "1edb8929-018a-1000-814c-5672cf7fc951",
            "name": "NiFi Flow2"
          }
        },
        "flow": {
          "processGroups": [],
          "remoteProcessGroups": [],
          "processors": [],
          "inputPorts": [],
          "outputPorts": [],
          "connections": [],
          "labels": [],
          "funnels": [
            {
              "revision": {
                "clientId": "2d12389f-018a-1000-2762-b1abccf8d105",
                "version": 1
              },
              "id": "2d1270d7-018a-1000-ae17-9624e25fae44",
              "uri": "https://localhost:8443/nifi-api/funnels/2d1270d7-018a-1000-ae17-9624e25fae44",
              "position": {
                "x": 892,
                "y": 150
              },
              "permissions": {
                "canRead": true,
                "canWrite": true
              },
              "component": {
                "id": "2d1270d7-018a-1000-ae17-9624e25fae44",
                "parentGroupId": "1edb8929-018a-1000-814c-5672cf7fc951",
                "position": {
                  "x": 892,
                  "y": 150
                }
              }
            }
          ]
        },
        "lastRefreshed": "10:21:41 EDT"
      }
    };
    return of(flow).pipe(delay(2000));
  }
}
