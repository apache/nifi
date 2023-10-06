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

import { Injectable } from '@angular/core';
import { Observable, throwError } from 'rxjs';
import { HttpClient } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class ExtensionTypesService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getProcessorTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/processor-types`);
    }

    getControllerServiceTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/controller-service-types`);
    }

    getReportingTaskTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/reporting-task-types`);
    }

    getFlowAnalysisRuleTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/flow-analysis-rule-types`);
    }

    getParameterProviderTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/parameter-provider-types`);
    }
}
