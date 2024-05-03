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
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Bundle } from '../state/shared';

@Injectable({ providedIn: 'root' })
export class ExtensionTypesService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    getProcessorTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/processor-types`);
    }

    getProcessorVersionsForType(processorType: string, bundle: Bundle): Observable<any> {
        const params = {
            bundleGroupFilter: bundle.group,
            bundleArtifactFilter: bundle.artifact,
            type: processorType
        };
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/processor-types`, { params });
    }

    getControllerServiceTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/controller-service-types`);
    }

    getControllerServiceVersionsForType(serviceType: string, bundle: Bundle): Observable<any> {
        const params: any = {
            serviceBundleGroup: bundle.group,
            serviceBundleArtifact: bundle.artifact,
            typeFilter: serviceType
        };
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/controller-service-types`, { params });
    }

    getImplementingControllerServiceTypes(serviceType: string, bundle: Bundle): Observable<any> {
        const params: any = {
            serviceType,
            serviceBundleGroup: bundle.group,
            serviceBundleArtifact: bundle.artifact,
            serviceBundleVersion: bundle.version
        };
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/controller-service-types`, { params });
    }

    getReportingTaskTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/reporting-task-types`);
    }

    getReportingTaskVersionsForType(reportingTaskType: string, bundle: Bundle): Observable<any> {
        const params = {
            serviceBundleGroup: bundle.group,
            serviceBundleArtifact: bundle.artifact,
            type: reportingTaskType
        };
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/reporting-task-types`, { params });
    }

    getRegistryClientTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/controller/registry-types`);
    }

    getPrioritizers(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/prioritizers`);
    }

    getFlowAnalysisRuleTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/flow-analysis-rule-types`);
    }

    getFlowAnalysisRuleVersionsForType(flowAnalysisRuleType: string, bundle: Bundle): Observable<any> {
        const params: any = {
            serviceBundleGroup: bundle.group,
            serviceBundleArtifact: bundle.artifact,
            type: flowAnalysisRuleType
        };
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/flow-analysis-rule-types`, { params });
    }

    getParameterProviderTypes(): Observable<any> {
        return this.httpClient.get(`${ExtensionTypesService.API}/flow/parameter-provider-types`);
    }
}
