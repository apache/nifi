/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {
    ConfigurationAnalysisResponse,
    InitiateVerificationRequest,
    PropertyVerificationResponse,
    VerifyPropertiesRequestContext
} from '../state/property-verification';
import { Observable } from 'rxjs';
import { ComponentType, NiFiCommon } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class PropertyVerificationService {
    private httpClient = inject(HttpClient);
    private nifiCommon = inject(NiFiCommon);

    private static readonly API: string = '../nifi-api';

    getAnalysis(request: VerifyPropertiesRequestContext): Observable<ConfigurationAnalysisResponse> {
        const body = {
            configurationAnalysis: {
                componentId: request.entity.id,
                properties: request.properties
            }
        };
        const path = this.nifiCommon.getComponentTypeApiPath(request.componentType);
        return this.httpClient.post(
            `${PropertyVerificationService.API}/${path}/${request.entity.id}/config/analysis`,
            body
        ) as Observable<ConfigurationAnalysisResponse>;
    }

    initiatePropertyVerification(request: InitiateVerificationRequest): Observable<PropertyVerificationResponse> {
        const path = this.nifiCommon.getComponentTypeApiPath(request.componentType);
        return this.httpClient.post(
            `${PropertyVerificationService.API}/${path}/${request.componentId}/config/verification-requests`,
            request.request
        ) as Observable<PropertyVerificationResponse>;
    }

    getPropertyVerificationRequest(
        componentType: ComponentType,
        componentId: string,
        requestId: string
    ): Observable<PropertyVerificationResponse> {
        const path = this.nifiCommon.getComponentTypeApiPath(componentType);
        return this.httpClient.get(
            `${PropertyVerificationService.API}/${path}/${componentId}/config/verification-requests/${requestId}`
        ) as Observable<PropertyVerificationResponse>;
    }

    deletePropertyVerificationRequest(componentType: ComponentType, componentId: string, requestId: string) {
        const path = this.nifiCommon.getComponentTypeApiPath(componentType);
        return this.httpClient.delete(
            `${PropertyVerificationService.API}/${path}/${componentId}/config/verification-requests/${requestId}`
        );
    }
}
