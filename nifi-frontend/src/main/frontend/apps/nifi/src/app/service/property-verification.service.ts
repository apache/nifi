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

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Client } from './client.service';
import {
    ConfigurationAnalysisResponse,
    InitiateVerificationRequest,
    PropertyVerificationResponse,
    VerifyPropertiesRequestContext
} from '../state/property-verification';
import { Observable } from 'rxjs';
import { NiFiCommon } from '@nifi/shared';

@Injectable({ providedIn: 'root' })
export class PropertyVerificationService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {}

    getAnalysis(request: VerifyPropertiesRequestContext): Observable<ConfigurationAnalysisResponse> {
        const body = {
            configurationAnalysis: {
                componentId: request.entity.id,
                properties: request.properties
            }
        };
        return this.httpClient.post(
            `${this.nifiCommon.stripProtocol(request.entity.uri)}/config/analysis`,
            body
        ) as Observable<ConfigurationAnalysisResponse>;
    }

    initiatePropertyVerification(request: InitiateVerificationRequest): Observable<PropertyVerificationResponse> {
        return this.httpClient.post(
            `${this.nifiCommon.stripProtocol(request.uri)}/config/verification-requests`,
            request.request
        ) as Observable<PropertyVerificationResponse>;
    }

    getPropertyVerificationRequest(requestId: string, uri: string): Observable<PropertyVerificationResponse> {
        return this.httpClient.get(
            `${this.nifiCommon.stripProtocol(uri)}/config/verification-requests/${requestId}`
        ) as Observable<PropertyVerificationResponse>;
    }

    deletePropertyVerificationRequest(requestId: string, uri: string) {
        return this.httpClient.delete(
            `${this.nifiCommon.stripProtocol(uri)}/config/verification-requests/${requestId}`
        );
    }
}
