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

export const propertyVerificationFeatureKey = 'propertyVerification';

export enum Outcome {
    SUCCESSFUL = 'SUCCESSFUL',
    FAILED = 'FAILED',
    SKIPPED = 'SKIPPED'
}

export interface ModifiedProperties {
    [key: string]: string | null;
}

export interface VerifyPropertiesRequestContext {
    entity: any;
    properties: ModifiedProperties;
}

export interface ConfigurationAnalysisResponse {
    requestContext: VerifyPropertiesRequestContext;
    configurationAnalysis: ConfigurationAnalysis;
}

export interface ConfigurationAnalysis {
    componentId: string;
    properties: any;
    referencedAttributes: any;
    supportsVerification: boolean;
}

export interface ConfigVerificationResult {
    outcome: Outcome;
    verificationStepName: string;
    explanation: string;
}

export interface PropertyVerificationRequest {
    complete: boolean;
    componentId: string;
    lastUpdated: string;
    percentCompleted: number;
    properties: any;
    requestId: string;
    uri: string;
    state: string;
    results?: ConfigVerificationResult[];
    failureReason?: string;
}

export interface PropertyVerificationResponse {
    request: PropertyVerificationRequest;
}

export interface VerifyConfigRequest {
    componentId: string;
    properties: any;
    attributes: any;
    results?: ConfigVerificationResult[];
}

export interface VerifyConfigRequestEntity {
    request: VerifyConfigRequest;
}

export interface InitiateVerificationRequest {
    uri: string;
    request: VerifyConfigRequestEntity;
}

export interface PropertyVerificationState {
    activeRequest: PropertyVerificationRequest | null;
    requestContext: VerifyPropertiesRequestContext | null;
    configurationAnalysis: ConfigurationAnalysis | null;
    results: ConfigVerificationResult[];
    attributes: { [key: string]: string } | null;
    status: 'pending' | 'loading' | 'success';
}
