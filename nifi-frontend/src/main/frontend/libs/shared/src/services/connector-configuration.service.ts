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

import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import {
    ConfigurationStepNamesEntity,
    ConfigurationStepEntity,
    ConfigurationStepConfiguration,
    ConnectorConfigStepVerificationRequestEntity,
    ConnectorPropertyAllowableValuesEntity,
    StepDocumentationEntity,
    ConnectorEntity,
    SecretsEntity,
    UploadContextKey,
    UploadRequest,
    Revision
} from '../types';

/**
 * Service for managing connector configuration via the NiFi REST API.
 * Handles the save-step and apply-update lifecycle.
 */
@Injectable({ providedIn: 'root' })
export class ConnectorConfigurationService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-api/connectors';

    /**
     * Get a single connector by ID (used for polling state).
     * Endpoint: GET /connectors/{id}
     */
    getConnector(id: string): Observable<ConnectorEntity> {
        return this.httpClient.get<ConnectorEntity>(`${ConnectorConfigurationService.API}/${id}`);
    }

    /**
     * Get configuration step names for a connector.
     * Endpoint: GET /connectors/{id}/configuration-steps
     */
    getConfigurationSteps(id: string): Observable<ConfigurationStepNamesEntity> {
        return this.httpClient.get<ConfigurationStepNamesEntity>(
            `${ConnectorConfigurationService.API}/${id}/configuration-steps`
        );
    }

    /**
     * Get a specific configuration step by name.
     * Endpoint: GET /connectors/{id}/configuration-steps/{stepName}
     */
    getConfigurationStep(id: string, stepName: string): Observable<ConfigurationStepEntity> {
        return this.httpClient.get<ConfigurationStepEntity>(
            `${ConnectorConfigurationService.API}/${id}/configuration-steps/${encodeURIComponent(stepName)}`
        );
    }

    /**
     * Update a configuration step (save step configuration).
     * Endpoint: PUT /connectors/{id}/configuration-steps/{stepName}
     */
    updateConfigurationStep(
        id: string,
        stepName: string,
        configuration: ConfigurationStepConfiguration,
        parentConnectorRevision: Revision,
        disconnectedNodeAcknowledged = false
    ): Observable<ConfigurationStepEntity> {
        return this.httpClient.put<ConfigurationStepEntity>(
            `${ConnectorConfigurationService.API}/${id}/configuration-steps/${encodeURIComponent(stepName)}`,
            {
                parentConnectorId: id,
                parentConnectorRevision,
                disconnectedNodeAcknowledged,
                configurationStep: configuration
            }
        );
    }

    /**
     * Submit a verification request for a configuration step (async API).
     * Returns immediately with a request ID for polling.
     * Endpoint: POST /connectors/{id}/configuration-steps/{stepName}/verify-config
     */
    submitVerificationRequest(
        connectorId: string,
        stepName: string,
        configuration: ConfigurationStepConfiguration
    ): Observable<ConnectorConfigStepVerificationRequestEntity> {
        return this.httpClient.post<ConnectorConfigStepVerificationRequestEntity>(
            `${ConnectorConfigurationService.API}/${connectorId}/configuration-steps/${encodeURIComponent(stepName)}/verify-config`,
            {
                request: {
                    connectorId,
                    configurationStepName: stepName,
                    configurationStep: configuration
                }
            }
        );
    }

    /**
     * Poll for verification request status.
     * Endpoint: GET /connectors/{id}/configuration-steps/{stepName}/verify-config/{requestId}
     */
    getVerificationRequest(
        connectorId: string,
        stepName: string,
        requestId: string
    ): Observable<ConnectorConfigStepVerificationRequestEntity> {
        return this.httpClient.get<ConnectorConfigStepVerificationRequestEntity>(
            `${ConnectorConfigurationService.API}/${connectorId}/configuration-steps/${encodeURIComponent(stepName)}/verify-config/${requestId}`
        );
    }

    /**
     * Delete a verification request (cleanup after completion or cancellation).
     * Endpoint: DELETE /connectors/{id}/configuration-steps/{stepName}/verify-config/{requestId}
     */
    deleteVerificationRequest(
        connectorId: string,
        stepName: string,
        requestId: string
    ): Observable<ConnectorConfigStepVerificationRequestEntity> {
        return this.httpClient.delete<ConnectorConfigStepVerificationRequestEntity>(
            `${ConnectorConfigurationService.API}/${connectorId}/configuration-steps/${encodeURIComponent(stepName)}/verify-config/${requestId}`
        );
    }

    /**
     * Get allowable values for a property that supports dynamic fetching.
     * Endpoint: GET /connectors/{id}/configuration-steps/{stepName}/property-groups/{groupName}/properties/{propertyName}/allowable-values
     */
    getPropertyAllowableValues(
        connectorId: string,
        stepName: string,
        propertyGroupName: string,
        propertyName: string,
        filter?: string
    ): Observable<ConnectorPropertyAllowableValuesEntity> {
        let url =
            `${ConnectorConfigurationService.API}/${connectorId}` +
            `/configuration-steps/${encodeURIComponent(stepName)}` +
            `/property-groups/${encodeURIComponent(propertyGroupName)}` +
            `/properties/${encodeURIComponent(propertyName)}` +
            `/allowable-values`;

        if (filter) {
            url += `?filter=${encodeURIComponent(filter)}`;
        }

        return this.httpClient.get<ConnectorPropertyAllowableValuesEntity>(url);
    }

    /**
     * Get step documentation for a configuration step.
     * Endpoint: GET /nifi-api/flow/steps/{group}/{artifact}/{version}/{connectorType}/{stepName}
     */
    getStepDocumentation(
        group: string,
        artifact: string,
        version: string,
        connectorType: string,
        stepName: string
    ): Observable<StepDocumentationEntity> {
        const flowApi = '../nifi-api/flow';
        return this.httpClient.get<StepDocumentationEntity>(
            `${flowApi}/steps/${encodeURIComponent(group)}/${encodeURIComponent(artifact)}/${encodeURIComponent(version)}/${encodeURIComponent(connectorType)}/${encodeURIComponent(stepName)}`
        );
    }

    // ========================================================================================
    // Apply Update Methods
    // ========================================================================================

    /**
     * Apply pending configuration changes to a connector.
     * Endpoint: POST /connectors/{id}/apply-update
     */
    applyConnectorUpdate(
        connectorId: string,
        revision: Revision,
        disconnectedNodeAcknowledged = false
    ): Observable<ConnectorEntity> {
        return this.httpClient.post<ConnectorEntity>(
            `${ConnectorConfigurationService.API}/${connectorId}/apply-update`,
            {
                id: connectorId,
                revision,
                disconnectedNodeAcknowledged
            }
        );
    }

    // ========================================================================================
    // Secrets Management Methods
    // ========================================================================================

    /**
     * Get all available secrets for configuring a connector.
     * Returns metadata for secrets from all configured secret providers.
     * Note: Actual secret values are never exposed via this API.
     * Endpoint: GET /connectors/{id}/secrets
     */
    getSecrets(connectorId: string): Observable<SecretsEntity> {
        return this.httpClient.get<SecretsEntity>(`${ConnectorConfigurationService.API}/${connectorId}/secrets`);
    }

    // ========================================================================================
    // Asset Management Methods
    // ========================================================================================

    /**
     * Create an upload request for a connector asset.
     * Uses the shared upload system with CONNECTOR_ASSET context.
     */
    createAssetUploadRequest(connectorId: string, file: File, propertyName: string): UploadRequest {
        return {
            file,
            context: UploadContextKey.CONNECTOR_ASSET,
            url: `${ConnectorConfigurationService.API}/${connectorId}/assets`,
            monitorGlobally: false,
            metadata: { propertyName }
        };
    }
}
