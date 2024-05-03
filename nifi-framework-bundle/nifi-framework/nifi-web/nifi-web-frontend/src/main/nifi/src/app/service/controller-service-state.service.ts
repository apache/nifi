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
import {
    ControllerServiceEntity,
    ControllerServiceReferencingComponent,
    ControllerServiceReferencingComponentEntity
} from '../state/shared';
import { Client } from './client.service';
import { NiFiCommon } from './nifi-common.service';
import { ClusterConnectionService } from './cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class ControllerServiceStateService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private nifiCommon: NiFiCommon,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {}

    getControllerService(id: string): Observable<any> {
        const uiOnly: any = { uiOnly: true };
        return this.httpClient.get(`${ControllerServiceStateService.API}/controller-services/${id}`, {
            params: uiOnly
        });
    }

    setEnable(controllerService: ControllerServiceEntity, enabled: boolean): Observable<any> {
        return this.httpClient.put(`${this.nifiCommon.stripProtocol(controllerService.uri)}/run-status`, {
            revision: this.client.getRevision(controllerService),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            state: enabled ? 'ENABLED' : 'DISABLED',
            uiOnly: true
        });
    }

    updateReferencingServices(controllerService: ControllerServiceEntity, enabled: boolean): Observable<any> {
        const referencingComponentRevisions: { [key: string]: any } = {};
        this.getReferencingComponentRevisions(
            controllerService.component.referencingComponents,
            referencingComponentRevisions,
            true
        );

        return this.httpClient.put(`${this.nifiCommon.stripProtocol(controllerService.uri)}/references`, {
            id: controllerService.id,
            state: enabled ? 'ENABLED' : 'DISABLED',
            referencingComponentRevisions: referencingComponentRevisions,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            uiOnly: true
        });
    }

    updateReferencingSchedulableComponents(
        controllerService: ControllerServiceEntity,
        running: boolean
    ): Observable<any> {
        const referencingComponentRevisions: { [key: string]: any } = {};
        this.getReferencingComponentRevisions(
            controllerService.component.referencingComponents,
            referencingComponentRevisions,
            false
        );

        return this.httpClient.put(`${this.nifiCommon.stripProtocol(controllerService.uri)}/references`, {
            id: controllerService.id,
            state: running ? 'RUNNING' : 'STOPPED',
            referencingComponentRevisions: referencingComponentRevisions,
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            uiOnly: true
        });
    }

    /**
     * Gathers all referencing component revisions.
     *
     * @param referencingComponents
     * @param referencingComponentRevisions
     * @param serviceOnly - true includes only services, false includes only schedulable components
     */
    private getReferencingComponentRevisions(
        referencingComponents: ControllerServiceReferencingComponentEntity[],
        referencingComponentRevisions: { [key: string]: any },
        serviceOnly: boolean
    ): void {
        if (this.nifiCommon.isEmpty(referencingComponents)) {
            return;
        }

        // include the revision of each referencing component
        referencingComponents.forEach((referencingComponentEntity) => {
            const referencingComponent: ControllerServiceReferencingComponent = referencingComponentEntity.component;

            if (serviceOnly) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    referencingComponentRevisions[referencingComponent.id] =
                        this.client.getRevision(referencingComponentEntity);
                }
            } else {
                if (
                    referencingComponent.referenceType === 'Processor' ||
                    referencingComponent.referenceType === 'ReportingTask'
                ) {
                    referencingComponentRevisions[referencingComponent.id] =
                        this.client.getRevision(referencingComponentEntity);
                }
            }

            // recurse
            this.getReferencingComponentRevisions(
                referencingComponent.referencingComponents,
                referencingComponentRevisions,
                serviceOnly
            );
        });
    }
}
