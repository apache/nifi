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
import { defer, Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { Client } from './client.service';
import { ControllerServiceEntity, ControllerServiceReferencingComponentEntity, safeApiPath } from '@nifi/shared';
import { ClusterConnectionService } from './cluster-connection.service';

@Injectable({ providedIn: 'root' })
export class ControllerServiceStateService {
    private httpClient = inject(HttpClient);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    private static readonly API: string = '../nifi-api';

    getControllerService(id: string): Observable<ControllerServiceEntity> {
        const uiOnly: any = { uiOnly: true };
        return defer(() =>
            this.httpClient.get<ControllerServiceEntity>(
                `${ControllerServiceStateService.API}/controller-services/${safeApiPath(id)}`,
                { params: uiOnly }
            )
        );
    }

    setEnable(controllerService: ControllerServiceEntity, enabled: boolean): Observable<any> {
        return defer(() =>
            this.httpClient.put(
                `${ControllerServiceStateService.API}/controller-services/${safeApiPath(controllerService.id)}/run-status`,
                {
                    revision: this.client.getRevision(controllerService),
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    state: enabled ? 'ENABLED' : 'DISABLED',
                    uiOnly: true
                }
            )
        );
    }

    updateReferencingServices(controllerService: ControllerServiceEntity, enabled: boolean): Observable<any> {
        const referencingComponentRevisions: { [key: string]: any } = {};
        this.getReferencingComponentRevisions(
            controllerService.component?.referencingComponents ?? [],
            referencingComponentRevisions,
            true
        );

        return defer(() =>
            this.httpClient.put(
                `${ControllerServiceStateService.API}/controller-services/${safeApiPath(controllerService.id)}/references`,
                {
                    id: controllerService.id,
                    state: enabled ? 'ENABLED' : 'DISABLED',
                    referencingComponentRevisions: referencingComponentRevisions,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    uiOnly: true
                }
            )
        );
    }

    updateReferencingSchedulableComponents(
        controllerService: ControllerServiceEntity,
        running: boolean
    ): Observable<any> {
        const referencingComponentRevisions: { [key: string]: any } = {};
        this.getReferencingComponentRevisions(
            controllerService.component?.referencingComponents ?? [],
            referencingComponentRevisions,
            false
        );

        return defer(() =>
            this.httpClient.put(
                `${ControllerServiceStateService.API}/controller-services/${safeApiPath(controllerService.id)}/references`,
                {
                    id: controllerService.id,
                    state: running ? 'RUNNING' : 'STOPPED',
                    referencingComponentRevisions: referencingComponentRevisions,
                    disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                    uiOnly: true
                }
            )
        );
    }

    /**
     * Gathers all referencing component revisions.
     *
     * @param referencingComponents
     * @param referencingComponentRevisions
     * @param serviceOnly - true includes only services, false includes only schedulable components
     */
    private getReferencingComponentRevisions(
        referencingComponents: ControllerServiceReferencingComponentEntity[] | undefined,
        referencingComponentRevisions: { [key: string]: any },
        serviceOnly: boolean
    ): void {
        // include the revision of each referencing component
        (referencingComponents ?? []).forEach((referencingComponentEntity) => {
            const referencingComponent = referencingComponentEntity.component;
            if (!referencingComponent) {
                return;
            }

            if (serviceOnly) {
                if (referencingComponent.referenceType === 'ControllerService') {
                    referencingComponentRevisions[referencingComponentEntity.id] =
                        this.client.getRevision(referencingComponentEntity);
                }
            } else {
                if (
                    referencingComponent.referenceType === 'Processor' ||
                    referencingComponent.referenceType === 'ReportingTask'
                ) {
                    referencingComponentRevisions[referencingComponentEntity.id] =
                        this.client.getRevision(referencingComponentEntity);
                }
            }

            // a reference cycle is evaluated where the service was first encountered, so its children are not walked again
            if (referencingComponent.referenceCycle) {
                return;
            }

            // recurse
            this.getReferencingComponentRevisions(
                referencingComponent.referencingComponents ?? [],
                referencingComponentRevisions,
                serviceOnly
            );
        });
    }
}
