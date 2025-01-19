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
import { MatDialog } from '@angular/material/dialog';
import { catchError, EMPTY, map, Observable, switchMap, take, takeUntil, tap } from 'rxjs';
import {
    ComponentHistoryEntity,
    ControllerServiceCreator,
    ControllerServiceEntity,
    CreateControllerServiceRequest,
    InlineServiceCreationRequest,
    InlineServiceCreationResponse,
    NewPropertyDialogRequest,
    NewPropertyDialogResponse,
    Property,
    PropertyDescriptor,
    PropertyDescriptorRetriever
} from '../state/shared';
import { NewPropertyDialog } from '../ui/common/new-property-dialog/new-property-dialog.component';
import { CreateControllerService } from '../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { ExtensionTypesService } from './extension-types.service';
import { Client } from './client.service';
import { NiFiState } from '../state';
import { Store } from '@ngrx/store';
import { snackBarError } from '../state/error/error.actions';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { LARGE_DIALOG, SMALL_DIALOG } from '@nifi/shared';
import { ErrorHelper } from './error-helper.service';

@Injectable({
    providedIn: 'root'
})
export class PropertyTableHelperService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private dialog: MatDialog,
        private store: Store<NiFiState>,
        private extensionTypesService: ExtensionTypesService,
        private client: Client,
        private errorHelper: ErrorHelper
    ) {}

    getComponentHistory(componentId: string): Observable<ComponentHistoryEntity> {
        return this.httpClient.get<ComponentHistoryEntity>(
            `${PropertyTableHelperService.API}/flow/history/components/${componentId}`
        );
    }

    /**
     * Returns a function that can be used to pass into a PropertyTable to support creating a new property
     * @param id                        id of the component to create the property for
     * @param propertyDescriptorService the service to call to get property descriptors
     */
    createNewProperty(
        id: string,
        propertyDescriptorService: PropertyDescriptorRetriever
    ): (existingProperties: string[], allowsSensitive: boolean) => Observable<Property> {
        return (existingProperties: string[], allowsSensitive: boolean) => {
            const dialogRequest: NewPropertyDialogRequest = { existingProperties, allowsSensitive };
            const newPropertyDialogReference = this.dialog.open(NewPropertyDialog, {
                ...SMALL_DIALOG,
                data: dialogRequest
            });

            return newPropertyDialogReference.componentInstance.newProperty.pipe(
                takeUntil(newPropertyDialogReference.afterClosed()),
                switchMap((dialogResponse: NewPropertyDialogResponse) => {
                    return propertyDescriptorService
                        .getPropertyDescriptor(id, dialogResponse.name, dialogResponse.sensitive)
                        .pipe(
                            take(1),
                            catchError((errorResponse: HttpErrorResponse) => {
                                this.store.dispatch(
                                    snackBarError({
                                        error: this.errorHelper.getErrorString(
                                            errorResponse,
                                            `Failed to get property descriptor for ${dialogResponse.name}.`
                                        )
                                    })
                                );

                                // handle the error here to keep the observable alive so the
                                // user can attempt to create the property again
                                return EMPTY;
                            }),
                            map((response) => {
                                newPropertyDialogReference.close();

                                return {
                                    property: dialogResponse.name,
                                    value: null,
                                    descriptor: response.propertyDescriptor
                                };
                            })
                        );
                })
            );
        };
    }

    /**
     * Returns a function that can be used to pass into a PropertyTable to create a controller service, inline.
     *
     * @param id                        id of the component where the inline-create controller service was initiated
     * @param controllerServiceCreator  service to use to create the controller service and lookup property descriptors
     * @param propertyDescriptorService the service to call to get property descriptors
     * @param afterServiceCreated       OPTIONAL - callback function to possibly dispatch an action after the controller service has been created
     * @param processGroupId            OPTIONAL - process group id to create the
     */
    createNewService(
        id: string,
        controllerServiceCreator: ControllerServiceCreator,
        propertyDescriptorService: PropertyDescriptorRetriever,
        processGroupId?: string | null,
        afterServiceCreated?: (createdResponse: ControllerServiceEntity) => void
    ): (request: InlineServiceCreationRequest) => Observable<InlineServiceCreationResponse> {
        return (request: InlineServiceCreationRequest) => {
            const descriptor: PropertyDescriptor = request.descriptor;

            // fetch all services that implement the requested service api
            return this.extensionTypesService
                .getImplementingControllerServiceTypes(
                    // @ts-ignore
                    descriptor.identifiesControllerService,
                    descriptor.identifiesControllerServiceBundle
                )
                .pipe(
                    take(1),
                    tap({
                        error: (errorResponse: HttpErrorResponse) => {
                            this.store.dispatch(
                                snackBarError({ error: this.errorHelper.getErrorString(errorResponse) })
                            );
                        }
                    }),
                    switchMap((implementingTypesResponse) => {
                        // show the create controller service dialog with the types that implemented the interface
                        const createServiceDialogReference = this.dialog.open(CreateControllerService, {
                            ...LARGE_DIALOG,
                            data: {
                                controllerServiceTypes: implementingTypesResponse.controllerServiceTypes
                            }
                        });

                        return createServiceDialogReference.componentInstance.createControllerService.pipe(
                            takeUntil(createServiceDialogReference.afterClosed()),
                            switchMap((controllerServiceType) => {
                                // typically this sequence would be implemented with ngrx actions, however we are
                                // currently in an edit session, and we need to return both the value (new service id)
                                // and updated property descriptor so the table renders correctly
                                const payload: CreateControllerServiceRequest = {
                                    revision: {
                                        clientId: this.client.getClientId(),
                                        version: 0
                                    },
                                    controllerServiceType: controllerServiceType.type,
                                    controllerServiceBundle: controllerServiceType.bundle
                                };

                                if (processGroupId) {
                                    payload.processGroupId = processGroupId;
                                }

                                return controllerServiceCreator.createControllerService(payload).pipe(
                                    take(1),
                                    catchError((errorResponse: HttpErrorResponse) => {
                                        this.store.dispatch(
                                            snackBarError({
                                                error: this.errorHelper.getErrorString(
                                                    errorResponse,
                                                    `Unable to create new Service.`
                                                )
                                            })
                                        );

                                        // handle the error here to keep the observable alive so the
                                        // user can attempt to create the service again
                                        return EMPTY;
                                    }),
                                    switchMap((createResponse) => {
                                        // if provided, call the callback function
                                        if (afterServiceCreated) {
                                            afterServiceCreated(createResponse);
                                        }

                                        // fetch an updated property descriptor
                                        return propertyDescriptorService
                                            .getPropertyDescriptor(id, descriptor.name, false)
                                            .pipe(
                                                take(1),
                                                tap({
                                                    error: (errorResponse: HttpErrorResponse) => {
                                                        // we've errored getting the descriptor but since the service
                                                        // was already created, we should close the create service dialog
                                                        // so multiple service instances are not inadvertently created
                                                        createServiceDialogReference.close();

                                                        this.store.dispatch(
                                                            snackBarError({
                                                                error: this.errorHelper.getErrorString(
                                                                    errorResponse,
                                                                    'Service created but unable to reload Property Descriptor.'
                                                                )
                                                            })
                                                        );
                                                    }
                                                }),
                                                map((descriptorResponse) => {
                                                    createServiceDialogReference.close();

                                                    return {
                                                        value: createResponse.id,
                                                        descriptor: descriptorResponse.propertyDescriptor
                                                    };
                                                })
                                            );
                                    })
                                );
                            })
                        );
                    })
                );
        };
    }
}
