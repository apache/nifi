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

import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import { take, takeUntil } from 'rxjs/operators';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorService } from '../../service/connector.service';
import { ErrorHelper } from '../../../../service/error-helper.service';
import { ErrorContextKey } from '../../../../state/error';
import * as ErrorActions from '../../../../state/error/error.actions';
import * as ConnectorCanvasActions from './connector-canvas.actions';

interface GoToServiceDialogRef {
    afterClosed(): Observable<unknown>;
    close(): void;
}

/**
 * Builds a `goToService` callback for a read-only property-table dialog: resolves the
 * current Connector id from the route, fetches the target Controller Service via the
 * connector-scoped endpoint (bypassing the Troubleshooting gate), then navigates to it
 * and closes the dialog. The fetch is torn down if the dialog closes first.
 *
 * Shared between the connector canvas and controller-services effects so the two
 * read-only dialogs cannot drift from each other.
 */
export function bindGoToService(
    store: Store,
    connectorService: ConnectorService,
    errorHelper: ErrorHelper,
    dialogRef: GoToServiceDialogRef,
    connectorId$: Observable<string | null>,
    errorContext: ErrorContextKey
): (serviceId: string) => void {
    return (serviceId: string) => {
        connectorId$.pipe(take(1)).subscribe((connectorId) => {
            if (!connectorId) {
                store.dispatch(
                    ErrorActions.addBannerError({
                        errorContext: {
                            errors: ['Unable to determine Connector id for navigation.'],
                            context: errorContext
                        }
                    })
                );
                return;
            }

            connectorService
                .getControllerService(connectorId, serviceId)
                .pipe(takeUntil(dialogRef.afterClosed()))
                .subscribe({
                    next: (serviceEntity) => {
                        store.dispatch(
                            ConnectorCanvasActions.navigateToControllerService({
                                processGroupId: serviceEntity.component.parentGroupId,
                                serviceId: serviceEntity.id
                            })
                        );
                        dialogRef.close();
                    },
                    error: (errorResponse: HttpErrorResponse) => {
                        store.dispatch(
                            ErrorActions.addBannerError({
                                errorContext: {
                                    errors: [errorHelper.getErrorString(errorResponse)],
                                    context: errorContext
                                }
                            })
                        );
                    }
                });
        });
    };
}
