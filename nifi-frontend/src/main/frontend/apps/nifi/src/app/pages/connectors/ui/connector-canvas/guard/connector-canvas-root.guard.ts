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

import { inject } from '@angular/core';
import { ActivatedRouteSnapshot, CanActivateFn, Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { catchError, of, switchMap, take } from 'rxjs';
import { HttpErrorResponse } from '@angular/common/http';
import { ConnectorEntity } from '@nifi/shared';
import { ErrorHelper } from '../../../../../service/error-helper.service';
import { ConnectorService } from '../../../service/connector.service';
import {
    loadConnectorEntityFailure,
    loadConnectorEntitySuccess
} from '../../../state/connector-canvas-entity/connector-canvas-entity.actions';

function findConnectorId(route: ActivatedRouteSnapshot): string | undefined {
    let r: ActivatedRouteSnapshot | null = route;
    while (r) {
        const id = r.params['id'];
        if (id) {
            return id;
        }
        r = r.parent;
    }
    return undefined;
}

export const connectorCanvasRootGuard: CanActivateFn = (route: ActivatedRouteSnapshot) => {
    const router = inject(Router);
    const connectorService = inject(ConnectorService);
    const store = inject(Store);
    const errorHelper = inject(ErrorHelper);

    const connectorId = findConnectorId(route);

    if (!connectorId) {
        return of(router.parseUrl('/connectors'));
    }

    return connectorService.getConnector(connectorId).pipe(
        take(1),
        switchMap((connector: ConnectorEntity) => {
            store.dispatch(loadConnectorEntitySuccess({ connectorEntity: connector }));
            return router.navigate(['/connectors', connectorId, 'canvas', connector.component.managedProcessGroupId]);
        }),
        catchError((error: HttpErrorResponse) => {
            store.dispatch(loadConnectorEntityFailure({ error: errorHelper.getErrorString(error) }));
            return of(true);
        })
    );
};
