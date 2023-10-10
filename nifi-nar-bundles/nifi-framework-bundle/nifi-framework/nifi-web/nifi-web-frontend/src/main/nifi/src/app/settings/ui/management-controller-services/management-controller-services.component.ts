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

import { Component, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { ManagementControllerServicesState } from '../../state/management-controller-services';
import { selectManagementControllerServicesState } from '../../state/management-controller-services/management-controller-services.selectors';
import {
    loadManagementControllerServices,
    openNewControllerServiceDialog,
    promptControllerServiceDeletion
} from '../../state/management-controller-services/management-controller-services.actions';
import { ControllerServiceEntity } from '../../../state/shared';

@Component({
    selector: 'management-controller-services',
    templateUrl: './management-controller-services.component.html',
    styleUrls: ['./management-controller-services.component.scss']
})
export class ManagementControllerServices implements OnInit {
    serviceState$ = this.store.select(selectManagementControllerServicesState);

    constructor(private store: Store<ManagementControllerServicesState>) {}

    ngOnInit(): void {
        this.store.dispatch(loadManagementControllerServices());
    }

    openNewControllerServiceDialog(): void {
        this.store.dispatch(openNewControllerServiceDialog());
    }

    refreshControllerServiceListing(): void {
        this.store.dispatch(loadManagementControllerServices());
    }

    deleteControllerService(entity: ControllerServiceEntity): void {
        this.store.dispatch(
            promptControllerServiceDeletion({
                request: {
                    controllerService: entity
                }
            })
        );
    }
}
