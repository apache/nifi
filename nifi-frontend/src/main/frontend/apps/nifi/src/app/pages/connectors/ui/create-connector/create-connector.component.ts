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

import { Component, EventEmitter, Output, inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { AsyncPipe } from '@angular/common';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { ExtensionCreation } from '../../../../ui/common/extension-creation/extension-creation.component';
import { DocumentedType } from '../../../../state/shared';
import { selectSaving } from '../../state/connectors-listing/connectors-listing.selectors';
import {
    selectConnectorTypes,
    selectExtensionTypesLoadingStatus
} from '../../../../state/extension-types/extension-types.selectors';

@Component({
    selector: 'create-connector',
    imports: [ExtensionCreation, AsyncPipe],
    templateUrl: './create-connector.component.html',
    styleUrls: ['./create-connector.component.scss']
})
export class CreateConnector extends CloseOnEscapeDialog {
    private store = inject(Store);

    connectorTypes$ = this.store.select(selectConnectorTypes);
    connectorTypesLoadingStatus$ = this.store.select(selectExtensionTypesLoadingStatus);
    saving$ = this.store.select(selectSaving);

    @Output() createConnector = new EventEmitter<DocumentedType>();

    createNewConnector(connectorType: DocumentedType): void {
        this.createConnector.emit(connectorType);
    }
}
