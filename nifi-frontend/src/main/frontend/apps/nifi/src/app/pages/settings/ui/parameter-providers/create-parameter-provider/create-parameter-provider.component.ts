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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Observable } from 'rxjs';
import { DocumentedType } from '../../../../../state/shared';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
import { CreateParameterProviderDialogRequest } from '../../../state/parameter-providers';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { CloseOnEscapeDialog } from '@nifi/shared';

@Component({
    selector: 'create-parameter-provider',
    imports: [CommonModule, ExtensionCreation],
    templateUrl: './create-parameter-provider.component.html',
    styleUrls: ['./create-parameter-provider.component.scss']
})
export class CreateParameterProvider extends CloseOnEscapeDialog {
    @Input() saving$!: Observable<boolean>;
    @Output() createParameterProvider: EventEmitter<DocumentedType> = new EventEmitter<DocumentedType>();

    parameterProviderTypes: DocumentedType[];

    constructor(@Inject(MAT_DIALOG_DATA) private dialogRequest: CreateParameterProviderDialogRequest) {
        super();
        this.parameterProviderTypes = dialogRequest.parameterProviderTypes;
    }

    parameterProviderTypeSelected(parameterProviderType: DocumentedType) {
        this.createParameterProvider.next(parameterProviderType);
    }
}
