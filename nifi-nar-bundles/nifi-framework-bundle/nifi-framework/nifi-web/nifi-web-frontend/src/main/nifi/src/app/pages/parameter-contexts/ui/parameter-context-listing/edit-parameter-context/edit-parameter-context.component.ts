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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe, NgForOf, NgIf } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable, take, tap } from 'rxjs';
import {
    EditParameterContextRequest,
    ParameterContextEntity,
    ParameterContextUpdateRequestEntity
} from '../../../state/parameter-context-listing';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { Client } from '../../../../../service/client.service';
import { ParameterTable } from '../parameter-table/parameter-table.component';
import { Parameter, ParameterEntity } from '../../../../../state/shared';
import { ProcessGroupReferences } from '../process-group-references/process-group-references.component';
import { ParameterContextInheritance } from '../parameter-context-inheritance/parameter-context-inheritance.component';

@Component({
    selector: 'edit-parameter-context',
    standalone: true,
    templateUrl: './edit-parameter-context.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        NgIf,
        MatTabsModule,
        MatOptionModule,
        MatSelectModule,
        NgForOf,
        AsyncPipe,
        NifiSpinnerDirective,
        NifiSpinnerDirective,
        ParameterTable,
        ProcessGroupReferences,
        ParameterContextInheritance
    ],
    styleUrls: ['./edit-parameter-context.component.scss']
})
export class EditParameterContext {
    @Input() createNewParameter!: () => Observable<Parameter>;
    @Input() editParameter!: (parameter: Parameter) => Observable<Parameter>;
    @Input() updateRequest!: Observable<ParameterContextUpdateRequestEntity | null>;
    @Input() availableParameterContexts$!: Observable<ParameterContextEntity[]>;
    @Input() saving$!: Observable<boolean>;

    @Output() addParameterContext: EventEmitter<any> = new EventEmitter<any>();
    @Output() editParameterContext: EventEmitter<any> = new EventEmitter<any>();

    editParameterContextForm: FormGroup;
    isNew: boolean;

    parameters!: ParameterEntity[];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditParameterContextRequest,
        private formBuilder: FormBuilder,
        private client: Client
    ) {
        if (request.parameterContext) {
            this.isNew = false;

            this.editParameterContextForm = this.formBuilder.group({
                name: new FormControl(request.parameterContext.component.name, Validators.required),
                description: new FormControl(request.parameterContext.component.description),
                parameters: new FormControl(request.parameterContext.component.parameters),
                inheritedParameterContexts: new FormControl(
                    request.parameterContext.component.inheritedParameterContexts
                )
            });
        } else {
            this.isNew = true;

            this.editParameterContextForm = this.formBuilder.group({
                name: new FormControl('', Validators.required),
                description: new FormControl(''),
                parameters: new FormControl([]),
                inheritedParameterContexts: new FormControl([])
            });
        }
    }

    getUpdatedParameters(): string {
        if (this.parameters) {
            const updatedParameters: string[] = this.parameters.map(
                (parameterEntity) => parameterEntity.parameter.name
            );
            return updatedParameters.join(', ');
        }
        return '';
    }

    submitForm() {
        if (this.isNew) {
            const payload: any = {
                revision: {
                    version: 0,
                    clientId: this.client.getClientId()
                },
                component: {
                    name: this.editParameterContextForm.get('name')?.value,
                    description: this.editParameterContextForm.get('description')?.value,
                    parameters: this.editParameterContextForm.get('parameters')?.value,
                    inheritedParameterContexts: this.editParameterContextForm.get('inheritedParameterContexts')?.value
                }
            };

            this.addParameterContext.next(payload);
        } else {
            // @ts-ignore
            const pc: ParameterContextEntity = this.request.parameterContext;

            this.parameters = this.editParameterContextForm.get('parameters')?.value;

            const payload: any = {
                revision: this.client.getRevision(pc),
                component: {
                    id: pc.id,
                    name: this.editParameterContextForm.get('name')?.value,
                    description: this.editParameterContextForm.get('description')?.value,
                    parameters: this.parameters,
                    inheritedParameterContexts: this.editParameterContextForm.get('inheritedParameterContexts')?.value
                }
            };

            this.editParameterContext.next(payload);
        }
    }
}
