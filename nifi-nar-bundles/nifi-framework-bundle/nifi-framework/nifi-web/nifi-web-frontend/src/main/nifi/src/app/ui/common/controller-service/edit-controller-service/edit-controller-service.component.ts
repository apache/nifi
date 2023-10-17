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
import { Client } from '../../../../service/client.service';
import { ControllerServiceEntity, EditControllerServiceRequest, Property } from '../../../../state/shared';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { NgForOf, NgIf } from '@angular/common';
import { MatTabsModule } from '@angular/material/tabs';
import { NiFiCommon } from '../../../../service/nifi-common.service';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { PropertyTable } from '../../property-table/property-table.component';
import { ControllerServiceApi } from '../controller-service-api/controller-service-api.component';
import { Observable } from 'rxjs';

@Component({
    selector: 'edit-controller-service',
    standalone: true,
    templateUrl: './edit-controller-service.component.html',
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
        PropertyTable,
        ControllerServiceApi
    ],
    styleUrls: ['./edit-controller-service.component.scss']
})
export class EditControllerService {
    @Input() createNewProperty!: (allowsSensitive: boolean) => Observable<Property>;
    @Output() editControllerService: EventEmitter<any> = new EventEmitter<any>();

    editControllerServiceForm: FormGroup;

    bulletinLevels = [
        {
            text: 'DEBUG',
            value: 'DEBUG'
        },
        {
            text: 'INFO',
            value: 'INFO'
        },
        {
            text: 'WARN',
            value: 'WARN'
        },
        {
            text: 'ERROR',
            value: 'ERROR'
        },
        {
            text: 'NONE',
            value: 'NONE'
        }
    ];

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditControllerServiceRequest,
        private formBuilder: FormBuilder,
        private client: Client,
        private nifiCommon: NiFiCommon
    ) {
        const serviceProperties: any = request.controllerService.component.properties;
        const properties: Property[] = Object.entries(serviceProperties).map((entry: any) => {
            const [property, value] = entry;
            return {
                property,
                value,
                descriptor: request.controllerService.component.descriptors[property]
            };
        });

        // build the form
        this.editControllerServiceForm = this.formBuilder.group({
            name: new FormControl(request.controllerService.component.name, Validators.required),
            bulletinLevel: new FormControl(request.controllerService.component.bulletinLevel, Validators.required),
            properties: new FormControl(properties),
            comments: new FormControl(request.controllerService.component.comments)
        });
    }

    formatType(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatType(entity.component);
    }

    formatBundle(entity: ControllerServiceEntity): string {
        return this.nifiCommon.formatBundle(entity.component.bundle);
    }

    submitForm() {
        const payload: any = {
            revision: this.client.getRevision(this.request.controllerService),
            component: {
                id: this.request.controllerService.id,
                name: this.editControllerServiceForm.get('name')?.value,
                comments: this.editControllerServiceForm.get('comments')?.value
            }
        };

        const properties: Property[] = this.editControllerServiceForm.get('properties')?.value;
        if (properties.length) {
            const values: { [key: string]: string | null } = {};
            properties.forEach((property) => (values[property.property] = property.value));
            payload.component.properties = values;
        }

        this.editControllerService.next(payload);
    }
}
