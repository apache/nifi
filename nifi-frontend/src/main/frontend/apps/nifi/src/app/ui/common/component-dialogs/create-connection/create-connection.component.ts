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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    ConnectionCreateBody,
    ConnectionCreateRequest,
    CreateConnection as CreateConnectionPayload,
    CreateConnectionDialogRequest,
    SelectedComponent
} from '../../../../state/flow-shared';
import {
    BreadcrumbEntity,
    DocumentedType,
    loadBalanceCompressionStrategies,
    loadBalanceStrategies
} from '../../../../state/shared';
import { AsyncPipe } from '@angular/common';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { MatTabsModule } from '@angular/material/tabs';
import { ComponentType, NifiSpinnerDirective, TextTip, NifiTooltipDirective, CloseOnEscapeDialog } from '@nifi/shared';
import { Prioritizers } from '../edit-connection/prioritizers/prioritizers.component';
import { SourceProcessor } from '../edit-connection/source/source-processor/source-processor.component';
import { DestinationFunnel } from '../edit-connection/destination/destination-funnel/destination-funnel.component';
import { Client } from '../../../../service/client.service';
import { SourceFunnel } from '../edit-connection/source/source-funnel/source-funnel.component';
import { DestinationProcessor } from '../edit-connection/destination/destination-processor/destination-processor.component';
import { DestinationOutputPort } from '../edit-connection/destination/destination-output-port/destination-output-port.component';
import { SourceInputPort } from '../edit-connection/source/source-input-port/source-input-port.component';
import { Observable, of, tap } from 'rxjs';
import { SourceProcessGroup } from '../edit-connection/source/source-process-group/source-process-group.component';
import { DestinationProcessGroup } from '../edit-connection/destination/destination-process-group/destination-process-group.component';
import { SourceRemoteProcessGroup } from '../edit-connection/source/source-remote-process-group/source-remote-process-group.component';
import { DestinationRemoteProcessGroup } from '../edit-connection/destination/destination-remote-process-group/destination-remote-process-group.component';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';
import { getConnectableTypeForDestination, getConnectableTypeForSource } from '../../utils/component-state.utils';

@Component({
    selector: 'create-connection',
    imports: [
        AsyncPipe,
        FormsModule,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        ReactiveFormsModule,
        MatTabsModule,
        Prioritizers,
        SourceProcessor,
        DestinationFunnel,
        SourceFunnel,
        DestinationProcessor,
        DestinationOutputPort,
        SourceInputPort,
        SourceProcessGroup,
        DestinationProcessGroup,
        SourceRemoteProcessGroup,
        DestinationRemoteProcessGroup,
        ContextErrorBanner
    ],
    templateUrl: './create-connection.component.html',
    styleUrls: ['./create-connection.component.scss']
})
export class CreateConnection extends CloseOnEscapeDialog {
    private dialogRequest = inject<CreateConnectionDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private clusterConnectionService = inject(ClusterConnectionService);
    private client = inject(Client);

    @Input() saving$: Observable<boolean> = of(false);
    @Input() availablePrioritizers$: Observable<DocumentedType[]> = of([]);
    @Input() breadcrumbs$: Observable<BreadcrumbEntity | null> = of(null);
    @Output() createConnection = new EventEmitter<CreateConnectionPayload>();

    @Input() set getChildOutputPorts(getChildOutputPorts: (groupId: string) => Observable<any>) {
        if (this.source.componentType == ComponentType.ProcessGroup) {
            this.childOutputPorts$ = getChildOutputPorts(this.source.id).pipe(
                tap((outputPorts) => {
                    if (outputPorts && outputPorts.length === 1) {
                        this.createConnectionForm.get('source')?.setValue(outputPorts[0].id);
                    }
                })
            );
        }
    }

    @Input() set getChildInputPorts(getChildInputPorts: (groupId: string) => Observable<any>) {
        if (this.destination.componentType == ComponentType.ProcessGroup) {
            this.childInputPorts$ = getChildInputPorts(this.destination.id).pipe(
                tap((inputPorts) => {
                    if (inputPorts && inputPorts.length === 1) {
                        this.createConnectionForm.get('destination')?.setValue(inputPorts[0].id);
                    }
                })
            );
        }
    }

    protected readonly loadBalanceStrategies = loadBalanceStrategies;
    protected readonly loadBalanceCompressionStrategies = loadBalanceCompressionStrategies;
    protected readonly ComponentType = ComponentType;
    protected readonly TextTip = TextTip;

    createConnectionForm: FormGroup;
    source: SelectedComponent;
    destination: SelectedComponent;

    childOutputPorts$!: Observable<any> | null;
    childInputPorts$!: Observable<any> | null;

    loadBalancePartitionAttributeRequired = false;
    loadBalanceCompressionRequired = false;

    constructor() {
        super();
        const dialogRequest = this.dialogRequest;

        this.source = dialogRequest.request.source;
        this.destination = dialogRequest.request.destination;

        this.createConnectionForm = this.formBuilder.group({
            name: new FormControl(''),
            flowFileExpiration: new FormControl(dialogRequest.defaults.flowfileExpiration, Validators.required),
            backPressureObjectThreshold: new FormControl(dialogRequest.defaults.objectThreshold, [
                Validators.required,
                Validators.min(0)
            ]),
            backPressureDataSizeThreshold: new FormControl(
                dialogRequest.defaults.dataSizeThreshold,
                Validators.required
            ),
            loadBalanceStrategy: new FormControl('DO_NOT_LOAD_BALANCE', Validators.required),
            prioritizers: new FormControl([])
        });

        if (this.source.componentType == ComponentType.Processor) {
            this.createConnectionForm.addControl('relationships', new FormControl([], Validators.required));
        }

        let sourceValue: string | null = null;
        if (this.source.componentType == ComponentType.RemoteProcessGroup) {
            const outputPorts: any[] = this.source.entity.component.contents.outputPorts;
            if (outputPorts && outputPorts.length === 1) {
                sourceValue = outputPorts[0].id;
            }
        }

        if (
            this.source.componentType == ComponentType.ProcessGroup ||
            this.source.componentType == ComponentType.RemoteProcessGroup
        ) {
            this.createConnectionForm.addControl('source', new FormControl(sourceValue, Validators.required));
        }

        let destinationValue: string | null = null;
        if (this.destination.componentType == ComponentType.RemoteProcessGroup) {
            const inputPorts: any[] = this.destination.entity.component.contents.inputPorts;
            if (inputPorts && inputPorts.length === 1) {
                destinationValue = inputPorts[0].id;
            }
        }

        if (
            this.destination.componentType == ComponentType.ProcessGroup ||
            this.destination.componentType == ComponentType.RemoteProcessGroup
        ) {
            this.createConnectionForm.addControl('destination', new FormControl(destinationValue, Validators.required));
        }
    }

    getCurrentGroupName(breadcrumbs: BreadcrumbEntity): string {
        if (breadcrumbs.permissions.canRead) {
            return breadcrumbs.breadcrumb.name;
        } else {
            return breadcrumbs.id;
        }
    }

    loadBalanceChanged(value: string): void {
        if (value == 'PARTITION_BY_ATTRIBUTE') {
            this.createConnectionForm.addControl('partitionAttribute', new FormControl('', Validators.required));
            this.loadBalancePartitionAttributeRequired = true;
        } else {
            this.createConnectionForm.removeControl('partitionAttribute');
            this.loadBalancePartitionAttributeRequired = false;
        }

        if (value == 'DO_NOT_LOAD_BALANCE') {
            this.loadBalanceCompressionRequired = false;
            this.createConnectionForm.removeControl('compression');
        } else {
            this.loadBalanceCompressionRequired = true;
            this.createConnectionForm.addControl(
                'compression',
                new FormControl('DO_NOT_COMPRESS', Validators.required)
            );
        }
    }

    submitCreateConnection(currentProcessGroupId: string): void {
        const sourceType = getConnectableTypeForSource(this.source.componentType);
        const destinationType = getConnectableTypeForDestination(this.destination.componentType);
        if (!sourceType || !destinationType) {
            throw new Error('Connection source and destination must be connectable component types');
        }

        const source = {
            groupId:
                this.source.componentType == ComponentType.ProcessGroup ||
                this.source.componentType == ComponentType.RemoteProcessGroup
                    ? this.source.id
                    : currentProcessGroupId,
            id:
                this.source.componentType == ComponentType.ProcessGroup ||
                this.source.componentType == ComponentType.RemoteProcessGroup
                    ? this.createConnectionForm.get('source')?.value
                    : this.source.entity.id,
            type: sourceType
        };
        const destination = {
            groupId:
                this.destination.componentType == ComponentType.ProcessGroup ||
                this.destination.componentType == ComponentType.RemoteProcessGroup
                    ? this.destination.id
                    : currentProcessGroupId,
            id:
                this.destination.componentType == ComponentType.ProcessGroup ||
                this.destination.componentType == ComponentType.RemoteProcessGroup
                    ? this.createConnectionForm.get('destination')?.value
                    : this.destination.id,
            type: destinationType
        };
        const component: ConnectionCreateBody = {
            backPressureDataSizeThreshold: this.createConnectionForm.get('backPressureDataSizeThreshold')?.value,
            backPressureObjectThreshold: this.createConnectionForm.get('backPressureObjectThreshold')?.value,
            flowFileExpiration: this.createConnectionForm.get('flowFileExpiration')?.value,
            loadBalanceStrategy: this.createConnectionForm.get('loadBalanceStrategy')?.value,
            name: this.createConnectionForm.get('name')?.value,
            labelIndex: 0,
            prioritizers: this.createConnectionForm.get('prioritizers')?.value,
            selectedRelationships:
                this.source.componentType == ComponentType.Processor
                    ? this.createConnectionForm.get('relationships')?.value
                    : undefined,
            source,
            destination,
            loadBalancePartitionAttribute: this.loadBalancePartitionAttributeRequired
                ? this.createConnectionForm.get('partitionAttribute')?.value
                : '',
            loadBalanceCompression: this.loadBalanceCompressionRequired
                ? this.createConnectionForm.get('compression')?.value
                : 'DO_NOT_COMPRESS',
            ...(this.dialogRequest.request.bends ? { bends: this.dialogRequest.request.bends } : {})
        };
        const payload: ConnectionCreateRequest = {
            revision: {
                version: 0,
                clientId: this.client.getClientId()
            },
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component
        };

        this.createConnection.emit({
            payload
        });
    }

    override isDirty(): boolean {
        return this.createConnectionForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
