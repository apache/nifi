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

import { Component, Inject, Input } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { BreadcrumbEntity, CreateConnectionDialogRequest, SelectedComponent } from '../../../state/flow';
import { Store } from '@ngrx/store';
import { ExtensionCreation } from '../../../../../ui/common/extension-creation/extension-creation.component';
import { selectBreadcrumbs, selectSaving } from '../../../state/flow/flow.selectors';
import { AsyncPipe, NgForOf, NgIf, NgSwitch, NgSwitchCase } from '@angular/common';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../ui/common/spinner/nifi-spinner.directive';
import { NifiTooltipDirective } from '../../../../../ui/common/nifi-tooltip.directive';
import { MatTabsModule } from '@angular/material/tabs';
import { TextTip } from '../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ComponentType, TextTipInput } from '../../../../../state/shared';
import { NiFiState } from '../../../../../state';
import { selectPrioritizerTypes } from '../../../../../state/extension-types/extension-types.selectors';
import { Prioritizers } from '../prioritizers/prioritizers.component';
import { SourceProcessor } from '../source/source-processor/source-processor.component';
import { DestinationFunnel } from '../destination/destination-funnel/destination-funnel.component';
import { createConnection } from '../../../state/flow/flow.actions';
import { Client } from '../../../../../service/client.service';
import { CanvasUtils } from '../../../service/canvas-utils.service';
import { SourceFunnel } from '../source/source-funnel/source-funnel.component';
import { DestinationProcessor } from '../destination/destination-processor/destination-processor.component';
import { DestinationOutputPort } from '../destination/destination-output-port/destination-output-port.component';
import { SourceInputPort } from '../source/source-input-port/source-input-port.component';
import { Observable } from 'rxjs';
import { SourceProcessGroup } from '../source/source-process-group/source-process-group.component';
import { DestinationProcessGroup } from '../destination/destination-process-group/destination-process-group.component';
import { SourceRemoteProcessGroup } from '../source/source-remote-process-group/source-remote-process-group.component';
import { DestinationRemoteProcessGroup } from '../destination/destination-remote-process-group/destination-remote-process-group.component';

@Component({
    selector: 'create-connection',
    standalone: true,
    imports: [
        ExtensionCreation,
        AsyncPipe,
        FormsModule,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        NgForOf,
        NgIf,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        ReactiveFormsModule,
        MatTabsModule,
        Prioritizers,
        NgSwitch,
        NgSwitchCase,
        SourceProcessor,
        DestinationFunnel,
        SourceFunnel,
        DestinationProcessor,
        DestinationOutputPort,
        SourceInputPort,
        SourceProcessGroup,
        DestinationProcessGroup,
        SourceRemoteProcessGroup,
        DestinationRemoteProcessGroup
    ],
    templateUrl: './create-connection.component.html',
    styleUrls: ['./create-connection.component.scss']
})
export class CreateConnection {
    @Input() set getChildOutputPorts(getChildOutputPorts: (groupId: string) => Observable<any>) {
        if (this.source.componentType == ComponentType.ProcessGroup) {
            this.childOutputPorts$ = getChildOutputPorts(this.source.id);
        }
    }

    @Input() set getChildInputPorts(getChildInputPorts: (groupId: string) => Observable<any>) {
        if (this.destination.componentType == ComponentType.ProcessGroup) {
            this.childInputPorts$ = getChildInputPorts(this.destination.id);
        }
    }

    protected readonly ComponentType = ComponentType;
    protected readonly TextTip = TextTip;

    saving$ = this.store.select(selectSaving);
    availablePrioritizers$ = this.store.select(selectPrioritizerTypes);
    breadcrumbs$ = this.store.select(selectBreadcrumbs);

    loadBalanceStrategies = [
        {
            text: 'Do not load balance',
            value: 'DO_NOT_LOAD_BALANCE',
            description: 'Do not load balance FlowFiles between nodes in the cluster.'
        },
        {
            text: 'Partition by attribute',
            value: 'PARTITION_BY_ATTRIBUTE',
            description:
                'Determine which node to send a given FlowFile to based on the value of a user-specified FlowFile Attribute. ' +
                'All FlowFiles that have the same value for said Attribute will be sent to the same node in the cluster.'
        },
        {
            text: 'Round robin',
            value: 'ROUND_ROBIN',
            description:
                'FlowFiles will be distributed to nodes in the cluster in a Round-Robin fashion. However, if a node in the ' +
                'cluster is not able to receive data as fast as other nodes, that node may be skipped in one or more iterations ' +
                'in order to maximize throughput of data distribution across the cluster.'
        },
        {
            text: 'Single node',
            value: 'SINGLE_NODE',
            description: 'All FlowFiles will be sent to the same node. Which node they are sent to is not defined.'
        }
    ];

    createConnectionForm: FormGroup;
    source: SelectedComponent;
    destination: SelectedComponent;

    childOutputPorts$!: Observable<any> | null;
    childInputPorts$!: Observable<any> | null;

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: CreateConnectionDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<NiFiState>,
        private canvasUtils: CanvasUtils,
        private client: Client
    ) {
        this.source = dialogRequest.request.source;
        this.destination = dialogRequest.request.destination;

        this.createConnectionForm = this.formBuilder.group({
            name: new FormControl(''),
            flowFileExpiration: new FormControl(dialogRequest.defaults.flowfileExpiration, Validators.required),
            backPressureObjectThreshold: new FormControl(dialogRequest.defaults.objectThreshold, Validators.required),
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

        if (
            this.source.componentType == ComponentType.ProcessGroup ||
            this.source.componentType == ComponentType.RemoteProcessGroup
        ) {
            this.createConnectionForm.addControl('source', new FormControl(null, Validators.required));
        }

        if (
            this.destination.componentType == ComponentType.ProcessGroup ||
            this.destination.componentType == ComponentType.RemoteProcessGroup
        ) {
            this.createConnectionForm.addControl('destination', new FormControl(null, Validators.required));
        }
    }

    getCurrentGroupName(breadcrumbs: BreadcrumbEntity): string {
        if (breadcrumbs.permissions.canRead) {
            return breadcrumbs.breadcrumb.name;
        } else {
            return breadcrumbs.id;
        }
    }

    getSelectOptionTipData(option: any): TextTipInput {
        return {
            text: option.description
        };
    }

    createConnection(): void {
        const payload: any = {
            revision: {
                version: 0,
                clientId: this.client.getClientId()
            },
            component: {
                backPressureDataSizeThreshold: this.createConnectionForm.get('backPressureDataSizeThreshold')?.value,
                backPressureObjectThreshold: this.createConnectionForm.get('backPressureObjectThreshold')?.value,
                flowFileExpiration: this.createConnectionForm.get('flowFileExpiration')?.value,
                loadBalanceStrategy: this.createConnectionForm.get('loadBalanceStrategy')?.value,
                name: this.createConnectionForm.get('name')?.value,
                prioritizers: this.createConnectionForm.get('prioritizers')?.value
            }
        };

        if (this.source.componentType == ComponentType.Processor) {
            payload.component.selectedRelationships = this.createConnectionForm.get('relationships')?.value;
        }

        if (
            this.source.componentType == ComponentType.ProcessGroup ||
            this.source.componentType == ComponentType.RemoteProcessGroup
        ) {
            payload.component.source = {
                groupId: this.source.id,
                id: this.createConnectionForm.get('source')?.value,
                type: this.canvasUtils.getConnectableTypeForSource(this.source.componentType)
            };
        } else {
            payload.component.source = {
                groupId: this.source.entity.component.parentGroupId,
                id: this.source.entity.id,
                type: this.canvasUtils.getConnectableTypeForSource(this.source.componentType)
            };
        }

        if (
            this.destination.componentType == ComponentType.ProcessGroup ||
            this.destination.componentType == ComponentType.RemoteProcessGroup
        ) {
            payload.component.destination = {
                groupId: this.destination.id,
                id: this.createConnectionForm.get('destination')?.value,
                type: this.canvasUtils.getConnectableTypeForDestination(this.destination.componentType)
            };
        } else {
            payload.component.destination = {
                groupId: this.destination.entity.component.parentGroupId,
                id: this.destination.id,
                type: this.canvasUtils.getConnectableTypeForDestination(this.destination.componentType)
            };
        }

        this.store.dispatch(
            createConnection({
                request: {
                    payload
                }
            })
        );
    }
}
