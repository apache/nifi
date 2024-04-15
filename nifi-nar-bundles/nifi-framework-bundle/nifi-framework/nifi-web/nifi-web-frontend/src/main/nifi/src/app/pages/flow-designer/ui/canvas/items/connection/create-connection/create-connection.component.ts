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
import {
    CreateConnectionDialogRequest,
    loadBalanceCompressionStrategies,
    loadBalanceStrategies,
    SelectedComponent
} from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { ExtensionCreation } from '../../../../../../../ui/common/extension-creation/extension-creation.component';
import { selectBreadcrumbs, selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { MatTabsModule } from '@angular/material/tabs';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { ComponentType, SelectOption, TextTipInput } from '../../../../../../../state/shared';
import { NiFiState } from '../../../../../../../state';
import { selectPrioritizerTypes } from '../../../../../../../state/extension-types/extension-types.selectors';
import { Prioritizers } from '../prioritizers/prioritizers.component';
import { SourceProcessor } from '../source/source-processor/source-processor.component';
import { DestinationFunnel } from '../destination/destination-funnel/destination-funnel.component';
import { createConnection } from '../../../../../state/flow/flow.actions';
import { Client } from '../../../../../../../service/client.service';
import { SourceFunnel } from '../source/source-funnel/source-funnel.component';
import { DestinationProcessor } from '../destination/destination-processor/destination-processor.component';
import { DestinationOutputPort } from '../destination/destination-output-port/destination-output-port.component';
import { SourceInputPort } from '../source/source-input-port/source-input-port.component';
import { Observable, tap } from 'rxjs';
import { SourceProcessGroup } from '../source/source-process-group/source-process-group.component';
import { DestinationProcessGroup } from '../destination/destination-process-group/destination-process-group.component';
import { SourceRemoteProcessGroup } from '../source/source-remote-process-group/source-remote-process-group.component';
import { DestinationRemoteProcessGroup } from '../destination/destination-remote-process-group/destination-remote-process-group.component';
import { BreadcrumbEntity } from '../../../../../state/shared';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { CanvasUtils } from '../../../../../service/canvas-utils.service';

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
        DestinationRemoteProcessGroup
    ],
    templateUrl: './create-connection.component.html',
    styleUrls: ['./create-connection.component.scss']
})
export class CreateConnection {
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

    saving$ = this.store.select(selectSaving);
    availablePrioritizers$ = this.store.select(selectPrioritizerTypes);
    breadcrumbs$ = this.store.select(selectBreadcrumbs);

    createConnectionForm: FormGroup;
    source: SelectedComponent;
    destination: SelectedComponent;

    childOutputPorts$!: Observable<any> | null;
    childInputPorts$!: Observable<any> | null;

    loadBalancePartitionAttributeRequired = false;
    loadBalanceCompressionRequired = false;

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: CreateConnectionDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<NiFiState>,
        private canvasUtils: CanvasUtils,
        private clusterConnectionService: ClusterConnectionService,
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

    getSelectOptionTipData(option: SelectOption): TextTipInput {
        return {
            // @ts-ignore
            text: option.description
        };
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

    createConnection(currentProcessGroupId: string): void {
        const payload: any = {
            revision: {
                version: 0,
                clientId: this.client.getClientId()
            },
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
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
                groupId: currentProcessGroupId,
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
                groupId: currentProcessGroupId,
                id: this.destination.id,
                type: this.canvasUtils.getConnectableTypeForDestination(this.destination.componentType)
            };
        }

        if (this.loadBalancePartitionAttributeRequired) {
            payload.component.loadBalancePartitionAttribute =
                this.createConnectionForm.get('partitionAttribute')?.value;
        } else {
            payload.component.loadBalancePartitionAttribute = '';
        }

        if (this.loadBalanceCompressionRequired) {
            payload.component.loadBalanceCompression = this.createConnectionForm.get('compression')?.value;
        } else {
            payload.component.loadBalanceCompression = 'DO_NOT_COMPRESS';
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
