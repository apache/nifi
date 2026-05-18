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

import { Component, Input } from '@angular/core';
import { RegistryClientEntity } from '../../../../state/shared';

export interface VersionControlInformation {
    groupId: string;
    registryId: string;
    registryName: string;
    bucketId: string;
    bucketName: string;
    flowId: string;
    flowName: string;
    flowDescription: string;
    version: string;
    storageLocation?: string;
    state: string;
    stateExplanation: string;
    branch?: string;
}

export interface VersionControlTipInput {
    versionControlInformation: VersionControlInformation;
    registryClients?: RegistryClientEntity[];
}

@Component({
    selector: 'version-control-tip',
    templateUrl: './version-control-tip.component.html',
    styleUrls: ['./version-control-tip.component.scss']
})
export class VersionControlTip {
    @Input() left = 0;
    @Input() top = 0;
    @Input() data: VersionControlTipInput | undefined;

    getTrackingMessage(): string {
        if (this.data) {
            const vci: VersionControlInformation = this.data.versionControlInformation;
            return `Tracking to "${vci.flowName}" Version ${vci.version} in "${vci.registryName} - ${vci.bucketName}"`;
        }

        return '';
    }

    supportsBranching(): boolean {
        if (this.data?.registryClients && this.data?.registryClients.length > 0) {
            const vci = this.data.versionControlInformation;
            return this.data.registryClients.some((client: RegistryClientEntity) => {
                return client.id === vci.registryId && client.component.supportsBranching;
            });
        }
        return false;
    }
}
