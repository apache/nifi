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
import { Bundle } from '../../../../state/shared';
import { NiFiCommon } from '@nifi/shared';

@Component({
    selector: 'controller-service-api',
    standalone: true,
    templateUrl: './controller-service-api.component.html',
    styleUrls: ['./controller-service-api.component.scss']
})
export class ControllerServiceApi {
    @Input() type!: string;
    @Input() bundle!: Bundle;

    constructor(private nifiCommon: NiFiCommon) {}

    formatControllerService(type: string, bundle: Bundle) {
        const formattedType: string = this.nifiCommon.formatType({ type, bundle });
        const formattedBundle: string = this.nifiCommon.formatBundle(bundle);
        return `${formattedType} from ${formattedBundle}`;
    }
}
