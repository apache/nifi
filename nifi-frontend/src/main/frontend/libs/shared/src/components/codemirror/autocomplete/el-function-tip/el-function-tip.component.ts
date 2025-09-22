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
import { NiFiCommon } from '../../../../services/nifi-common.service';
import { ElFunction, ElFunctionTipInput } from '../../../../types';

@Component({
    selector: 'el-function-tip',
    standalone: true,
    templateUrl: './el-function-tip.component.html',
    styleUrls: ['./el-function-tip.component.scss']
})
export class ElFunctionTip {
    @Input() data: ElFunctionTipInput | null = null;

    constructor(private nifiCommon: NiFiCommon) {}

    hasDescription(elFunction: ElFunction): boolean {
        return !this.nifiCommon.isBlank(elFunction.description);
    }

    hasArguments(elFunction: ElFunction): boolean {
        return Object.keys(elFunction.args).length > 0;
    }

    getArguments(elFunction: ElFunction): string[] {
        return Object.keys(elFunction.args);
    }

    getArgumentDescription(elFunction: ElFunction, argument: string) {
        return elFunction.args[argument];
    }
}
