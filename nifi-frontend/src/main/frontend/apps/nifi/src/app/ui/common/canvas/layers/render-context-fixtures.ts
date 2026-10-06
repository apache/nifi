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

import { NiFiCommon } from '@nifi/shared';
import { CanvasFormatUtils } from '../canvas-format-utils.service';
import { TextEllipsisUtils } from '../utils/text-ellipsis.utils';
import { BaseRenderContext } from './render-context.types';

export interface BaseRenderContextFixtureOverrides {
    textEllipsis?: Partial<TextEllipsisUtils>;
    formatUtils?: Partial<CanvasFormatUtils>;
    nifiCommon?: Partial<NiFiCommon>;
}

export function createBaseRenderContextFixture(
    overrides: BaseRenderContextFixtureOverrides = {}
): Pick<BaseRenderContext, 'textEllipsis' | 'formatUtils' | 'nifiCommon'> {
    return {
        textEllipsis: (overrides.textEllipsis ?? {}) as unknown as TextEllipsisUtils,
        formatUtils: (overrides.formatUtils ?? {}) as unknown as CanvasFormatUtils,
        nifiCommon: (overrides.nifiCommon ?? {}) as unknown as NiFiCommon
    };
}
