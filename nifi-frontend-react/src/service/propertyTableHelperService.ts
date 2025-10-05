// PropertyTableHelperService migrated from Angular (scaffold)
// The original Angular service is tightly coupled to Angular Material dialogs and
// NgRx store dispatches. This scaffold retains the data-fetching portions and
// documents the remaining UI/workflow pieces for future React implementation.

import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export interface ComponentHistoryEntity {
  componentId?: string;
  componentHistory?: unknown;
  [key: string]: unknown;
}

export function getComponentHistory$(componentId: string): Observable<ComponentHistoryEntity> {
  return ajax
    .getJSON<ComponentHistoryEntity>(`${API}/flow/history/components/${componentId}`)
    .pipe(map((response) => response));
}

// TODO: Port createNewProperty/createNewService workflows once React dialog
// infrastructure is in place. The Angular implementations rely on Material
// dialogs, inline controller-service creation flows, and NgRx dispatches.
