// StatusHistoryService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

export const API = '../nifi-api';

export enum ComponentType {
  Processor = 'Processor',
  ProcessGroup = 'ProcessGroup',
  Connection = 'Connection',
}

export function getComponentStatusHistory$(componentType: ComponentType, componentId: string): Observable<any> {
  let componentPath: string;
  switch (componentType) {
    case ComponentType.Processor:
      componentPath = 'processors';
      break;
    case ComponentType.ProcessGroup:
      componentPath = 'process-groups';
      break;
    case ComponentType.Connection:
      componentPath = 'connections';
      break;
    default:
      throw new Error('Unknown component type');
  }
  return ajax.getJSON(`${API}/flow/${componentPath}/${componentId}/status/history`).pipe(
    map(response => response)
  );
}
