// ClusterConnectionService migrated from Angular
import { Observable } from 'rxjs';
import store from '../state/store';

export function isDisconnectionAcknowledged(): boolean {
  const state = store.getState();
  return state.clusterSummary?.disconnectionAcknowledged ?? false;
}

export function disconnectionAcknowledged$(): Observable<boolean> {
  return new Observable<boolean>((subscriber) => {
    let current = isDisconnectionAcknowledged();
    subscriber.next(current);

    const unsubscribe = store.subscribe(() => {
      const next = isDisconnectionAcknowledged();
      if (next !== current) {
        current = next;
        subscriber.next(next);
      }
    });

    return unsubscribe;
  });
}
