// ClientService migrated from Angular with TypeScript support
import { v4 as uuidv4 } from 'uuid';

export interface RevisionHolder {
  revision?: {
    version: number;
  };
}

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem?(key: string): void;
}

const memoryStorage = new Map<string, string>();

const defaultStorage: StorageLike =
  typeof window !== 'undefined' && window.sessionStorage
    ? window.sessionStorage
    : {
        getItem: (key: string) => memoryStorage.get(key) ?? null,
        setItem: (key: string, value: string) => {
          memoryStorage.set(key, value);
        },
        removeItem: (key: string) => {
          memoryStorage.delete(key);
        }
      };

export class ClientService {
  private clientId: string;

  constructor(private storage: StorageLike = defaultStorage) {
    const storedClientId = this.storage.getItem('clientId');
    const resolvedClientId = storedClientId ?? uuidv4();

    if (!storedClientId) {
      this.storage.setItem('clientId', resolvedClientId);
    }

    this.clientId = resolvedClientId;
  }

  getClientId(): string {
    return this.clientId;
  }

  getRevision<T extends RevisionHolder>(component: T): { clientId: string; version: number } {
    const version = component.revision?.version ?? 0;
    return {
      clientId: this.clientId,
      version
    };
  }
}

const clientService = new ClientService();
export default clientService;
