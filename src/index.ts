import { MongoClientOptions } from 'mongodb';
import { ImplementInterface } from '@ajs/core/beta';
import { Connect, Disconnect } from './connection';

export interface Options {
  url: string;
  options?: MongoClientOptions;
}

export async function construct(options: Options) {
  await Connect(options?.url, options?.options);

  ImplementInterface(
    await import('@ajs.local/database/beta/query'),
    await import('./implementations/database/beta/query'),
  );
  ImplementInterface(
    await import('@ajs.local/database/beta/schema'),
    await import('./implementations/database/beta/schema'),
  );
}

export async function destroy() {
  await Disconnect();
}
