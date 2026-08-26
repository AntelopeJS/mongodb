type SchemaInitialization = () => Promise<void>;

interface InitializationFailure {
  sequence: number;
  error: unknown;
}

const pendingInitializations = new Set<Promise<void>>();
const initializationFailures: InitializationFailure[] = [];
let isAcceptingInitializations = false;
let nextInitializationSequence = 0;

export function AllowSchemaInitializations(): void {
  isAcceptingInitializations = true;
}

export function PreventSchemaInitializations(): void {
  isAcceptingInitializations = false;
}

export function StartSchemaInitialization(
  initialize: SchemaInitialization,
): boolean {
  if (!isAcceptingInitializations) {
    return false;
  }
  const sequence = nextInitializationSequence++;
  let pending!: Promise<void>;
  pending = Promise.resolve()
    .then(initialize)
    .catch((error) => {
      initializationFailures.push({ sequence, error });
    })
    .finally(() => pendingInitializations.delete(pending));
  pendingInitializations.add(pending);
  return true;
}

export async function DrainSchemaInitializations(): Promise<unknown[]> {
  while (pendingInitializations.size) {
    await Promise.all([...pendingInitializations]);
  }
  const failures = initializationFailures
    .sort((first, second) => first.sequence - second.sequence)
    .map(({ error }) => error);
  initializationFailures.length = 0;
  return failures;
}
