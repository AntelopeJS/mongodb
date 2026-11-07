/**
 * Splits the drawn scenarios across worker threads and merges what they
 * report into what one thread running them in order would have found.
 * Scenario `i` belongs to shard `i % jobs`; a shard explores its scenarios
 * in increasing index order, so the first break it reports is its lowest.
 */
import { Worker } from "node:worker_threads";

/** The shortest counterexample of one rule: the trace length and the printed run. */
export interface Counterexample {
  length: number;
  text: string;
}

/** What exploring one scenario under every order left. */
export interface ScenarioResult {
  index: number;
  orders: number;
  exhausted: boolean;
  /** A rule outside the recorded findings has a counterexample in this scenario. */
  broken: boolean;
  /** Per rule, the shortest counterexample this scenario found that its shard had not found before. */
  counterexamples: [rule: string, counterexample: Counterexample][];
}

/** What a run explored, as the report prints it. */
export interface Exploration {
  counterexamples: Map<string, string>;
  scenarios: number;
  orders: number;
  cut: number;
}

/** Which scenarios a thread explores: `index` is 0-based and below `jobs`. */
export interface Shard {
  index: number;
  jobs: number;
}

/**
 * Explores the scenarios of `shard` in order, reporting each result, and
 * stops after its own first break or once `isCut` says a lower scenario
 * broke in another shard.
 */
export async function exploreShard(
  total: number,
  shard: Shard,
  explore: (index: number) => Promise<ScenarioResult>,
  report: (result: ScenarioResult) => void,
  isCut: (index: number) => boolean,
): Promise<void> {
  for (let index = shard.index; index < total && !isCut(index); index += shard.jobs) {
    const result = await explore(index);
    report(result);
    if (result.broken) {
      return;
    }
  }
}

/**
 * Starts one worker per shard on `file`, which must explore its shard and
 * post each `ScenarioResult`, and tells every worker the lowest scenario
 * that broke a rule so the others stop early. `argv` is appended to each
 * worker's arguments after `--shard <n>`, 1-based.
 */
export function runShards(file: string, argv: string[], jobs: number): Promise<ScenarioResult[]> {
  return new Promise((resolve, reject) => {
    const results: ScenarioResult[] = [];
    let cut = Infinity;
    let running = jobs;
    const workers = Array.from(
      { length: jobs },
      (_, index) => new Worker(file, { argv: [...argv, "--shard", String(index + 1)] }),
    );
    const fail = (error: Error): void => {
      for (const worker of workers) {
        void worker.terminate();
      }
      reject(error);
    };
    for (const worker of workers) {
      worker.on("message", (result: ScenarioResult) => {
        results.push(result);
        if (result.broken && result.index < cut) {
          cut = result.index;
          for (const other of workers) {
            other.postMessage(cut);
          }
        }
      });
      worker.on("error", fail);
      worker.on("exit", (code) => {
        if (code !== 0) {
          fail(new Error(`a shard exited with code ${code}`));
          return;
        }
        running -= 1;
        if (running === 0) {
          resolve(results);
        }
      });
    }
  });
}

/**
 * What exploring the scenarios in index order would have found: for each
 * rule the counterexample of the smallest trace length, the lowest scenario
 * on a tie, cut after the first scenario that broke a rule.
 */
export function mergeResults(results: ScenarioResult[], total: number): Exploration {
  const exploration: Exploration = {
    counterexamples: new Map(),
    scenarios: 0,
    orders: 0,
    cut: 0,
  };
  const shortest = new Map<string, number>();
  const ordered = [...results].sort((left, right) => left.index - right.index);
  for (const result of ordered) {
    if (result.index !== exploration.scenarios) {
      break;
    }
    for (const [rule, { length, text }] of result.counterexamples) {
      if (length < (shortest.get(rule) ?? Infinity)) {
        shortest.set(rule, length);
        exploration.counterexamples.set(rule, text);
      }
    }
    exploration.orders += result.orders;
    exploration.cut += result.exhausted ? 0 : 1;
    exploration.scenarios += 1;
    if (result.broken) {
      return exploration;
    }
  }
  if (exploration.scenarios !== total) {
    throw new Error(`scenario ${exploration.scenarios} is missing from the shards`);
  }
  return exploration;
}
