import type {
  ColumnDescription,
  RelationDescription,
} from "@antelopejs/interface-database-catalog";

import { GetDatabase } from "../../connection";
import type { CollectionKind, KeyClass } from "../database/external-descriptor";
import { KindOf } from "../database/external-probe";
import {
  DescribeRelation,
  KeyFinding,
  type ListedRelation,
  ReadRelations,
} from "./discovery";

/** A relation a catalog call names, found and described. */
export interface Located {
  name: string;
  kind: CollectionKind;
  /** The key type class, when both ends of the `_id` index share one. */
  key?: KeyClass;
  listed: ListedRelation;
  description: RelationDescription;
}

/** Finds `"database.collection"` in the configured database, which is the only schema it has. */
export async function Locate(relation: string): Promise<Located> {
  const db = await GetDatabase();
  const dot = relation.indexOf(".");
  const name = relation.slice(dot + 1);
  const [listed] =
    dot > 0 && relation.slice(0, dot) === db.databaseName
      ? await ReadRelations([name])
      : [];
  if (!listed) {
    throw new Error(
      `Relation '${relation}' does not exist in database ${db.databaseName}, or the connected account cannot list it`,
    );
  }
  return {
    name,
    kind: KindOf(listed.info),
    key: (await KeyFinding(listed)).key,
    listed,
    description: await DescribeRelation(listed),
  };
}

/** The described columns a request names, every one when it names none; an unknown one is refused. */
export function SelectColumns(
  located: Located,
  names: string[] | undefined,
): ColumnDescription[] {
  const columns = located.description.columns;
  const unknown = (names ?? []).filter(
    (name) => !columns.some((column) => column.name === name),
  );
  if (unknown.length > 0) {
    throw new Error(
      `Relation '${located.name}' has no field ${unknown.join(", ")} in its validator or its sample`,
    );
  }
  return names
    ? names.flatMap((name) => columns.filter((column) => column.name === name))
    : columns;
}
