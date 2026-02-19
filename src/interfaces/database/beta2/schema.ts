import { RegisteringProxy } from '@ajs/core/beta';
import { QueryStage, StagedObject } from './common';
import { Query } from './query';
import { Table } from './selection';

/**
 * Secondary table index definition
 */
export interface IndexDefinition {
  /**
   * Fields to use for compound indexes
   */
  fields?: string[];

  /**
   * Whether or not this is a multi index
   */
  multi?: boolean;
}

export type FieldType = string | Array<FieldType> | { [subfield: string]: FieldType };

/**
 * Schema table definition
 */
export interface TableDefinition {
  /**
   * Field names and their data types
   */
  fields: Record<string, FieldType>;

  /**
   * List of secondary indexes
   */
  indexes: Record<string, IndexDefinition>;
}

/**
 * Table names and their definitions
 */
export interface SchemaDefinition {
  [tableName: string]: TableDefinition;
}

//@internal
export const Schemas = new RegisteringProxy<(name: string, def: SchemaDefinition) => void>();

/**
 * A schema determines the structure of a database
 *
 * Each schema can have multiple instances
 *
 * The internal organization of these instances is left up to the module implementation
 */
export class Schema<T = any> extends StagedObject {
  /**
   * Default instance of this schema for convenience
   */
  public readonly default = this.instance('default');

  /**
   * Define a new schema with the given ID
   *
   * @param id ID of this schema, changing this will leave previous data inaccessible
   * @param definition Schema definition (tables, fields, indexes..)
   */
  public constructor(
    public readonly id: string,
    public readonly definition: SchemaDefinition,
  ) {
    super(QueryStage('schema', { id }));
    Schemas.register(id, definition);
  }

  /**
   * Gets a specific instance of the schema
   *
   * @param id Instance ID
   * @returns Schema instance
   */
  public instance(id: string) {
    return this.stage(SchemaInstance<T>, 'instance', { id });
  }

  /**
   * Creates a new instance of the schema
   *
   * @param id Instance ID
   * @returns Created instance ID
   */
  public createInstance(id?: string) {
    return this.stage(Query<string>, 'createInstance', { id });
  }
}

/**
 * Schema instance, could be a database or a filtered portion of one depending on the implementation
 */
export class SchemaInstance<T> extends StagedObject {
  /**
   * Gets a table from the instance
   *
   * @param id Table name
   * @returns Table
   */
  public table<TK extends keyof T>(id: TK) {
    return this.stage(Table<T[TK]>, 'table', { id });
  }
}
