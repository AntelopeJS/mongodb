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

export interface SchemaOptions {
  rowLevel?: boolean;
}

//@internal
export const Schemas = new RegisteringProxy<(name: string, def: SchemaDefinition, options: SchemaOptions) => void>();

/**
 * A schema determines the structure of a database
 *
 * Each schema can have multiple instances
 *
 * The internal organization of these instances is left up to the module implementation
 */
export class Schema<T = any> extends StagedObject {
  private static readonly registry = new Map<string, Schema>();

  /**
   * Retrieves a previously defined schema by its ID
   *
   * @param id Schema ID
   * @returns The schema instance, or undefined if not found
   */
  public static get(id: string): Schema | undefined {
    return Schema.registry.get(id);
  }

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
    public readonly options: SchemaOptions = {},
  ) {
    super(QueryStage('schema', { id }));
    Schemas.register(id, definition, options);
    Schema.registry.set(id, this);
  }

  /**
   * Gets a specific instance of the schema
   *
   * @param id Instance ID
   * @returns Schema instance
   */
  public instance(id?: string) {
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

  /**
   * Destroys an existing instance of the schema
   *
   * @param id Instance ID
   */
  public destroyInstance(id?: string) {
    return this.stage(Query<void>, 'destroyInstance', { id });
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
