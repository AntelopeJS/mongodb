import { StagedObject } from './common';
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
export type FieldType = string | Array<FieldType> | {
    [subfield: string]: FieldType;
};
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
/**
 * A schema determines the structure of a database
 *
 * Each schema can have multiple instances
 *
 * The internal organization of these instances is left up to the module implementation
 */
export declare class Schema<T = any> extends StagedObject {
    readonly id: string;
    readonly definition: SchemaDefinition;
    private static readonly registry;
    /**
     * Retrieves a previously defined schema by its ID
     *
     * @param id Schema ID
     * @returns The schema instance, or undefined if not found
     */
    static get(id: string): Schema | undefined;
    /**
     * Default instance of this schema for convenience
     */
    readonly default: SchemaInstance<T>;
    /**
     * Define a new schema with the given ID
     *
     * @param id ID of this schema, changing this will leave previous data inaccessible
     * @param definition Schema definition (tables, fields, indexes..)
     */
    constructor(id: string, definition: SchemaDefinition);
    /**
     * Gets a specific instance of the schema
     *
     * @param id Instance ID
     * @returns Schema instance
     */
    instance(id: string): SchemaInstance<T>;
    /**
     * Creates a new instance of the schema
     *
     * @param id Instance ID
     * @returns Created instance ID
     */
    createInstance(id?: string): Query<string>;
    /**
     * Destroys an existing instance of the schema
     *
     * @param id Instance ID
     */
    destroyInstance(id: string): Query<void>;
}
/**
 * Schema instance, could be a database or a filtered portion of one depending on the implementation
 */
export declare class SchemaInstance<T> extends StagedObject {
    /**
     * Gets a table from the instance
     *
     * @param id Table name
     * @returns Table
     */
    table<TK extends keyof T>(id: TK): Table<T[TK]>;
}
