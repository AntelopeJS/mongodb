/**
 * Antelope Query Language (AQL) - A type-safe query builder for RethinkDB
 *
 * This file defines the TypeScript interfaces for AQL, which provides a fluent,
 * type-safe API for building database queries. AQL follows a similar structure
 * to ReQL (RethinkDB's query language) but with enhanced TypeScript support.
 *
 * Key concepts:
 * - ValueProxy: Records operations performed on temporary objects for callbacks
 * - Datum/Stream/Feed/Selection: Different query result types
 * - Tables/Databases: Structure for organizing data
 */

export type DeepPartial<T> = {
  [K in keyof T]?: T[K] extends Array<infer U1>
    ? Array<DeepPartial<U1>>
    : T[K] extends ReadonlyArray<infer U2>
      ? ReadonlyArray<DeepPartial<U2>>
      : DeepPartial<T[K]>;
};

/**
 * MultiFieldSelector is used for selecting multiple fields from documents
 * It can be used in operations like pluck, hasFields, withFields, etc.
 */
export declare type MultiFieldSelector =
  | Datum<MultiFieldSelector>
  | {
      [property: string]: MultiFieldSelector;
    }
  | string
  | string[]
  | boolean
  | number;

/**
 * Helper type that maps an array of object keys to an object containing those values
 * Used for group operations that work with multiple indices
 */
type MapKeyArrayToObjectValues<Ob, Ar extends Array<keyof Ob>> = { [K in keyof Ar]: Ob[Ar[K]] };

/**
 * Value Proxies are temporary objects that record operations done to them for callbacks.
 * They enable the fluent, chainable API that makes AQL queries readable and type-safe.
 */
export namespace ValueProxy {
  /**
   * Boolean proxy with equality operations
   */
  type BooleanProxy = Boolean & Eq;

  /**
   * Number proxy with arithmetic, comparison and equality operations
   */
  type NumberProxy = Arithmetic & Comparison<number> & Eq;

  /**
   * Date proxy with date-specific arithmetic, comparison and equality operations
   */
  type DateProxy = DateArithmetic & Comparison<Date> & Eq;

  /**
   * String proxy with string-specific operations, comparison and equality
   */
  type StringProxy = String & Comparison<string> & Eq;

  /**
   * Array proxy with array-specific operations
   */
  type ArrayProxy<T> = Arrays<T>; // & { readonly [index: number]: Proxy<T> };

  /**
   * Object proxy with object-specific operations
   */
  type ObjectProxy<T> = Objects<T>; // & { [K in keyof T]: Proxy<T[K]> };

  /**
   * Generic Value Proxy for type T.
   * Provides a type-appropriate interface based on the underlying data type,
   * enabling chained operations that preserve type information.
   */
  export type Proxy<T> = T extends boolean
    ? BooleanProxy
    : T extends number
      ? NumberProxy
      : T extends Date
        ? DateProxy
        : T extends string
          ? StringProxy
          : T extends ReadonlyArray<infer A> | Array<infer A>
            ? ArrayProxy<A>
            : T extends Record<any, any>
              ? ObjectProxy<T>
              : unknown extends T
                ? BooleanProxy | NumberProxy | DateProxy | StringProxy | ArrayProxy<any> | ObjectProxy<any>
                : never;

  /**
   * Value Proxy or straight type - represents either a direct value or its proxy wrapper.
   * This flexibility allows functions to accept either raw values or query expressions.
   */
  export type ProxyOrVal<T = any> = T | Proxy<T>;

  /**
   * Database types representing single items
   */
  type DBType<A> = Datum<A> | SingleSelection<A>;

  /**
   * Database types representing multiple items
   */
  type DBTypeMulti<A> = Stream<A> | Feed<A> | Selection<A> | Table<A>;

  /**
   * Helper type to determine if a type contains a proxy at some depth
   */
  type HasProxy<T, Depth extends any[]> = T extends
    | BooleanProxy
    | NumberProxy
    | DateProxy
    | StringProxy
    | ArrayProxy<any>
    | ObjectProxy<any>
    ? true
    : T extends DBTypeMulti<any> | DBType<any>
      ? true
      : T extends Record<any, any>
        ? Depth extends [any, ...infer SubDepth]
          ? HasProxy<T[keyof T], SubDepth>
          : never
        : never;

  /**
   * Extract a straight type out of a Value Proxy.
   * This recursively unwraps the proxy types to reveal the underlying data types.
   * Essential for inferring result types from query operations.
   */
  export type ExtractType<T> =
    T extends DBTypeMulti<infer A>
      ? ExtractType<A>[]
      : T extends DBType<infer A>
        ? ExtractType<A>
        : T extends BooleanProxy
          ? boolean
          : T extends NumberProxy
            ? number
            : T extends DateProxy
              ? Date
              : T extends StringProxy
                ? string
                : T extends ArrayProxy<infer A>
                  ? Array<ExtractType<A>>
                  : T extends ObjectProxy<infer A>
                    ? ExtractType<A>
                    : T extends {}
                      ? true extends HasProxy<T, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]>
                        ? {
                            [K in keyof T]: ExtractType<T[K]>;
                          }
                        : T
                      : T;

  /**
   * Boolean proxy interface defining logical operations on boolean values
   */
  interface Boolean {
    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<boolean | U>;

    /**
     * AND operator.
     *
     * @param other Operand B
     * @returns A && B
     */
    and(other: any): Proxy<boolean>;

    /**
     * OR operator.
     *
     * @param other Operand B
     * @returns A || B
     */
    or(other: any): Proxy<boolean>;

    /**
     * NOT operator.
     *
     * @returns !A
     */
    not(): Proxy<boolean>;
  }

  /**
   * DateArithmetic proxy interface for operations on Date objects
   * Supports date manipulation, extraction of components, and timezone operations
   */
  interface DateArithmetic {
    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<Date | U>;

    /**
     * Addition operator.
     * Adds a number of seconds to the date.
     *
     * @param other Operand B
     * @returns New date
     */
    add(other: ProxyOrVal<number>): Proxy<Date>;

    /**
     * Subtraction operator.
     * Removes a number of seconds from the date.
     *
     * @param other Operand B
     * @returns New date
     */
    sub(other: ProxyOrVal<number>): Proxy<Date>;

    /**
     * Difference operator.
     * Gives the difference between two dates in seconds.
     *
     * @param other Operand B
     * @returns Difference in seconds
     */
    sub(other: ProxyOrVal<Date>): Proxy<number>;

    /**
     * Determines whether or not the proxy is between the two bounds.
     *
     * @param left Left bound (inclusive)
     * @param right Right bound (exclusive)
     * @returns True if the date is within the bounds
     */
    during(left: ProxyOrVal<Date>, right: ProxyOrVal<Date>): Proxy<boolean>;

    /**
     * Changes the timezone of the date to the specified offset.
     *
     * Note: may not perform validation on the offset string.
     *
     * @param timezone UTC Offset
     * @returns New date
     */
    inTimezone(timezone: string): Proxy<Date>;

    /**
     * Gets the timezone offset of the date.
     *
     * @returns Timezone offset
     */
    timezone(): Proxy<string>;

    /**
     * Number of seconds since the start of the day.
     *
     * @returns Seconds
     */
    timeOfDay(): Proxy<number>;

    /**
     * Year.
     *
     * @returns Year
     */
    year(): Proxy<number>;

    /**
     * Month.
     *
     * @returns Month
     */
    month(): Proxy<number>;

    /**
     * Day of the month.
     *
     * @returns Day of the month
     */
    day(): Proxy<number>;

    /**
     * Day of the week.
     *
     * @returns Day of the week
     */
    dayOfWeek(): Proxy<number>;

    /**
     * Day of the year.
     *
     * @returns Day of the year
     */
    dayOfYear(): Proxy<number>;

    /**
     * Hour of the day.
     *
     * @returns Hours
     */
    hours(): Proxy<number>;

    /**
     * Minutes.
     *
     * @returns Minutes
     */
    minutes(): Proxy<number>;

    /**
     * Seconds.
     *
     * @returns Seconds
     */
    seconds(): Proxy<number>;

    /**
     * Seconds since the UNIX epoch with millisecond precision.
     *
     * @returns Seconds
     */
    toEpochTime(): Proxy<number>;
  }

  /**
   * Arithmetic proxy interface for numeric operations
   * Provides mathematical operations, bitwise operations, and rounding functions
   */
  interface Arithmetic {
    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<number | U>;

    /**
     * Addition operator.
     *
     * @param other Operand B
     * @returns A + B
     */
    add(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Subtraction operator.
     *
     * @param other Operand B
     * @returns A - B
     */
    sub(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Multiplication operator.
     *
     * @param other Operand B
     * @returns A * B
     */
    mul(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Division operator.
     *
     * @param other Operand B
     * @returns A / B
     */
    div(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Modulo operator.
     *
     * @param other Operand B
     * @returns A % B
     */
    mod(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Bitwise AND operator.
     *
     * @param other Operand B
     * @return A & B
     */
    bitAnd(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Bitwise OR operator.
     *
     * @param other Operand B
     * @return A | B
     */
    bitOr(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Bitwise XOR operator.
     *
     * @param other Operand B
     * @return A ^ B
     */
    bitXor(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Bitwise NOT operator.
     *
     * @return ~A
     */
    bitNot(): Proxy<number>;

    /**
     * Bitwise left shift operator.
     *
     * @param other Operand B
     * @returns A << B
     */
    bitLShift(other: ProxyOrVal<number>): Proxy<number>;

    /**
     * Bitwise right shift operator.
     *
     * @param other Operand B
     * @param preserveSign Preserve sign bit
     * @returns A >> B
     */
    bitRShift(other: ProxyOrVal<number>, preserveSign?: boolean): Proxy<number>;

    /**
     * Round to the nearest integer.
     *
     * @returns Integer
     */
    round(): Proxy<number>;

    /**
     * Round to the higher integer.
     *
     * @returns Integer
     */
    ceil(): Proxy<number>;

    /**
     * Round to the lower integer.
     *
     * @returns Integer
     */
    floor(): Proxy<number>;
  }

  /**
   * Equality comparison interface
   * Provides equality and inequality comparison operators
   */
  interface Eq {
    /**
     * Equality operator.
     *
     * @param other Operand B
     * @returns A == B
     */
    eq(other: any): Proxy<boolean>;

    /**
     * Inequality operator.
     *
     * @param other Operand B
     * @returns A != B
     */
    ne(other: any): Proxy<boolean>;
  }

  /**
   * Generic comparison interface for ordered types
   * Provides relational operators (greater than, less than, etc.)
   *
   * @typeParam T The type being compared
   */
  interface Comparison<T> {
    /**
     * Greater-than operator.
     *
     * @param other Operand B
     * @returns A > B
     */
    gt(other: ProxyOrVal<T>): Proxy<boolean>;

    /**
     * Greater-or-equal operator.
     *
     * @param other Operand B
     * @returns A >= B
     */
    ge(other: ProxyOrVal<T>): Proxy<boolean>;

    /**
     * Lesser-than operator.
     *
     * @param other Operand B
     * @returns A < B
     */
    lt(other: ProxyOrVal<T>): Proxy<boolean>;

    /**
     * Lesser-or-equal operator.
     *
     * @param other Operand B
     * @returns A <= B
     */
    le(other: ProxyOrVal<T>): Proxy<boolean>;
  }

  /**
   * String proxy interface for string operations
   * Provides string manipulation methods like split, case conversion, and concatenation
   */
  interface String {
    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<string | U>;

    /**
     * Splits the string using a separator.
     *
     * @param separator Separator string
     * @param maxSplits Maximum number of results
     * @returns Array of sub-strings
     */
    split(separator?: ProxyOrVal<string>, maxSplits?: ProxyOrVal<number>): Proxy<string[]>;

    /**
     * Converts the string to all upper case.
     *
     * @returns New string
     */
    upcase(): Proxy<string>;

    /**
     * Converts the string to all lower case.
     *
     * @returns New string
     */
    downcase(): Proxy<string>;

    /**
     * Gets the number of Unicode codepoints in the string.
     *
     * @returns Number of codepoints
     */
    count(): Proxy<number>;

    /**
     * Concatenate the string with another.
     *
     * @param other Second string
     * @returns Concatenated string
     */
    add(other: ProxyOrVal<string>): Proxy<string>;

    /**
     * Checks if the string matches a regex.
     *
     * @param regex Regex
     * @returns True if the string matched
     */
    match(regex: ProxyOrVal<string>): Proxy<boolean>;
  }

  /**
   * Array proxy interface for array operations
   * Provides array methods like map, filter, slice, and aggregation operations
   *
   * @typeParam T The type of elements in the array
   */
  interface Arrays<T> {
    /**
     * Indexes the array.
     *
     * @param k Index
     * @returns Value at index k
     */
    (k: ProxyOrVal<number>): Proxy<T>;

    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<Array<T> | U>;

    /**
     * Tests if the arrays contains a value.
     *
     * @param val Value to search for.
     * @returns True if the value was found.
     */
    includes(val: ProxyOrVal): Proxy<boolean>;

    /**
     * Returns a slice of the array.
     *
     * @param start Start index (inclusive, 0-indexed)
     * @param end End index (exclusive)
     * @returns Sub-array
     */
    slice(start: ProxyOrVal<number>, end?: ProxyOrVal<number>): Proxy<Array<T>>;

    /**
     * Maps the array values using a mapping function.
     *
     * @param mapper Mapping function
     * @returns New array
     */
    map<U>(mapper: (val: Proxy<T>) => U): Proxy<Array<ExtractType<U>>>;

    /**
     * Filters the array using a predicate function.
     *
     * @param predicate Predicate function.
     * @returns Filtered array
     */
    filter(predicate: (val: Proxy<T>) => ProxyOrVal<boolean>): Proxy<Array<T>>;

    /**
     * Filters the array using a field list.
     *
     * @param fields Multifield selector
     */
    hasFields(...fields: MultiFieldSelector[]): Proxy<Array<T>>;

    /**
     * Checks if the array is empty.
     *
     * @returns True if the array is empty
     */
    isEmpty(): Proxy<boolean>;

    /**
     * Gets the length of the array.
     *
     * @returns Length
     */
    count(): Proxy<number>;

    /**
     * Gets the sum of a number array.
     *
     * @returns Sum
     */
    sum(): T extends number ? Proxy<number> : never;

    /**
     * Gets the average of a number array.
     *
     * @returns Average
     */
    avg(): T extends number ? Proxy<number> : never;

    /**
     * Gets the minimum of a number array.
     *
     * @returns Minimum
     */
    min(): T extends number ? Proxy<number> : never;

    /**
     * Gets the maxmimum of a number array.
     *
     * @returns Maximum
     */
    max(): T extends number ? Proxy<number> : never;
  }

  /**
   * Object proxy interface for object operations
   * Provides object methods like merge, keys, values, and field testing
   *
   * @typeParam T The object type
   */
  interface Objects<T> {
    /**
     * Indexes the object.
     *
     * @param k Index
     * @returns Value at index k
     */
    <K extends keyof T>(k: ProxyOrVal<K>): Proxy<T[K]>;

    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ProxyOrVal<U>): Proxy<T | U>;

    /**
     * Merges the object with another.
     *
     * @param value Other object
     * @returns `{...A, ...B}`
     */
    merge<U>(value: ProxyOrVal<U>): Proxy<Omit<T, keyof U> & U>;

    /**
     * Gets the keys of the object as an array.
     *
     * @returns Array of keys
     */
    keys(): Proxy<Array<keyof T>>;

    /**
     * Gets the values of the object as an array.
     *
     * @returns Array of values
     */
    values(): Proxy<Array<T[keyof T]>>;

    /**
     * Tests if the object has the specified fields.
     *
     * @param fields Multifield selector
     * @returns True if the object matches
     */
    hasFields(...fields: MultiFieldSelector[]): Proxy<boolean>;
  }
}

/**
 * Result namespace containing types for query operation results
 */
export namespace Result {
  /**
   * Write operation results.
   */
  export interface Write<_T = any> {
    /**
     * Number of documents that were deleted.
     */
    deleted?: number;

    /**
     * Number of errors.
     */
    errors?: number;

    /**
     * Error.
     */
    first_error?: string;

    /**
     * Number of documents that were inserted.
     */
    inserted?: number;

    /**
     * Number of documents that were modified.
     */
    replaced?: number;

    /**
     * Generated primary keys.
     */
    generated_keys?: string[];

    /**
     * Warnings.
     */
    warnings?: string[];
  }

  /**
   * Index operation results.
   */
  export interface IndexChange {
    /**
     * Number of indexes that were created.
     */
    created?: number;

    /**
     * Number of indexes that were renamed.
     */
    renamed?: number;

    /**
     * Number of indexes that were removed.
     */
    dropped?: number;
  }

  /**
   * Table operation results.
   */
  export interface TableChange {
    /**
     * Number of tables that were created.
     */
    tables_created?: number;

    /**
     * Number of tables that were removed.
     */
    tables_dropped?: number;
  }

  /**
   * Database operation results.
   */
  export interface DatabaseChange {
    /**
     * Number of tables that were removed.
     */
    tables_dropped: number;

    /**
     * Number of databases that were created.
     */
    dbs_created: number;

    /**
     * Number of databases that were removed.
     */
    dbs_dropped: number;
  }
}

/**
 * Options namespace containing configuration options for various operations
 */
export namespace Options {
  /**
   * Table creation options.
   */
  export interface TableCreate {
    /**
     * Primary key. Defaults to `id`.
     */
    primary?: string; // Defaults to 'id'
  }

  /**
   * Update operation options.
   */
  export interface Update {
    /**
     * Return a ValueChange for the updated documents.
     */
    returnChanges?: boolean;
  }

  /**
   * Insert operation options.
   */
  export interface Insert extends Update {
    /**
     * Operation to perform on primary key conflict.
     * - `error`: Throw an error. (default)
     * - `replace`: Replace the documents.
     * - `update`: Update the documents (merge contents).
     */
    conflict?: 'error' | 'replace' | 'update';
  }

  /**
   * Changefeed options interface
   * Defines configuration options for change feeds
   */
  export interface Changes {
    /**
     * Squash changes if possible to reduce network traffic.
     */
    squash?: boolean | number;

    /**
     * Number of changes to buffer without a read operation on the client before erroring.
     */
    changefeedQueueSize?: number;

    /**
     * Send an initial ValueChange with the current value.
     */
    includeInitial?: boolean;
  }
}

/**
 * ValueChange interface for representing document changes in change feeds
 * Contains before and after states of documents
 *
 * @typeParam T The document type
 */
export interface ValueChange<T = any> {
  /**
   * Error.
   */
  error?: string;

  /**
   * Value prior to the change event.
   */
  old_val?: T;

  /**
   * New value.
   */
  new_val?: T;
}

/**
 * Represents a value that can be either a raw value or a Datum containing the value
 *
 * @typeParam T The type of the value
 */
export declare type Value<T = any> = Datum<T> | T;

/**
 * Join type enumeration defining different types of joins between collections
 * Similar to SQL join types (INNER JOIN, LEFT JOIN, etc.)
 */
export enum JoinType {
  /**
   * Cross Join.
   * Returns the Cartesian product of both collections.
   */
  Cross = 0,

  /**
   * Exclusive Left Join.
   * Returns all records from the left collection where there is NO match in the right collection.
   */
  LeftExcl = 1,

  /**
   * Inner Join.
   * Returns records that have matching values in both collections.
   */
  Inner = 2,

  /**
   * Left Inner Join.
   * Returns all records from the left collection, and the matched records from the right collection.
   */
  Left = 3,

  /**
   * Exclusive Right Join.
   * Returns all records from the right collection where there is NO match in the left collection.
   */
  RightExcl = 4,

  /**
   * Exclusive Full Join.
   * Returns records from both collections where there is NO match in the other collection.
   */
  FullExcl = 5,

  /**
   * Right Inner Join.
   * Returns all records from the right collection, and the matched records from the left collection.
   */
  Right = 6,

  /**
   * Full Outer Join.
   * Returns all records when there is a match in either left or right collection.
   */
  FullOuter = 7,
}

/**
 * Base query interface representing any database query
 * Provides methods to execute queries and access results
 *
 * @typeParam T The result type of the query
 */
export interface Query<T = any> {
  /**
   * Run the query.
   *
   * @returns The query result
   */
  run(): Promise<T>;

  /**
   * Promise-like then() method for asynchronous functions.
   */
  then<T2>(next: (val: T) => T2): Promise<Awaited<T2>>;

  /**
   * Asynchronous generator.
   */
  iterator(): AsyncGenerator<T extends Array<infer T1> ? T1 : T, void, unknown>;

  /**
   * Asynchronous generator.
   */
  [Symbol.asyncIterator](): AsyncGenerator<T extends Array<infer T1> ? T1 : T, void, unknown>;
}

/**
 * Datum interface representing a single value in a query
 * Provides operations that work on a single value
 *
 * @typeParam T The type of the value
 */
export interface Datum<T = any> extends Query<T> {
  /**
   * Run a mapping function on the datum.
   *
   * @param mapper Mapping function
   * @returns New datum with the result of the mapper
   */
  do<U>(mapper: (obj: ValueProxy.Proxy<T>) => U): Datum<ValueProxy.ExtractType<U>>;

  /**
   * Indexes the datum.
   *
   * @param attr Index
   * @returns New datum with the value
   */
  <U extends keyof NonNullable<T>>(attr: U): Datum<NonNullable<T>[U]>;

  /**
   * Defaults the datum to a given value if it is null.
   *
   * @param value Value to use
   * @returns Current datum or given value
   */
  default<U>(value: Value<U>): Datum<NonNullable<T> | U>;

  /**
   * Perform a foreign key lookup
   * 
   * @param other Other table
   * @param localKey Key in local object
   * @param otherKey Key in other table
   */
  lookup<U = any, TK extends keyof T = keyof T>(
    other: Stream<U>,
    localKey: TK,
    otherKey: keyof U,
  ): Datum<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>;

  /**
   * For array datums, appends the given value.
   *
   * @param value The value
   * @returns New datum
   */
  append<U>(value: Value<U>): T extends U[] ? Datum<T> : never;

  /**
   * For array datums, prepends the given value.
   *
   * @param value The value
   * @returns New datum
   */
  prepend<U>(value: Value<U>): T extends U[] ? Datum<T> : never;

  /**
   * Plucks fields from the documents in the stream.
   *
   * @param fields Fields to keep
   * @returns New stream
   */
  pluck(...fields: MultiFieldSelector[]): Datum<Partial<T>>;

  /**
   * Converts the Datum to a value proxy to use ValueProxy-specific methods.
   *
   * @returns New ValueProxy
   */
  value(): ValueProxy.Proxy<T>;
}

/**
 * Stream interface representing a sequence of values
 * Provides operations for filtering, transforming, and aggregating sequences
 *
 * @typeParam T The type of elements in the stream
 */
export interface Stream<T = any> extends Query<T[]> {
  /**
   * Transforms the stream into a feed of ValueChange events.
   *
   * @param options Change options
   * @returns The new feed
   */
  changes(options?: Options.Changes): Feed<ValueChange<T>>;

  /**
   * Indexes the elements of the stream.
   *
   * @param attr Index
   * @returns New stream of values
   */
  <U extends keyof T>(attr: U): Stream<T[U]>;

  /**
   * Defaults the elements of the stream to a given value when they are null.
   *
   * @param value Value to use
   * @returns New stream
   */
  default<U>(value: Value<U>): Stream<NonNullable<T> | U>;

  /**
   * Performs a join operation.
   *
   * @param right Other stream
   * @param type Join type {@link JoinType}
   * @param mapper Mapping function
   * @param predicate Predicate function
   * @returns New stream
   */
  join<U = any, V = any>(
    right: Stream<U> | Value<U[]>,
    type: JoinType,
    mapper: (left: ValueProxy.Proxy<T | null>, right: ValueProxy.Proxy<U | null>) => V,
    predicate: (left: ValueProxy.Proxy<T>, right: ValueProxy.Proxy<U>) => ValueProxy.ProxyOrVal<boolean>,
  ): Stream<ValueProxy.ExtractType<V>>;
  join<U = any, V = any>(
    right: Stream<U> | Value<U[]>,
    type: JoinType.Cross,
    mapper: (left: ValueProxy.Proxy<T | null>, right: ValueProxy.Proxy<U | null>) => V,
  ): Stream<ValueProxy.ExtractType<V>>;

  /**
   * Perform a foreign key lookup
   * 
   * @param other Other table
   * @param localKey Key in local object
   * @param otherKey Key in other table
   */
  lookup<U = any, TK extends keyof T = keyof T>(
    other: Stream<U>,
    localKey: TK,
    otherKey: keyof U,
  ): Stream<Omit<T, TK> & Record<TK, T[TK] extends any[] ? U[] : U>>;

  /**
   * Performs a union of two streams.
   *
   * @param other Other stream
   * @returns New stream
   */
  union<U = T>(other: Stream<U> | Value<U[]>): Stream<U | T>;
  union<U = T>(other: Feed<U>): Feed<U | T>;

  /**
   * Maps the elements of the stream using a mapping function.
   *
   * @param mapper Mapping function
   * @returns New stream
   */
  map<U>(mapper: (obj: ValueProxy.Proxy<T>) => U): Stream<ValueProxy.ExtractType<U>>;

  /**
   * Combination of a hasFields operation followed by a pluck operation.
   *
   * @param fields Multifield selector
   * @returns New stream
   */
  withFields(...fields: MultiFieldSelector[]): Stream<Partial<T>>;

  /**
   * Filters the stream using a field list.
   *
   * @param fields Multifield selector
   * @returns New stream
   */
  hasFields(...fields: MultiFieldSelector[]): this;

  /**
   * Filters the stream using a predicate.
   *
   * @param predicate Predicate
   * @returns New stream
   */
  filter(predicate: DeepPartial<T> | ((doc: ValueProxy.Proxy<T>) => ValueProxy.ProxyOrVal<boolean>)): this;

  /**
   * Sorts the stream on the given field.
   *
   * @param field Field to use for sorting
   * @param direction Sort direction (asc, desc)
   * @param noIndex Ignore indexes
   * @returns Sorted stream
   */
  orderBy(field: keyof NonNullable<T>, direction?: 'asc' | 'desc', noIndex?: boolean): this;

  /**
   * Groups the documents using the given index and maps the result using a mapping function.
   *
   * The parameters of this function are:
   * - The stream with all the elements inside one group
   * - The index value for this group
   *
   * The result of this function is used as the element in the new stream
   *
   * @param index Index to group on
   * @param mapper Mapping function
   * @returns Stream of grouped data
   */
  group<K extends keyof T, U>(
    index: K,
    mapper: (stream: Stream<T>, group: ValueProxy.Proxy<T[K]>) => U,
  ): Stream<ValueProxy.ExtractType<U>>;
  group<K extends Array<keyof T>, U>(
    index: K,
    mapper: (stream: Stream<T>, group: ValueProxy.Proxy<MapKeyArrayToObjectValues<T, K>>) => U,
  ): Stream<ValueProxy.ExtractType<U>>;

  /**
   * Gets the count of documents or the count of distinct values of a given field.
   *
   * @param field Field to count distinct entries
   * @returns Datum of the result
   */
  count(field?: keyof T): Datum<number>;

  /**
   * Sum of the values on the given field.
   *
   * @param field Field to use
   * @returns Datum of the result
   */
  sum(field?: keyof T): Datum<number>;

  /**
   * Average of the values on the given field.
   *
   * @param field Field to use
   * @returns Datum of the result
   */
  avg(field?: keyof T): Datum<number>;

  /**
   * Minimum of the values on the given field.
   *
   * @param field Field to use
   * @returns Datum of the result
   */
  min(field?: keyof T): Datum<T>;

  /**
   * Maximum of the values on the given field.
   *
   * @param field Field to use
   * @returns Datum of the result
   */
  max(field?: keyof T): Datum<T>;

  /**
   * Gets an array of distinct documents in the stream.
   *
   * @returns Datum of the array
   */
  distinct(): Datum<Array<T>>;

  /**
   * Gets a stream of the distinct values of a field.
   *
   * @param index Field to use
   * @returns New stream
   */
  distinct<K extends keyof T = keyof T>(index: K): Stream<T[K]>;

  /**
   * Plucks fields from the documents in the stream.
   *
   * @param fields Fields to keep
   * @returns New stream
   */
  pluck(...fields: MultiFieldSelector[]): Stream<Partial<T>>;

  /**
   * Removes fields from the documents in the stream.
   *
   * @param fields Fields to remove
   * @returns New stream
   */
  without(...fields: MultiFieldSelector[]): Stream<Partial<T>>;

  /**
   * Returns a slice of the documents in the stream.
   *
   * @param offset Starting offset
   * @param count Count
   * @returns New stream
   */
  slice(offset: Value<number>, count?: Value<number>): this;

  /**
   * Returns a datum of the Nth document in the stream.
   *
   * @param n number
   * @returns New datum
   */
  nth(n: Value<number>): Datum<T>;
}

/**
 * Feed interface representing a real-time stream of changes
 * Similar to Stream but optimized for change notifications
 *
 * @typeParam T The type of elements in the feed
 */
export interface Feed<T = any> extends Query<T[]> {
  /**
   * Indexes the elements of the feed.
   *
   * @param attr Index
   * @returns New feed of values
   */
  <U extends keyof T>(attr: U): Feed<T[U]>;

  /**
   * Maps the elements of the feed using a mapping function.
   *
   * @param mapper Mapping function
   * @returns New feed
   */
  map<U>(mapper: (obj: ValueProxy.Proxy<T>) => U): Feed<ValueProxy.ExtractType<U>>;

  /**
   * Combination of a hasFields operation followed by a pluck operation.
   *
   * @param fields Multifield selector
   * @returns New feed
   */
  withFields(...fields: MultiFieldSelector[]): Feed<Partial<T>>;

  /**
   * Filters the feed using a field list.
   *
   * @param fields Multifield selector
   * @returns New feed
   */
  hasFields(...fields: MultiFieldSelector[]): this;

  /**
   * Filters the feed using a predicate.
   *
   * @param predicate Predicate
   * @returns New feed
   */
  filter(predicate: DeepPartial<T> | ((doc: ValueProxy.Proxy<T>) => ValueProxy.ProxyOrVal<boolean>)): this;

  /**
   * Plucks fields from the documents in the feed.
   *
   * @param fields Fields to keep
   * @returns New feed
   */
  pluck(...fields: MultiFieldSelector[]): Feed<Partial<T>>;

  /**
   * Removes fields from the documents in the feed.
   *
   * @param fields Fields to remove
   * @returns New feed
   */
  without(...fields: MultiFieldSelector[]): Feed<Partial<T>>;
}

/**
 * SingleSelection interface representing a query that selects a single document
 * Provides operations for reading and modifying a single document
 *
 * @typeParam T The document type
 */
export interface SingleSelection<T = any> extends Datum<T> {
  /**
   * Updates the document of the selection using the given object.
   *
   * Unlike replace, update operations keep values unaffected if they are not in the new object.
   *
   * @param obj New object
   * @param options Update options
   * @returns Write result
   */
  update(
    obj: Value<DeepPartial<T>> | ((arg: ValueProxy.Proxy<T>) => any),
    options?: Options.Update,
  ): Query<Result.Write<T>>;

  /**
   * Replaces the document of the selection by the given object.
   *
   * @param obj New object
   * @param options Replace options
   * @returns Write result
   */
  replace(obj: Value<T> | ((arg: ValueProxy.Proxy<T>) => any), options?: Options.Update): Query<Result.Write<T>>;

  /**
   * Delete the document.
   *
   * @param options Delete options
   * @returns Write result
   */
  delete(options?: Options.Update): Query<Result.Write<T>>;

  /**
   * Gets a change feed of this selection.
   *
   * @param options Change options
   * @returns New feed
   */
  changes(options?: Options.Changes): Feed<ValueChange<T>>;
}

/**
 * Selection interface representing a query that selects multiple documents
 * Provides operations for reading and modifying multiple documents
 *
 * @typeParam T The document type
 */
export interface Selection<T = any> extends Stream<T> {
  /**
   * Updates the documents of the selection using the given object.
   *
   * Unlike replace, update operations keep values unaffected if they are not in the new object.
   *
   * @param obj New object
   * @param options Update options
   * @returns Write result
   */
  update(
    obj: Value<DeepPartial<T>> | ((arg: ValueProxy.Proxy<T>) => any),
    options?: Options.Update,
  ): Query<Result.Write<T>>;

  /**
   * Replaces the documents of the selection by the given object.
   *
   * @param obj New object
   * @param options Replace options
   * @returns Write result
   */
  replace(obj: Value<T> | ((arg: ValueProxy.Proxy<T>) => any), options?: Options.Update): Query<Result.Write<T>>;

  /**
   * Delete the documents.
   *
   * @param options Delete options
   * @returns Write result
   */
  delete(options?: Options.Update): Query<Result.Write<T>>;

  /**
   * Returns a selection of the Nth document.
   *
   * @param n number
   * @returns New selection
   */
  nth(n: Value<number>): SingleSelection<T>;
}

/**
 * Table interface representing a database table
 * Provides operations for querying, modifying, and indexing documents
 *
 * @typeParam T The document type stored in the table
 */
export interface Table<T = any> extends Selection<T> {
  /**
   * Creates a new index on this table
   *
   * @param indexName Index name
   * @param keys Keys to use for the index
   * @returns Index change result
   */
  indexCreate(indexName: string, ...keys: string[]): Query<Result.IndexChange>;

  /**
   * Removes an index from this table.
   *
   * @param indexName Index name
   * @returns Index change result
   */
  indexDrop(indexName: string): Query<Result.IndexChange>;

  /**
   * Gets the list of indexes on this table.
   *
   * @returns List of index names
   */
  indexList(): Query<string[]>;

  /**
   * Insert a new document in this table
   *
   * @param obj New object(s)
   * @param options Insert options
   * @returns Write result
   */
  insert(obj: DeepPartial<T> | Array<DeepPartial<T>>, options?: Options.Insert): Query<Result.Write<T>>;

  /**
   * Gets the document identified by the given value.
   *
   * @param key Primary key value
   * @returns New selection
   */
  get(key: any): SingleSelection<T | null>;

  /**
   * Gets the documents identified by the given value on an index.
   *
   * @param index Index name
   * @param params Index values
   * @returns New selection
   */
  getAll(index: string, ...params: any[]): Selection<T>;

  /**
   * Gets the documents where the given index is between the given bounds.
   *
   * @param index Index name
   * @param low Lower bound (inclusive)
   * @param high Upper bound (exclusive)
   */
  between(index: string, low: any, high: any): Selection<T>;
}

/**
 * Database interface representing a database connection
 * Provides operations for managing tables within the database
 *
 * @typeParam T Type map of table names to document types
 */
export interface Database<T = any> {
  /**
   * Creates a new table with the given name.
   *
   * @param name Table name
   * @param options Table creation options
   * @returns Table change result
   */
  tableCreate(name: string, options?: Options.TableCreate): Query<Result.TableChange>;

  /**
   * Removes a table.
   *
   * @param name Table name
   * @returns Table change result
   */
  tableDrop(name: string): Query<Result.TableChange>;

  /**
   * Lists the tables in the database.
   *
   * @returns List of names
   */
  tableList(): Query<string[]>;

  /**
   * Gets the table with the given name.
   *
   * @param name Table name
   * @returns The table
   */
  table<U = unknown, K extends keyof T = keyof T>(name: K): Table<U extends {} ? U : T[K]>;
}

/**
 * Creates a connection to a database
 *
 * @param name The name of the database to connect to
 * @returns A Database interface for the specified database
 */
export declare function Database<T = any>(name: string): Database<T>;

/**
 * Creates a new database in the RethinkDB instance
 *
 * @param name The name of the database to create
 * @returns A Query that resolves to a DatabaseChange result
 */
export declare function CreateDatabase(name: string): Query<Result.DatabaseChange>;

/**
 * Deletes a database from the RethinkDB instance
 *
 * @param name The name of the database to delete
 * @returns A Query that resolves to a DatabaseChange result
 */
export declare function DeleteDatabase(name: string): Query<Result.DatabaseChange>;

/**
 * Lists all databases in the RethinkDB instance
 *
 * @returns A Query that resolves to an array of database names
 */
export declare function ListDatabases(): Query<string[]>;

/**
 * Creates a variable reference for use in complex query expressions
 * Particularly useful in join predicates and other multi-step operations
 *
 * @param name The name of the variable
 * @returns A reference to the variable that can be used in expressions
 */
export declare function Var(name: string): any;

/**
 * Creates a ValueProxy from a raw value
 * Allows using raw JavaScript values in AQL query expressions
 *
 * @param value The value to convert to a ValueProxy
 * @returns A ValueProxy representation of the value
 */
export declare function Expr<T>(value: T): ValueProxy.Proxy<T>;

Object.assign(exports, require('./runtime'));
