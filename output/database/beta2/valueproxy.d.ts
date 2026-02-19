import { ExtractType, StagedObject } from './common';
export type ValueProxyOrValue<T> = ValueProxy<T> | T;
/**
 * Proxy to an actual value in the database
 *
 * Actions performed on this proxy are not executed right away but instead recorded for the actual query
 */
export declare class ValueProxy<T> extends StagedObject {
    /**
     * Create a new proxy with a constant value
     *
     * @param value Value
     * @returns Proxy
     */
    static constant<T = unknown>(value: unknown): ValueProxy<T>;
    /**
     * Changes the type of this proxy.
     * This does not actually perform any conversion, it only changes the typescript type.
     *
     * @returns Same proxy with a different type
     */
    cast<U>(): ValueProxy<U>;
    /**
     * Returns the parameter if the proxy is null.
     *
     * @param value Value to use in case the proxy is null
     * @returns Non-null value.
     */
    default<U>(value: ValueProxyOrValue<U>): ValueProxy<U | Exclude<T, null | undefined>>;
    /**
     * AND operator.
     *
     * @param other Operand B
     * @returns A && B
     */
    and(value: unknown): ValueProxy<boolean>;
    /**
     * OR operator.
     *
     * @param other Operand B
     * @returns A || B
     */
    or(value: unknown): ValueProxy<boolean>;
    /**
     * NOT operator.
     *
     * @returns !A
     */
    not(): ValueProxy<boolean>;
    /**
     * Equality operator.
     *
     * @param other Operand B
     * @returns A == B
     */
    eq(value: unknown): ValueProxy<boolean>;
    /**
     * Inequality operator.
     *
     * @param other Operand B
     * @returns A != B
     */
    ne(value: unknown): ValueProxy<boolean>;
    /**
     * Addition operator.
     *
     * @param other Operand B
     * @returns New value
     */
    add(this: Is<Date | number, T, this>, value: ValueProxyOrValue<number>): ValueProxy<T>;
    /**
     * Subtraction operator.
     *
     * @param other Operand B
     * @returns New value
     */
    sub<U>(this: Is<Date | number, T, this>, value: ValueProxyOrValue<U>): ValueProxy<Date extends U ? (Date extends T ? number : never) | (number extends U ? T extends number ? number : Date : never) : Extract<number, T> | Extract<Date, T>>;
    /**
     * Determines whether or not the proxy is between the two bounds.
     *
     * @param left Left bound (inclusive)
     * @param right Right bound (exclusive)
     * @returns True if the date is within the bounds
     */
    during(this: Is<Date, T, this>, left: ValueProxyOrValue<Date>, right: ValueProxyOrValue<Date>): ValueProxy<boolean>;
    /**
     * Number of seconds since the start of the day.
     *
     * @returns Seconds
     */
    timeofday(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Year.
     *
     * @returns Year
     */
    year(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Month.
     *
     * @returns Month
     */
    month(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Day of the month.
     *
     * @returns Day of the month
     */
    day(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Day of the week.
     *
     * @returns Day of the week
     */
    dayofweek(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Day of the year.
     *
     * @returns Day of the year
     */
    dayofyear(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Hour of the day.
     *
     * @returns Hours
     */
    hours(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Minutes.
     *
     * @returns Minutes
     */
    minutes(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Seconds.
     *
     * @returns Seconds
     */
    seconds(this: Is<Date, T, this>, timezone?: ValueProxyOrValue<string>): ValueProxy<number>;
    /**
     * Seconds since the UNIX epoch with millisecond precision.
     *
     * @returns Seconds
     */
    epoch(this: Is<Date, T, this>): ValueProxy<number>;
    /**
     * Multiplication operator.
     *
     * @param other Operand B
     * @returns A * B
     */
    mul(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Division operator.
     *
     * @param other Operand B
     * @returns A / B
     */
    div(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Modulo operator.
     *
     * @param other Operand B
     * @returns A % B
     */
    mod(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Round to the nearest integer.
     *
     * @returns Integer
     */
    round(this: Is<number, T, this>): ValueProxy<number>;
    /**
     * Round to the higher integer.
     *
     * @returns Integer
     */
    ceil(this: Is<number, T, this>): ValueProxy<number>;
    /**
     * Round to the lower integer.
     *
     * @returns Integer
     */
    floor(this: Is<number, T, this>): ValueProxy<number>;
    /**
     * Bitwise AND operator.
     *
     * @param other Operand B
     * @return A & B
     */
    band(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Bitwise OR operator.
     *
     * @param other Operand B
     * @return A | B
     */
    bor(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Bitwise XOR operator.
     *
     * @param other Operand B
     * @return A ^ B
     */
    bxor(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Bitwise NOT operator.
     *
     * @return ~A
     */
    bnot(this: Is<number, T, this>): ValueProxy<number>;
    /**
     * Bitwise left shift operator.
     *
     * @param other Operand B
     * @returns A << B
     */
    blshift(this: Is<number, T, this>, other: ValueProxyOrValue<number>): ValueProxy<number>;
    /**
     * Bitwise right shift operator.
     *
     * @param other Operand B
     * @param preserveSign Preserve sign bit
     * @returns A >> B
     */
    brshift(this: Is<number, T, this>, other: ValueProxyOrValue<number>, preserveSign?: boolean): ValueProxy<number>;
    /**
     * Greater-than operator.
     *
     * @param other Operand B
     * @returns A > B
     */
    gt(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>): ValueProxy<boolean>;
    /**
     * Greater-or-equal operator.
     *
     * @param other Operand B
     * @returns A >= B
     */
    ge(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>): ValueProxy<boolean>;
    /**
     * Lesser-than operator.
     *
     * @param other Operand B
     * @returns A < B
     */
    lt(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>): ValueProxy<boolean>;
    /**
     * Lesser-or-equal operator.
     *
     * @param other Operand B
     * @returns A <= B
     */
    le(this: Is<Date | number | string, T, this>, other: ValueProxyOrValue<T>): ValueProxy<boolean>;
    /**
     * Splits the string using a separator.
     *
     * @param separator Separator string
     * @param maxSplits Maximum number of results
     * @returns Array of sub-strings
     */
    split(this: Is<string, T, this>, separator?: string, maxSplits?: number): ValueProxy<string[]>;
    /**
     * Concatenate the string with another.
     *
     * @param other Second string
     * @returns Concatenated string
     */
    concat(this: Is<string, T, this>, other: ValueProxyOrValue<string>): ValueProxy<string>;
    /**
     * Converts the string to all upper case.
     *
     * @returns New string
     */
    upcase(this: Is<string, T, this>): ValueProxy<string>;
    /**
     * Converts the string to all lower case.
     *
     * @returns New string
     */
    downcase(this: Is<string, T, this>): ValueProxy<string>;
    /**
     * Gets the number of Unicode codepoints in the string.
     *
     * @returns Number of codepoints
     */
    strlen(this: Is<string, T, this>): ValueProxy<number>;
    /**
     * Checks if the string matches a regex.
     *
     * @param regex Regex
     * @returns True if the string matched
     */
    match(this: Is<string, T, this>, regex: ValueProxyOrValue<string>): ValueProxy<boolean>;
    /**
     * Indexes the array with a given number position
     *
     * @param key Position in the array
     * @returns Element at the given position
     */
    index(this: IsArray<unknown, T, this>, key: ValueProxyOrValue<number>): ValueProxy<ArrayValue<T>>;
    /**
     * Tests if the arrays contains a value.
     *
     * @param val Value to search for.
     * @returns True if the value was found.
     */
    includes(this: IsArray<unknown, T, this>, val: ValueProxyOrValue<ArrayValue<T>>): ValueProxy<boolean>;
    /**
     * Returns a slice of the array.
     *
     * @param start Start index (inclusive, 0-indexed)
     * @param end End index (exclusive)
     * @returns Sub-array
     */
    slice(this: IsArray<unknown, T, this>, start: ValueProxyOrValue<number>, end?: ValueProxyOrValue<number>): ValueProxy<ArrayValue<T>[]>;
    /**
     * Maps the array values using a mapping function.
     *
     * @param mapper Mapping function
     * @returns New array
     */
    map<U>(this: IsArray<unknown, T, this>, mapper: (val: ValueProxy<ArrayValue<T>>) => U): ValueProxy<ExtractType<U>[]>;
    /**
     * Filters the array using a predicate function.
     *
     * @param predicate Predicate function.
     * @returns Filtered array
     */
    filter(this: IsArray<unknown, T, this>, predicate: (val: ValueProxy<ArrayValue<T>>) => ValueProxyOrValue<boolean>): ValueProxy<ArrayValue<T>[]>;
    /**
     * Checks if the array is empty.
     *
     * @returns True if the array is empty
     */
    isempty(this: IsArray<unknown, T, this>): ValueProxy<boolean>;
    /**
     * Gets the length of the array.
     *
     * @returns Length
     */
    count(this: IsArray<unknown, T, this>): ValueProxy<number>;
    /**
     * Gets the sum of a number array.
     *
     * @returns Sum
     */
    sum(this: IsArray<number, T, this>): ValueProxy<number>;
    /**
     * Gets the average of a number array.
     *
     * @returns Average
     */
    avg(this: IsArray<number, T, this>): ValueProxy<number>;
    /**
     * Gets the minimum of a number array.
     *
     * @returns Minimum
     */
    min(this: IsArray<number, T, this>): ValueProxy<number>;
    /**
     * Gets the maxmimum of a number array.
     *
     * @returns Maximum
     */
    max(this: IsArray<number, T, this>): ValueProxy<number>;
    /**
     * Indexes the object.
     *
     * TODO: Better name?
     *
     * @param key Field name
     * @param def Default value
     * @returns Value of the field
     */
    key<TO = OnlyObject<T>, K extends keyof TO = keyof TO, U = undefined>(this: Is<T, Record<any, any>, this>, key: ValueProxyOrValue<K>, def?: ValueProxyOrValue<U>): ValueProxy<U extends undefined ? TO[K] : U | Exclude<TO[K], null | undefined>>;
    /**
     * Merges the object with another.
     *
     * @param value Other object
     * @returns `{...A, ...B}`
     */
    merge<U, TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>, other: ValueProxyOrValue<U>): ValueProxy<Omit<TO, keyof U> & ExtractType<U>>;
    /**
     * Gets the keys of the object as an array.
     *
     * @returns Array of keys
     */
    keys<TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>): ValueProxy<(keyof TO)[]>;
    /**
     * Gets the values of the object as an array.
     *
     * @returns Array of values
     */
    values<TO = OnlyObject<T>>(this: Is<T, Record<any, any>, this>): ValueProxy<TO[keyof TO][]>;
    /**
     * Tests if the object has the specified fields.
     *
     * @param fields field list
     * @returns True if the object matches
     */
    hasfields(this: Is<T, Record<any, any>, this>, ...fields: string[]): ValueProxy<boolean>;
}
