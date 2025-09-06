spark.sql("set spark.sql.merge.schema=true") # enable schema evolution for merge without having to alter the table property

'''
Schema Evolution and INSERT Behavior in Delta Lake

1. Schema Evolution with spark.sql("set spark.sql.merge.schema=true")

-   This allows Delta to add new columns during a write or insert into … select ….

-   Example:
        insert into delta_table
        select a,b,c,d,e from some_source;

    ## Column e will be added to the schema, so the table becomes  a,b,c,d,e.

2. INSERT INTO delta_table VALUES (…)

-   This form is positional, not name-based.
-   If you provide more values than columns → Spark throws an error:

   Error: INSERT INTO table must have the same number of columns as the target table
        (e.g., 6 values for 5 columns = error).

3. What if counts match?

    insert into delta_table values (1,2,3,4,5);

-   Mapping:
    -   a = 1
    -   b = 2
    -   c = 3
    -   d = 4
    -   e = 5

# Works fine.

Answer: Schema evolution only applies when new column names are introduced
through a named insert or write (like insert into … select …).
With a plain VALUES insert, mapping is positional, so you cannot
introduce new column names.
If you supply more values than columns, Spark will throw an error rather
than invent new column names.
'''


''''
Type Widening in Delta Lake
===================================================
1. Supported behavior:
   - Delta supports "type widening" during writes and merges.
   - Examples:
     * IntegerType -> LongType (OK)
     * FloatType -> DoubleType (OK)

2. Not supported (risk of data loss or corruption):
   - LongType -> IntegerType (❌ data loss risk)
   - DoubleType -> FloatType (❌ data loss risk)
   - StringType -> IntegerType (❌ corruption risk)

3. MERGE statement:
   - Same rules apply as for writes.
   - Integer -> Long (OK), but Long -> Integer (❌)

4. Manual schema changes:
   - Delta does not automatically widen types.
   - To widen a column type, you must manually alter the schema:
     ```sql
     ALTER TABLE delta_table ALTER COLUMN a TYPE BIGINT;
     ```

Summary:
Type widening in Delta Lake allows safe promotion to wider numeric types,
but never automatic narrowing or unsafe casts. Manual ALTER is needed
to persist the change in table schema.

"Delta Lake supports type widening, 
meaning values can be safely promoted from a smaller type to a larger one, but not the other way around."
'''

'''
In Spark, a struct basically means a nested data type — like a mini-row (with its own fields) inside a column.

id | name    | address
-------------------------------------------
1  | Binaya  | {city=Kathmandu, zip=44600}
2  | Anisha  | {city=Pokhara, zip=33700}
'''

