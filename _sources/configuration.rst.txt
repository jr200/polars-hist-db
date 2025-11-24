Configuration
=============

The components of a configuration file are:

.. contents::
   :local:
   :depth: 2

This document describes the core parts of the configuration file. For more details refer to the :doc:`api` section.

db
---

Connection details for the target database.
Example:

.. code-block:: yaml

   db:
     backend: mariadb
     hostname: 127.0.0.1
     port: 3306
     username: root
     password: admin



table_configs
-------------

A collection of tables that will be managed by this configuration.
Example:

.. code-block:: yaml

    table_configs:
    - schema: test
      name: simple_table
      columns:
      - id
      - another_id
      - [ col_double, col_varchar ]
      primary_keys:
      - id
      foreign_keys:
      - name: another_id
        references:
          table: another_table
          column: id
      is_temporal: true
      delta_config:
        drop_unchanged_rows: true
        on_duplicate_key: take_last
        prefill_nulls_with_default: false
        row_finality: dropout


The main section defines usual properties of the table, including the name of the table, the column names, plus any primary and foreign keys.

A flag ``is_temporal`` indicates whether the table uses system versioning. Uploading of temporal data is done in two parts. First the data is staged to a temporary table, allowing some computations to be done before modifying the target table. Specifically:

- rows can be dropped from the staged table if they have not changed, meaning the as-of date of the associated data would not be modified.
- sometimes within a single batch of data, multiple versions of the same row may be present. The ``primary_key`` defines how rows are tested for 'equality' and the ``on_duplicate_key`` option controls how duplicates are handled.
- the ``row_finality`` option controls what happens when a row is no longer present in the data-batch.


column_definitions
------------------

A set of typed column definitions referenced by ``table_configs`` and ``datasets`` section of the files.

.. code-block:: yaml

    column_definitions:
    - { name: rating, data_type: INT, header: Rating, nullable: false, default_value: 0 }
    - {
        name: price_usd,
        data_type: 'DECIMAL(10,4)',
        transforms: {
            try_to_usd: [ 'Price', 'Year' ]
        }
      }
    - { name: month, data_type: VARCHAR(2), header: Month }
    - { name: year, data_type: INT, header: Year }



Each `column definition` defines a column to be created in the staging table. This is either a mapping from a column in the source data file (called ``header`` when the name is different), or a derived column according to some user-defined transformation. An example of how to define a transformation is the ``price_usd`` column above, with full details given in the example notebook. 


datasets
--------

This is a set of data sources and pipelines, describing `where` to look for new data, and `how` to process it into the defined tables. Example:

.. code-block:: yaml

    datasets:
    - name: turkey_food_prices
      delta_table_schema: test
      scrape_limit: ~
      search_paths:
      - root_path: ../_testdata_dataset_data
        file_include: ['turkey_food_prices.csv']
        is_enabled: true
        timestamp:
            source_tz: Europe/London
            method: mtime
       - ...

      pipeline:
      - table: unit_info
        columns:
        - id<um_id!
        - name<um_name!

      - table: food_prices
        type: primary
        columns:
        - product_id!
        - um_id!
        - price!
        - price_usd!

The ``search_paths`` section defines where to find the data. It example above uses the ``timestamp.method`` of `mtime` to set the `as-of` date of the data.

The ``pipeline`` section contains a sequence of tasks that are run in order. The example above demonstrates two tasks:

**Task 1: Unit Info Table**

This task populates the ``unit_info`` table:

- Reads ``um_id`` and ``um_name`` columns from the staging table
- Renames them using ``<`` operator:

  - ``um_id`` to ``id``
  - ``um_name`` to ``name``
- Both columns are required (marked with ``!``)

**Task 2: Food Prices Table**

This task populates the ``food_prices`` table:

- Reads ``product_id``, ``um_id``, ``price``, and ``price_usd`` columns
- Maps directly to matching column names (no renaming needed)
- All columns are required (marked with ``!``)
- Table is marked as ``type: primary`` for system versioning
- Note: Currently only one primary task is allowed per pipeline
