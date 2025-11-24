API Reference
=============

.. automodule:: polars_hist_db
   :members:
   :undoc-members:
   :show-inheritance:

Configuration Management
------------------------

This section contains objects to access the configuration structure instantiated from YAML.

.. automodule:: polars_hist_db.config
   :members:
   :undoc-members:
   :show-inheritance:

Core Operations
---------------

The core module contains the operations that interface between polars and the database. It provides:

* Database operations (DbOps)
* Table operations and management (TableOps, DeltaTableOps)
* Dataframe operations (DataframeOps)
* Audit fo data inputs functionality (AuditOps)
* Table configuration operations (TableConfigOps)

Some helpers for:
* Constructing time-based hints at query time
* Database engine creation utilities

.. automodule:: polars_hist_db.core
   :members:
   :undoc-members:
   :show-inheritance:

Dataset Management
------------------

The dataset module is used to execute the scrape.

.. automodule:: polars_hist_db.dataset
   :members:
   :undoc-members:
   :show-inheritance:

Data Loading
------------

The loaders provide advanced utilities for processing files. It includes:

* DSV (Delimiter-Separated Values) file loading with type support
* Fast file search capabilities
* ZIP file processing tools for converting zipped CSVs to Parquet format

.. automodule:: polars_hist_db.loaders
   :members:
   :undoc-members:
   :show-inheritance:

Type Helpers
------------

This module contains some helpers to map between SQL Types, SQLAlchemy Types, and Polars Types.

.. automodule:: polars_hist_db.types
   :members:
   :undoc-members:
   :show-inheritance: 