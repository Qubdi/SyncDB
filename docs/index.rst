SyncDB
======

Python ETL helper for moving tabular data between **MSSQL**, **PostgreSQL**, **MySQL**, and **local files**
(CSV, Parquet, Excel, Pickle), with automatic schema creation, schema evolution, and batch progress reporting.

.. code-block:: python

   from syncdb import DatabaseConfig, SyncDB

   src = DatabaseConfig(engine="mssql", connection_string="...")
   dst = DatabaseConfig(engine="postgresql", connection_string="...")

   sync = SyncDB(source=src, target=dst)
   sync.sync_tables({
       "orders": {
           "source": "dbo.orders",
           "destination": "public.orders",
           "mode": "append",
           "primary_key": ["order_id"],
       }
   })

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   installation
   quickstart

.. toctree::
   :maxdepth: 1
   :caption: User Guide

   user-guide/configuration
   user-guide/transfer-modes
   user-guide/syncing
   user-guide/schema-evolution
   user-guide/files
   user-guide/incremental
   user-guide/data-quality
   user-guide/progress

.. toctree::
   :maxdepth: 1
   :caption: API Reference

   api/index
   api/syncdb
   api/config
   api/models
   api/files
   api/schema

.. toctree::
   :maxdepth: 1
   :caption: Development

   contributing
   changelog
