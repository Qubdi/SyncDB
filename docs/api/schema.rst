Schema Mapping
==============

SchemaMapper
------------

Translates column types between database engines. Used internally by SyncDB during table creation and schema evolution.

.. autoclass:: syncdb.SchemaMapper
   :members:
   :undoc-members: False
   :show-inheritance:
   :member-order: bysource

Column
------

Descriptor dataclass representing a single column in a table schema.

.. autoclass:: syncdb.Column
   :members:
   :undoc-members: True
   :show-inheritance:

Type mapping reference
----------------------

The table below shows conservative mappings applied when copying from one engine to another.
SyncDB always widens rather than truncates.

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - MSSQL source
     - PostgreSQL target
     - Notes
   * - ``nvarchar(n)``
     - ``varchar(n)``
     -
   * - ``nvarchar(max)``
     - ``text``
     -
   * - ``bit``
     - ``boolean``
     -
   * - ``datetime``, ``datetime2``
     - ``timestamp``
     -
   * - ``decimal(p,s)``
     - ``numeric(p,s)``
     -
   * - ``uniqueidentifier``
     - ``uuid``
     -
   * - ``int``
     - ``integer``
     -
   * - ``bigint``
     - ``bigint``
     -
   * - ``float``
     - ``double precision``
     -
   * - ``varbinary(max)``
     - ``bytea``
     -

Use ``type_overrides`` in a table spec to override the automatic mapping for specific columns:

.. code-block:: python

   "orders": {
       "source": "dbo.orders",
       "destination": "public.orders",
       "type_overrides": {
           "price": "numeric(18,4)",
           "notes": "text",
           "flags": "jsonb",
       },
   }
