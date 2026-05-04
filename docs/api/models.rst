Models
======

TransferMode
------------

Enum controlling how SyncDB handles existing rows in the target table.

.. autoclass:: syncdb.TransferMode
   :members:
   :undoc-members: True
   :show-inheritance:

Values
~~~~~~

.. list-table::
   :header-rows: 1

   * - Value
     - String alias
     - Description
   * - ``TransferMode.APPEND``
     - ``"append"``
     - Upsert by primary key — delete matching PKs then insert
   * - ``TransferMode.INSERT_ONLY``
     - ``"insert_only"``
     - Insert every row; never update or delete existing rows
   * - ``TransferMode.UPSERT``
     - ``"upsert"``
     - Explicit upsert semantics (same mechanics as ``APPEND``)
   * - ``TransferMode.FULL_REFRESH``
     - ``"full_refresh"``
     - Truncate target once, then insert all source rows
   * - ``TransferMode.APPEND_STAGING``
     - ``"append_staging"``
     - Load into staging table, then swap into live table
   * - ``TransferMode.SNAPSHOT``
     - ``"snapshot"``
     - Append all rows with a ``_synced_at`` timestamp column
   * - ``TransferMode.SOFT_DELETE``
     - ``"soft_delete"``
     - Upsert existing rows; set ``deleted_at`` on missing rows

TableSyncResult
---------------

Dataclass returned by :meth:`~syncdb.SyncDB.sync_tables` for each table. All fields are read-only.

.. autoclass:: syncdb.TableSyncResult
   :members:
   :undoc-members: True
   :show-inheritance:

Fields
~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Field
     - Type
     - Description
   * - ``name``
     - ``str``
     - Logical name from the table spec dict key
   * - ``source``
     - ``str``
     - Source table as specified
   * - ``destination``
     - ``str``
     - Destination table as specified
   * - ``mode``
     - ``str``
     - Transfer mode used
   * - ``rows_read``
     - ``int``
     - Total rows read from source
   * - ``rows_written``
     - ``int``
     - Total rows written to target
   * - ``batches``
     - ``int``
     - Number of batches processed
   * - ``table_created``
     - ``bool``
     - ``True`` if the target table was created
   * - ``schema_created``
     - ``bool``
     - ``True`` if the target schema was created
   * - ``columns_added``
     - ``list[str]``
     - Column names added to the target
   * - ``columns_dropped``
     - ``list[str]``
     - Column names dropped from the target
   * - ``rows_soft_deleted``
     - ``int``
     - Rows marked ``deleted_at`` in ``soft_delete`` mode
   * - ``expectations_failed``
     - ``list[str]``
     - Failure messages from ``expect`` checks (empty if all passed)
   * - ``watermark_value``
     - ``Any``
     - Highest watermark value seen in this sync run
   * - ``dry_run``
     - ``bool``
     - ``True`` if this was a dry run
   * - ``duration_seconds``
     - ``float``
     - Wall-clock seconds from start to finish
