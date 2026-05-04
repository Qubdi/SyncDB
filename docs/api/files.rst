File Transfer
=============

FileTransfer
------------

Helper for reading and writing tabular data from/to local files. No database connection required.

.. autoclass:: syncdb.FileTransfer
   :members:
   :undoc-members: False
   :show-inheritance:
   :member-order: bysource

FileFormat
----------

Enum for supported file formats. The format is inferred from the file extension when not specified explicitly.

.. autoclass:: syncdb.FileFormat
   :members:
   :undoc-members: True
   :show-inheritance:

Values
~~~~~~

.. list-table::
   :header-rows: 1

   * - Value
     - Extension(s)
     - Extra required
   * - ``FileFormat.CSV``
     - ``.csv``
     - none
   * - ``FileFormat.PARQUET``
     - ``.parquet``
     - ``pandas``, ``pyarrow``
   * - ``FileFormat.EXCEL``
     - ``.xlsx``, ``.xls``
     - ``pandas``, ``openpyxl``
   * - ``FileFormat.PICKLE``
     - ``.pickle``
     - none
