SyncDB
======

The main orchestrator class. Create one instance per job and call ``sync_tables``,
``sync_schema``, or one of the file methods.

.. autoclass:: syncdb.SyncDB
   :members:
   :undoc-members: False
   :show-inheritance:
   :member-order: bysource

Constructor
-----------

.. code-block:: python

   SyncDB(
       source: DatabaseConfig | None = None,
       target: DatabaseConfig | None = None,
       batch_size: int | str = 5000,
       progress_mode: ProgressMode | str = ProgressMode.multi_line,
       dry_run: bool = False,
       drop_extra_columns: bool = False,
       verbose: str | None = "standard",
       verbose_stream: TextIO | None = None,
       retry_count: int = 0,
       retry_delay_seconds: float = 1.0,
   )

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``source``
     - ``None``
     - Source database configuration
   * - ``target``
     - ``None``
     - Target database configuration
   * - ``batch_size``
     - ``5000``
     - Rows per batch — integer count or ``"10%"`` percentage string
   * - ``progress_mode``
     - ``multi_line``
     - Progress bar display mode
   * - ``dry_run``
     - ``False``
     - Report changes without writing data
   * - ``drop_extra_columns``
     - ``False``
     - Drop target columns absent from source
   * - ``verbose``
     - ``"standard"``
     - Auto-print summary: ``"standard"``, ``"detailed"``, or ``None``
   * - ``verbose_stream``
     - ``sys.stdout``
     - Output stream for the summary table
   * - ``retry_count``
     - ``0``
     - Retry failed batch writes up to this many times
   * - ``retry_delay_seconds``
     - ``1.0``
     - Initial retry delay; doubles after each retry
