DatabaseConfig
==============

Frozen dataclass describing a database connection. Create one per database and pass it to :class:`~syncdb.SyncDB`.

.. autoclass:: syncdb.DatabaseConfig
   :members:
   :undoc-members: False
   :show-inheritance:
   :member-order: bysource

Parameters
----------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Parameter
     - Default
     - Description
   * - ``engine``
     - **required**
     - Engine name or alias: ``"mssql"``, ``"postgresql"``, ``"mysql"``, ``"sqlite"``
   * - ``connection_string``
     - ``None``
     - Full DSN or connection URL
   * - ``host``
     - ``None``
     - Server hostname
   * - ``port``
     - engine default
     - Server port
   * - ``database``
     - ``None``
     - Database name
   * - ``user``
     - ``None``
     - Login username
   * - ``password``
     - ``None``
     - Login password
   * - ``default_schema``
     - engine default
     - Schema prefix for unqualified table names
   * - ``connect_timeout``
     - ``30``
     - Seconds before a connection attempt fails
   * - ``options``
     - ``{}``
     - Extra driver-specific keyword arguments

Engine aliases
--------------

.. list-table::
   :header-rows: 1

   * - Alias(es)
     - Resolved engine
   * - ``"mssql"``, ``"sqlserver"``, ``"sql_server"``
     - ``mssql``
   * - ``"postgresql"``, ``"postgres"``, ``"pg"``
     - ``postgresql``
   * - ``"mysql"``
     - ``mysql``
   * - ``"sqlite"``, ``"sqlite3"``
     - ``sqlite``
