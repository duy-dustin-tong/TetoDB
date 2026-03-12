"""
TetoDB DBAPI 2.0 Driver (PEP-249)
Communicates with TetoDB server via the TetoWire binary protocol.
"""

import socket
import struct
import re

# --- PEP-249 Module-Level Attributes ---
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections
paramstyle = "qmark"  # ? style parameters


# --- Exceptions (PEP-249) ---
class Error(Exception):
    pass

class DatabaseError(Error):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class InterfaceError(Error):
    pass


# --- TetoWire Helpers ---

def _send_query(sock, sql):
    """Send a Q packet: [type 'Q'] [4-byte len] [sql string + null]"""
    payload = sql.encode("utf-8") + b"\x00"
    length = len(payload) + 4  # length includes itself
    packet = b"Q" + struct.pack("!I", length) + payload
    sock.sendall(packet)


def _send_disconnect(sock):
    """Send an X packet to disconnect."""
    packet = b"X" + struct.pack("!I", 4)
    sock.sendall(packet)


def _recv_all(sock, n):
    """Read exactly n bytes from socket."""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise InterfaceError("Connection closed by server")
        data += chunk
    return data


def _recv_packet(sock):
    """
    Read one TetoWire packet.
    Returns (type_char, payload_bytes)
    """
    type_byte = _recv_all(sock, 1)
    len_bytes = _recv_all(sock, 4)
    total_len = struct.unpack("!I", len_bytes)[0]
    payload_len = total_len - 4
    payload = _recv_all(sock, payload_len) if payload_len > 0 else b""
    return type_byte.decode("ascii"), payload


def _read_string(data, offset):
    """Read a null-terminated string from data at offset."""
    end = data.index(b"\x00", offset)
    return data[offset:end].decode("utf-8"), end + 1


def _parse_row_description(payload):
    """Parse a 'T' packet into a list of (col_name, type_name) tuples."""
    col_count = struct.unpack("!H", payload[0:2])[0]
    offset = 2
    columns = []
    for _ in range(col_count):
        name, offset = _read_string(payload, offset)
        type_name, offset = _read_string(payload, offset)
        columns.append((name, type_name))
    return columns


def _parse_data_row(payload):
    """Parse a 'D' packet into a list of Python values (strings or None)."""
    col_count = struct.unpack("!H", payload[0:2])[0]
    offset = 2
    values = []
    for _ in range(col_count):
        col_len = struct.unpack("!i", payload[offset:offset + 4])[0]
        offset += 4
        if col_len == -1:
            values.append(None)
        else:
            val_bytes = payload[offset:offset + col_len]
            values.append(val_bytes.decode("utf-8"))
            offset += col_len
    return values


def _coerce_value(val_str, type_name):
    """Convert a string value from the server into the appropriate Python type."""
    if val_str is None:
        return None
    if type_name in ("INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
        try:
            return int(val_str)
        except ValueError:
            return val_str
    if type_name == "DECIMAL":
        try:
            return float(val_str)
        except ValueError:
            return val_str
    if type_name == "BOOLEAN":
        return val_str.lower() in ("true", "1", "t")
    return val_str


def _parse_rowcount(status_msg):
    """Extract rowcount from status message like 'INSERT 0 3' or 'UPDATE 2'."""
    parts = status_msg.split()
    if len(parts) >= 2:
        try:
            return int(parts[-1])
        except ValueError:
            pass
    return -1


# --- PEP-249 Connection ---

class Connection:
    def __init__(self, host="127.0.0.1", port=5432, database="e2e"):
        self._host = host
        self._port = port
        self._database = database
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._closed = False

        # Wait for initial ReadyForQuery ('Z') packet
        ptype, payload = _recv_packet(self._sock)
        if ptype != "Z":
            raise InterfaceError(f"Expected 'Z' ready signal, got '{ptype}'")

    def cursor(self):
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def commit(self):
        if self._closed:
            raise InterfaceError("Connection is closed")
        c = self.cursor()
        c.execute("COMMIT;")
        c.close()

    def rollback(self):
        if self._closed:
            raise InterfaceError("Connection is closed")
        c = self.cursor()
        c.execute("ROLLBACK;")
        c.close()

    def close(self):
        if not self._closed:
            try:
                _send_disconnect(self._sock)
            except Exception:
                pass
            self._sock.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


# --- PEP-249 Cursor ---

class Cursor:
    def __init__(self, connection):
        self._conn = connection
        self._rows = []
        self._row_idx = 0
        self.description = None
        self.rowcount = -1
        self._closed = False
        self._columns = []  # (name, type_name) tuples

    def execute(self, sql, parameters=None):
        if self._closed:
            raise InterfaceError("Cursor is closed")

        # Simple parameter substitution (replace ? with values)
        if parameters:
            parts = sql.split("?")
            if len(parts) - 1 != len(parameters):
                raise ProgrammingError("Number of parameters doesn't match placeholders")
            new_sql = parts[0]
            for i, param in enumerate(parameters):
                if param is None:
                    new_sql += "NULL"
                elif isinstance(param, str):
                    escaped = param.replace("'", "''")
                    new_sql += f"'{escaped}'"
                elif isinstance(param, bool):
                    new_sql += "TRUE" if param else "FALSE"
                elif isinstance(param, (int, float)):
                    new_sql += str(param)
                else:
                    new_sql += f"'{param}'"
                new_sql += parts[i + 1]
            sql = new_sql

        self._rows = []
        self._row_idx = 0
        self.description = None
        self.rowcount = -1
        self._columns = []

        _send_query(self._conn._sock, sql)

        # Read response until 'Z'
        while True:
            ptype, payload = _recv_packet(self._conn._sock)

            if ptype == "T":
                self._columns = _parse_row_description(payload)
                # Build PEP-249 description: (name, type_code, ...)
                self.description = [
                    (name, type_name, None, None, None, None, None)
                    for name, type_name in self._columns
                ]
            elif ptype == "D":
                raw_vals = _parse_data_row(payload)
                # Coerce values based on column types
                row = []
                for i, val in enumerate(raw_vals):
                    type_name = self._columns[i][1] if i < len(self._columns) else "VARCHAR"
                    row.append(_coerce_value(val, type_name))
                self._rows.append(tuple(row))
            elif ptype == "C":
                status, _ = _read_string(payload, 0)
                self.rowcount = _parse_rowcount(status)
            elif ptype == "E":
                error_msg, _ = _read_string(payload, 0)
                raise DatabaseError(error_msg)
            elif ptype == "Z":
                break

    def executemany(self, sql, seq_of_parameters):
        for params in seq_of_parameters:
            self.execute(sql, params)

    def fetchone(self):
        if self._row_idx < len(self._rows):
            row = self._rows[self._row_idx]
            self._row_idx += 1
            return row
        return None

    def fetchmany(self, size=None):
        if size is None:
            size = 1
        rows = self._rows[self._row_idx:self._row_idx + size]
        self._row_idx += len(rows)
        return rows

    def fetchall(self):
        rows = self._rows[self._row_idx:]
        self._row_idx = len(self._rows)
        return rows

    def close(self):
        self._closed = True

    @property
    def lastrowid(self):
        return None

    def __iter__(self):
        return iter(self._rows)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


# --- Module-level connect() ---

def connect(host="127.0.0.1", port=5432, database="e2e", **kwargs):
    """Create a new TetoDB connection."""
    return Connection(host=host, port=port, database=database)
