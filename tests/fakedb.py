"""A fake Supabase client with real filter semantics for service tests.

Unlike the minimal fake in test_reconciliation_v2 (no-op filters), this one
applies eq/neq/in_/is_/gte/lte/like, order, limit/range, and supports
insert/update/delete — enough to exercise the upsert-style services
(commission rules versioning, exception cases, disputes, audit findings).
"""
import uuid


class FakeQuery:
    def __init__(self, db, table):
        self.db, self.tname = db, table
        self.filters = []
        self.order_by = []
        self._limit = None
        self._range = None
        self._action = ("select", None)

    # -- builders ----------------------------------------------------------
    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: r.get(col) == val)
        return self

    def neq(self, col, val):
        self.filters.append(lambda r: r.get(col) != val)
        return self

    def in_(self, col, vals):
        vals = list(vals)
        self.filters.append(lambda r: r.get(col) in vals)
        return self

    def is_(self, col, val):
        if val == "null":
            self.filters.append(lambda r: r.get(col) is None)
        return self

    def gte(self, col, val):
        self.filters.append(lambda r: r.get(col) is not None and r.get(col) >= val)
        return self

    def lte(self, col, val):
        self.filters.append(lambda r: r.get(col) is not None and r.get(col) <= val)
        return self

    def like(self, col, pattern):
        needle = pattern.strip("%")
        self.filters.append(lambda r: needle in str(r.get(col) or ""))
        return self

    def order(self, col, desc=False):
        self.order_by.append((col, desc))
        return self

    def limit(self, n):
        self._limit = n
        return self

    def range(self, a, b):
        self._range = (a, b)
        return self

    def insert(self, rows):
        self._action = ("insert", rows if isinstance(rows, list) else [rows])
        return self

    def update(self, fields):
        self._action = ("update", fields)
        return self

    def delete(self):
        self._action = ("delete", None)
        return self

    # -- executor -----------------------------------------------------------
    def _matching(self, rows):
        return [r for r in rows if all(f(r) for f in self.filters)]

    def execute(self):
        class R:
            data = None
            count = 0
        res = R()
        table = self.db.tables.setdefault(self.tname, [])
        kind, payload = self._action

        if kind == "insert":
            stored = []
            for row in payload:
                row = dict(row)
                row.setdefault("id", str(uuid.uuid4()))
                table.append(row)
                stored.append(row)
            res.data = stored
        elif kind == "update":
            updated = []
            for r in self._matching(table):
                r.update(payload)
                updated.append(dict(r))
            res.data = updated
        elif kind == "delete":
            keep, gone = [], []
            for r in table:
                (gone if all(f(r) for f in self.filters) else keep).append(r)
            self.db.tables[self.tname] = keep
            res.data = gone
        else:
            rows = self._matching(table)
            for col, desc in reversed(self.order_by):
                rows = sorted(rows, key=lambda r: (r.get(col) is None, r.get(col)),
                              reverse=desc)
            if self._range:
                a, b = self._range
                rows = rows[a:b + 1]
            elif self._limit is not None:
                rows = rows[:self._limit]
            res.data = [dict(r) for r in rows]
        res.count = len(res.data or [])
        return res


class _FakeBucket:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def upload(self, path, blob, opts=None):
        self.store[f"{self.name}/{path}"] = blob

    def download(self, path):
        return self.store[f"{self.name}/{path}"]


class _FakeStorage:
    def __init__(self):
        self.blobs = {}

    def from_(self, bucket):
        return _FakeBucket(self.blobs, bucket)

    def create_bucket(self, name):
        pass


class FakeDB:
    def __init__(self):
        self.tables = {}
        self.storage = _FakeStorage()

    def table(self, name):
        return FakeQuery(self, name)
