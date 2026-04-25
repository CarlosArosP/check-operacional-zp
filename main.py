"""Check Operacional ZP — FastAPI backend."""
from __future__ import annotations

import hashlib
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Paths ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "check.db"
STATIC   = BASE_DIR / "static"

# ── Admin credentials (sha256 de "Carlett2042") ────────────────────────
ADMIN_USER = "caros"
ADMIN_HASH = hashlib.sha256(b"Carlett2042").hexdigest()

# ── Database driver ───────────────────────────────────────────────────
# Turso en prod (env vars) → SQLite en la nube, datos permanentes.
# SQLite local en dev → archivo check.db.
TURSO_URL   = os.environ.get("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")


class _TursoRow(dict):
    """Acceso por clave str Y por índice int, igual que sqlite3.Row."""
    def __init__(self, cols: list, values: list):
        super().__init__(zip(cols, values))
        self._v = list(values)
    def __getitem__(self, key):
        return self._v[key] if isinstance(key, int) else super().__getitem__(key)
    def __iter__(self):
        return iter(self._v)


class _TursoCursor:
    """Resultado de TursoConn.execute() — porta lastrowid y filas."""
    def __init__(self, rows: list, lastrowid):
        self._rows    = rows
        self.lastrowid = lastrowid
    def fetchall(self):  return self._rows
    def fetchone(self):  return self._rows[0] if self._rows else None
    def __getitem__(self, i): return self._rows[i]


class TursoConn:
    """Wrapper sqlite3-compatible sobre la HTTP API de Turso. Sin deps nativos."""

    def __init__(self, db_url: str, token: str):
        self._url   = db_url.replace("libsql://", "https://") + "/v2/pipeline"
        self._token = token
        self.row_factory = None   # aceptado pero ignorado

    # ── helpers ──────────────────────────────────────────────────────
    @staticmethod
    def _enc(v):
        if v is None:            return {"type": "null",    "value": None}
        if isinstance(v, bool):  return {"type": "integer", "value": str(int(v))}
        if isinstance(v, int):   return {"type": "integer", "value": str(v)}
        if isinstance(v, float): return {"type": "float",   "value": str(v)}
        return {"type": "text", "value": str(v)}

    @staticmethod
    def _dec(v):
        t = v["type"]
        if t == "null":    return None
        if t == "integer": return int(v["value"])
        if t == "float":   return float(v["value"])
        return v["value"]

    def _pipeline(self, stmts: list) -> list:
        import json, urllib.request
        body = json.dumps({
            "requests": [{"type": "execute", "stmt": s} for s in stmts]
                        + [{"type": "close"}]
        }).encode()
        req = urllib.request.Request(
            self._url, data=body, method="POST",
            headers={"Authorization": f"Bearer {self._token}",
                     "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())["results"]

    def _stmt(self, sql: str, params=()):
        return {"sql": sql, "args": [self._enc(p) for p in params]}

    def _parse(self, result: dict) -> _TursoCursor:
        if result["type"] != "ok":
            raise Exception(f"Turso error: {result}")
        res  = result["response"]["result"]
        cols = [c["name"] for c in res["cols"]]
        rows = [_TursoRow(cols, [self._dec(v) for v in row]) for row in res["rows"]]
        rid  = res.get("last_insert_rowid")
        return _TursoCursor(rows, int(rid) if rid else None)

    # ── API pública (compatible con sqlite3) ─────────────────────────
    def execute(self, sql: str, params=()):
        return self._parse(self._pipeline([self._stmt(sql, params)])[0])

    def executemany(self, sql: str, seq):
        results = self._pipeline([self._stmt(sql, p) for p in seq])
        for r in results:
            if r["type"] != "ok": raise Exception(f"Turso error: {r}")
        return self

    def executescript(self, script: str):
        import re
        stmts = [
            self._stmt(s.strip())
            for s in re.split(r";\s*", script)
            if s.strip() and not s.strip().startswith("--")
        ]
        if not stmts: return self
        results = self._pipeline(stmts)
        for r in results:
            if r["type"] != "ok": raise Exception(f"Turso error: {r}")
        return self

    def commit(self): pass   # Turso auto-commit por statement
    def close(self):  pass


@contextmanager
def get_db():
    if TURSO_URL:
        con: Any = TursoConn(TURSO_URL, TURSO_TOKEN)
    else:
        con = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    if TURSO_URL:
        # Tablas ya creadas manualmente en Turso — solo sembrar si está vacío
        _seed_rutinas()
    else:
        # SQLite local: crear tablas y sembrar
        with get_db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS rutinas (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    turno       TEXT    NOT NULL,
                    horario     TEXT    DEFAULT '',
                    rutina      TEXT    NOT NULL,
                    accionable  TEXT    DEFAULT '',
                    responsable TEXT    DEFAULT '',
                    evidencia   TEXT    DEFAULT '',
                    orden       INTEGER DEFAULT 0,
                    created_at  TEXT    DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS registros (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    local       TEXT    NOT NULL,
                    fecha       TEXT    NOT NULL,
                    rutina_id   INTEGER NOT NULL,
                    done        INTEGER NOT NULL DEFAULT 0,
                    obs         TEXT    DEFAULT '',
                    ts          TEXT    DEFAULT '',
                    UNIQUE(local, fecha, rutina_id)
                );
                CREATE INDEX IF NOT EXISTS idx_reg_local_fecha
                    ON registros(local, fecha)
            """)
        _seed_rutinas()


def _seed_rutinas() -> None:
    """Inserta rutinas por defecto si la tabla está vacía."""
    with get_db() as db:
        if db.execute("SELECT COUNT(*) FROM rutinas").fetchone()[0]:
            return
        defaults = [
            ("AM","07:30-08:00","Analisis de Datos",
             "Revisar WIC e imprimir planificacion diaria ZP por hora. Revisar disponibilidad Apoyo durante turno AM.",
             "Tesorero","Fotografia al grupo interno",0),
            ("AM","08:00-08:15","Planificación Colaciones",
             "Coordinar con Enc. Local la conexión de Dispo. Apoyo en horas peak / verificar inasistencias.",
             "Tesorero / Enc. Local","Evidencia fotográfica en grupo interno / adherencia a planificación",1),
            ("AM","12:00-14:00","Control de Colaciones AM",
             "Verificar conexión de apoyo de tienda.",
             "Encargado Local","Registro en grupo interno / adherencia a planificación",2),
            ("PM","14:00-15:00","Entrega de Turno - Adherencia AM",
             "Entrega Adherencia turno AM.",
             "Tesorero / Enc. Local","Evidencia fotográfica turno PM",3),
            ("PM","14:00-15:00","Planificación Turno PM",
             "Planificación turno PM, apoyos en horas peak y planificación de EST PM.",
             "Tesorero / Enc. Local","Evidencia fotográfica turno PM",4),
            ("PM","17:00-19:30","Control de Colaciones PM",
             "Verificar colaciones de dispo. Apoyo.",
             "Encargado Local","Adherencia a planificación",5),
        ]
        db.executemany(
            "INSERT INTO rutinas(turno,horario,rutina,accionable,responsable,evidencia,orden) VALUES(?,?,?,?,?,?,?)",
            defaults,
        )


# ── App ────────────────────────────────────────────────────────────────
app = FastAPI(title="Check Operacional ZP", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


# ── Schemas ────────────────────────────────────────────────────────────
class RutinaIn(BaseModel):
    """Payload para crear/actualizar una rutina (incluye credenciales admin)."""
    turno: str
    horario: str = ""
    rutina: str
    accionable: str = ""
    responsable: str = ""
    evidencia: str = ""
    orden: int = 0
    # credenciales admin — requeridas en escritura
    usuario: str = ""
    password_hash: str = ""


class RegistroIn(BaseModel):
    local: str
    fecha: str
    rutina_id: int
    done: bool
    obs: str = ""
    ts: str = ""


class DeleteBody(BaseModel):
    """Body para DELETE con auth admin."""
    usuario: str = ""
    password_hash: str = ""


# ── Auth helper ────────────────────────────────────────────────────────
def require_admin(usuario: str, password_hash: str) -> None:
    ok = (usuario == ADMIN_USER
          and password_hash.lower() == ADMIN_HASH)
    if not ok:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Sin permisos de admin.")


# ── Routes ─────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html)


# ── Rutinas ────────────────────────────────────────────────────────────
@app.get("/api/rutinas")
def get_rutinas():
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM rutinas ORDER BY orden, id"
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/rutinas", status_code=201)
def add_rutina(body: RutinaIn):
    require_admin(body.usuario, body.password_hash)
    with get_db() as db:
        cur = db.execute(
            "INSERT INTO rutinas(turno,horario,rutina,accionable,responsable,evidencia,orden)"
            " VALUES(?,?,?,?,?,?,?)",
            (body.turno, body.horario, body.rutina,
             body.accionable, body.responsable, body.evidencia, body.orden),
        )
    return {"id": cur.lastrowid}


@app.put("/api/rutinas/{rid}")
def update_rutina(rid: int, body: RutinaIn):
    require_admin(body.usuario, body.password_hash)
    with get_db() as db:
        db.execute(
            "UPDATE rutinas SET turno=?,horario=?,rutina=?,accionable=?,responsable=?,evidencia=?,orden=?"
            " WHERE id=?",
            (body.turno, body.horario, body.rutina,
             body.accionable, body.responsable, body.evidencia, body.orden, rid),
        )
    return {"ok": True}


@app.delete("/api/rutinas/{rid}")
def delete_rutina(rid: int, body: DeleteBody):
    require_admin(body.usuario, body.password_hash)
    with get_db() as db:
        db.execute("DELETE FROM rutinas WHERE id=?", (rid,))
    return {"ok": True}


# ── Registros ──────────────────────────────────────────────────────────
@app.get("/api/check")
def get_check(local: str, fecha: str):
    with get_db() as db:
        rows = db.execute(
            "SELECT * FROM registros WHERE local=? AND fecha=?",
            (local, fecha),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/check")
def save_check(body: RegistroIn):
    with get_db() as db:
        db.execute(
            """INSERT INTO registros(local,fecha,rutina_id,done,obs,ts)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(local,fecha,rutina_id)
               DO UPDATE SET done=excluded.done, obs=excluded.obs, ts=excluded.ts""",
            (body.local, body.fecha, body.rutina_id,
             int(body.done), body.obs, body.ts),
        )
    return {"ok": True}


# ── Stats ──────────────────────────────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    corte = (date.today() - timedelta(days=30)).isoformat()
    with get_db() as db:
        rows = db.execute(
            "SELECT local, fecha, rutina_id, done FROM registros WHERE fecha>=?",
            (corte,),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Debug (temporal) ──────────────────────────────────────────────────
@app.get("/api/debug")
def debug_info():
    return {
        "usando_turso": bool(TURSO_URL),
        "turso_url": TURSO_URL[:40] + "..." if TURSO_URL else "NO CONFIGURADO",
        "token_ok": bool(TURSO_TOKEN),
    }

# ── Startup ────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    try:
        init_db()
    except Exception as e:
        # Logueamos el error pero NO bloqueamos el arranque del servidor
        import traceback
        print(f"[STARTUP WARNING] init_db falló: {e}")
        traceback.print_exc()
