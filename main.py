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

# ── Database driver ────────────────────────────────────────────────────
# En producción (Render) se usan variables de entorno TURSO_DATABASE_URL
# y TURSO_AUTH_TOKEN → SQLite persistente en la nube (gratis, nunca se borra).
# En desarrollo local cae al archivo check.db.
TURSO_URL   = os.environ.get("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")

if TURSO_URL:
    import libsql_experimental as _sql   # drop-in idéntico a sqlite3
    def _connect():
        return _sql.connect(TURSO_URL, auth_token=TURSO_TOKEN)
else:
    _sql = sqlite3                        # type: ignore[assignment]
    def _connect():
        con = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        return con

@contextmanager
def get_db():
    con = _connect()
    con.row_factory = _sql.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    with get_db() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS rutinas (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                turno       TEXT    NOT NULL CHECK(turno IN ('AM','PM')),
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
                rutina_id   INTEGER NOT NULL REFERENCES rutinas(id),
                done        INTEGER NOT NULL DEFAULT 0,
                obs         TEXT    DEFAULT '',
                ts          TEXT    DEFAULT '',
                UNIQUE(local, fecha, rutina_id)
            );

            CREATE INDEX IF NOT EXISTS idx_reg_local_fecha
                ON registros(local, fecha);
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


# ── Startup ────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    init_db()
