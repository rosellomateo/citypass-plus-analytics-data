"""Capa gold de reclamos.

Timer trigger diario que agrega la tabla completa de silver/Reclamos/
reclamos.parquet por barrio/categoria/prioridad/origenClasificacion/
estado_actual/causaTipo, y guarda el resultado en
gold/Reclamos/reclamos_resumen.parquet.

Dos columnas cruzan con otros modulos (MODELADO_EVENTOS_PARTE2.md 4.1 y 7):
  - afectaOtrosModulos (caso 2): el reclamoId aparece como causaId con
    causaTipo = RECLAMO en silver de Residuos, Movilidad (estaciones),
    Espacios (actividades y espacios afectados) o Emergencias. Es la cuenta
    inversa: "reclamos que afectan a los demas modulos".
  - vinculadoEmergenciaBici (caso 5): el correlationId/causaId del reclamo es
    el emergenciaId de una emergencia durante un viaje en bici (Emergencias
    las marca con causaTipo = MOVILIDAD_BICICLETA). Comparar contra el resto
    de reclamos de via (categoria BACHES/TRANSITO).
Si alguna de esas tablas no se puede leer, la columna queda en False para
todos (se loguea) y el resumen se escribe igual.

A diferencia de silver, aca no hay incremental/checkpoint: como es una
agregacion (GROUP BY), cada corrida la recalcula entera a partir de la tabla
de silver actual. Los domingos, ademas de sobreescribir la tabla principal,
se guarda una copia con la semana ISO en el nombre (snapshot historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from .bp_reclamos_silver import COLUMNA_FECHA_POR_ESTADO, leer_tabla
from .bp_reclamos_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_RECLAMOS_GOLD_BLOB = "Reclamos/reclamos_resumen.parquet"

COLUMNAS_GRANULARIDAD = [
    "barrio", "categoria", "prioridad", "origenClasificacion", "estado_actual", "causaTipo",
    "afectaOtrosModulos", "vinculadoEmergenciaBici",
]

CAUSA_TIPO_EMERGENCIA_BICI = "MOVILIDAD_BICICLETA"


# --- Acceso a storage (gold) -----------------------------------------------

def _get_gold_container_client():
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(GOLD_CONTAINER_NAME)
    try:
        container_client.create_container()
    except ResourceExistsError:
        pass
    return container_client


def _get_gold_blob_client(nombre_blob: str = TABLA_RECLAMOS_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def _fecha_estado_actual(fila: pd.Series):
    """Fecha en la que el reclamo llego a su estado_actual (busca la
    columna fecha_<estado> correspondiente, vía el mismo mapeo que usa
    bp_reclamos_silver para no duplicar la relacion estado -> columna)."""
    columna = COLUMNA_FECHA_POR_ESTADO.get(fila["estado_actual"])
    if columna is None:
        return pd.NaT
    return fila[columna]


def reclamos_referenciados(tablas: list[pd.DataFrame]) -> set[str]:
    """reclamoId que aparecen como causaId (causaTipo = RECLAMO) en las
    tablas de silver de los otros modulos."""
    ids: set[str] = set()
    for df in tablas:
        if df is None or df.empty or "causaId" not in df.columns or "causaTipo" not in df.columns:
            continue
        ids |= set(df.loc[df["causaTipo"] == "RECLAMO", "causaId"].dropna().astype(str))
    return ids


def emergencias_en_bici(df_emergencias: pd.DataFrame | None) -> set[str]:
    """Ids (emergenciaId, correlationId y causaId) de las emergencias
    ocurridas durante un viaje en bici (caso 5)."""
    if df_emergencias is None or df_emergencias.empty or "causaTipo" not in df_emergencias.columns:
        return set()
    bici = df_emergencias[df_emergencias["causaTipo"] == CAUSA_TIPO_EMERGENCIA_BICI]
    ids: set[str] = set()
    for col in ("emergenciaId", "correlationId", "causaId"):
        if col in bici.columns:
            ids |= set(bici[col].dropna().astype(str))
    return ids


def calcular_resumen(
    df_silver: pd.DataFrame,
    fecha_snapshot: datetime | None = None,
    referenciados: set[str] | None = None,
    emergencias_bici: set[str] | None = None,
) -> pd.DataFrame:
    """Una fila por combinacion de barrio/categoria/prioridad/
    origenClasificacion/estado_actual, con la cantidad de reclamos y el
    promedio de horas entre fecha_creado y la fecha en la que llegaron a su
    estado_actual. fecha_snapshot marca el momento en que se calculo este
    resumen (por defecto, ahora); se puede pasar explicito para reprocesos."""
    df = df_silver.copy()
    referenciados = referenciados or set()
    emergencias_bici = emergencias_bici or set()
    df["afectaOtrosModulos"] = df["reclamoId"].astype(str).isin(referenciados)
    vinculo = pd.Series(False, index=df.index)
    for col in ("correlationId", "causaId"):
        if col in df.columns:
            vinculo |= df[col].astype(str).isin(emergencias_bici)
    df["vinculadoEmergenciaBici"] = vinculo
    df["_fecha_estado_actual"] = df.apply(_fecha_estado_actual, axis=1)
    df["_horas_hasta_estado_actual"] = (
        (df["_fecha_estado_actual"] - df["fecha_creado"]).dt.total_seconds() / 3600
    )

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            row_count=("reclamoId", "count"),
            tiempo_prom_hasta_estado_actual=("_horas_hasta_estado_actual", "mean"),
        )
        .reset_index()
    )
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


def _leer_tablas_de_otros_modulos() -> tuple[list[pd.DataFrame], pd.DataFrame | None]:
    """Silver de los modulos que pueden tener causaTipo = RECLAMO, mas el de
    Emergencias (para el caso 5). Una tabla que falla se saltea."""
    from emergencias import bp_emergencias_silver as emergencias
    from espacios import bp_espacios_silver as espacios
    from movilidad import bp_movilidad_silver as movilidad
    from residuos import bp_residuos_silver as residuos

    lectores = {
        "residuos": lambda: residuos.leer_tabla(residuos.get_blob_client()),
        "movilidad estaciones": lambda: movilidad.leer_tabla_estaciones(movilidad.get_blob_client_estaciones()),
        "espacios actividades": lambda: espacios.leer_tabla_actividades(espacios.get_blob_client_actividades()),
        "espacios afectados": lambda: espacios._leer_tabla(
            espacios.TABLA_AFECTADOS_BLOB, espacios.dataframe_vacio_afectados, espacios.COLUMNAS_AFECTADO),
        "emergencias": lambda: emergencias.leer_tabla(emergencias.get_blob_client()),
    }
    tablas: dict[str, pd.DataFrame] = {}
    for nombre, leer in lectores.items():
        try:
            tablas[nombre] = leer()
        except Exception:
            logging.exception(f"No se pudo leer silver de {nombre} para los cruces de reclamos")
    return list(tablas.values()), tablas.get("emergencias")


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Reclamos/reclamos_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_RECLAMOS_GOLD_BLOB) -> None:
    buffer = BytesIO()
    df_resumen.to_parquet(buffer, index=False)
    buffer.seek(0)
    _get_gold_blob_client(nombre_blob).upload_blob(buffer, overwrite=True)


# --- Trigger: corre 1 vez por dia, despues de silver ------------------------

@bp.timer_trigger(
    schedule="0 0 1 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def reclamos_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_reclamos_silver corre a las 00:00, y recalcula el resumen agregado de
    gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de reclamos silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    otras, df_emergencias = _leer_tablas_de_otros_modulos()
    resumen = calcular_resumen(
        df_silver,
        fecha_snapshot=fecha_snapshot,
        referenciados=reclamos_referenciados(otras),
        emergencias_bici=emergencias_en_bici(df_emergencias),
    )
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de reclamos actualizado: {len(resumen)} filas.")
