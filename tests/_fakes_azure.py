"""Piezas falsas compartidas por todos los tests de nivel 3 (mockean Azure
Blob Storage). Un solo lugar para no repetir esto en cada dominio.

La idea: BlobServiceClient (la puerta de entrada a Azure) se reemplaza por
un objeto falso que responde igual (mismos metodos) pero sin red. Cada
contenedor falso guarda lo que el codigo real "subio", para poder revisarlo
despues del test.
"""
from unittest.mock import MagicMock

from azure.core.exceptions import ResourceNotFoundError


class BlobFalso:
    """Lo que Azure devuelve al listar blobs: solo hace falta .name y
    .last_modified."""

    def __init__(self, nombre, last_modified):
        self.name = nombre
        self.last_modified = last_modified


class DescargaFalsa:
    """Lo que devuelve blob_client.download_blob(): el codigo real solo le
    pide .readall()."""

    def __init__(self, contenido_bytes):
        self._contenido = contenido_bytes

    def readall(self):
        return self._contenido


class BlobClientFalso:
    """Un blob puntual (el checkpoint, la tabla de silver, o un resumen de
    gold). Guarda cada upload_blob en `subidas` para poder inspeccionarlo."""

    def __init__(self, contenido_inicial=None):
        self._contenido = contenido_inicial
        self.subidas = []

    def download_blob(self):
        if self._contenido is None:
            raise ResourceNotFoundError("no existe (primera corrida)")
        return DescargaFalsa(self._contenido)

    def upload_blob(self, data, overwrite=True):
        # escribir_tabla/escribir_resumen mandan un BytesIO; escribir_checkpoint manda un str.
        contenido = data.read() if hasattr(data, "read") else data
        self.subidas.append(contenido)
        self._contenido = contenido


class ContenedorBronzeFalso:
    """El contenedor 'bronze': tiene los eventos de prueba que decidimos que
    'ya estan subidos'."""

    def __init__(self, blobs: dict):
        # blobs = {nombre_del_blob: (contenido_bytes, fecha_last_modified)}
        self._blobs = blobs

    def list_blobs(self, name_starts_with=""):
        for nombre, (_, last_modified) in self._blobs.items():
            if nombre.startswith(name_starts_with):
                yield BlobFalso(nombre, last_modified)

    def download_blob(self, nombre):
        contenido, _ = self._blobs[nombre]
        return DescargaFalsa(contenido)


class ContenedorSilverFalso:
    """El contenedor 'silver': devuelve el blob de checkpoint o el de la
    tabla, segun el nombre que se le pida."""

    def __init__(self, checkpoint_previo: str | None = None, tabla_previa: bytes | None = None):
        self.checkpoint_blob = BlobClientFalso(contenido_inicial=checkpoint_previo)
        self.tabla_blob = BlobClientFalso(contenido_inicial=tabla_previa)

    def get_blob_client(self, nombre_blob):
        if nombre_blob.endswith("_checkpoint.json"):
            return self.checkpoint_blob
        return self.tabla_blob


class ContenedorGoldFalso:
    """El contenedor 'gold': cada nombre de blob pedido (el acumulado, o un
    snapshot semanal con la semana en el nombre) tiene su propio blob
    falso, creado la primera vez que se pide."""

    def __init__(self):
        self._blobs: dict[str, BlobClientFalso] = {}

    def create_container(self):
        pass  # el codigo real llama esto "por las dudas"; no hace falta simular nada mas

    def get_blob_client(self, nombre_blob):
        if nombre_blob not in self._blobs:
            self._blobs[nombre_blob] = BlobClientFalso()
        return self._blobs[nombre_blob]


def blob_service_client_falso(**contenedores):
    """El codigo real hace BlobServiceClient.from_connection_string(...).get_container_client(nombre).
    Esto arma el objeto falso que responde eso. Uso: blob_service_client_falso(bronze=..., silver=...)."""
    cliente_falso = MagicMock()
    cliente_falso.get_container_client.side_effect = lambda nombre: contenedores[nombre]
    return cliente_falso
