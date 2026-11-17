"""Consumidor de Kafka -> bronze, mockeando Kafka y Azure.

consumir_lote recibe el consumer, el deserializador y el contenedor por
parametro, asi que aca se le pasan versiones falsas: no hay red ni broker.
Lo que importa probar es el orden leer -> guardar -> commitear y que nunca
se commitee algo que no quedo guardado.
"""
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import bronze.bp_kafka_ingestion_bronze as kafka_mod
from tests._fakes_azure import ContenedorBronzeEscribibleFalso


class MensajeFalso:
    def __init__(self, valor, topic="com.citypass.reclamos.ReclamoCreado", offset=0, error=None):
        self._valor = valor
        self._topic = topic
        self._offset = offset
        self._error = error

    def value(self):
        return self._valor

    def topic(self):
        return self._topic

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class ConsumerFalso:
    """Devuelve los mensajes en orden y despues None para siempre (= no hay
    mas nada). Anota cada commit."""

    def __init__(self, mensajes, asignado=True):
        self._mensajes = list(mensajes)
        self._asignado = asignado
        self.commits = []

    def poll(self, timeout):
        return self._mensajes.pop(0) if self._mensajes else None

    def assignment(self):
        return ["particion-0"] if self._asignado else []

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.commits.append(message.offset())


class RelojFalso:
    """Cada lectura avanza 1 segundo, asi los loops con timeout terminan
    rapido y de forma determinista."""

    def __init__(self):
        self.t = 0

    def __call__(self):
        self.t += 1
        return self.t


def _evento(event_id, received_at=1786547143000, **data):
    return {
        "metadata": {
            "eventId": event_id,
            "eventType": "com.citypass.reclamos.ReclamoCreado",
            "receivedAt": received_at,
        },
        "data": data,
    }


def _deserializar_identidad(valor, ctx):
    # Los mensajes falsos ya traen el dict "deserializado".
    return valor


def test_guarda_cada_evento_en_bronze_y_commitea_despues():
    bronze = ContenedorBronzeEscribibleFalso()
    consumer = ConsumerFalso([
        MensajeFalso(_evento("aaaaaaaa-1", reclamoId="R-1"), offset=10),
        MensajeFalso(_evento("bbbbbbbb-2", reclamoId="R-2"), offset=11),
    ])

    stats = kafka_mod.consumir_lote(consumer, _deserializar_identidad, bronze, reloj=RelojFalso())

    assert stats == {"guardados": 2, "duplicados": 0, "no_deserializables": 0}
    assert consumer.commits == [10, 11]
    nombres = [nombre for nombre, _ in bronze.subidas]
    # El nombre sale de receivedAt + eventId, no de la hora de ingesta.
    assert nombres == [
        "Reclamos/ReclamoCreado/ReclamoCreado_20260812_150543_aaaaaaaa.json",
        "Reclamos/ReclamoCreado/ReclamoCreado_20260812_150543_bbbbbbbb.json",
    ]


def test_evento_repetido_no_se_duplica_en_bronze_pero_se_commitea():
    bronze = ContenedorBronzeEscribibleFalso()
    evento = _evento("aaaaaaaa-1", reclamoId="R-1")
    consumer = ConsumerFalso([MensajeFalso(evento, offset=1), MensajeFalso(evento, offset=2)])

    stats = kafka_mod.consumir_lote(consumer, _deserializar_identidad, bronze, reloj=RelojFalso())

    assert stats["guardados"] == 1
    assert stats["duplicados"] == 1
    assert len(bronze.subidas) == 1
    assert consumer.commits == [1, 2]


def test_si_falla_guardar_en_bronze_no_se_commitea():
    class BronzeRoto:
        def upload_blob(self, name, data, overwrite=False):
            raise ConnectionError("storage caido")

    consumer = ConsumerFalso([MensajeFalso(_evento("aaaaaaaa-1"), offset=5)])

    with pytest.raises(ConnectionError):
        kafka_mod.consumir_lote(consumer, _deserializar_identidad, BronzeRoto(), reloj=RelojFalso())

    assert consumer.commits == []


def test_mensaje_no_deserializable_se_aparta_y_no_traba_la_particion():
    bronze = ContenedorBronzeEscribibleFalso()

    def deserializar_que_falla(valor, ctx):
        raise ValueError("magic byte invalido")

    consumer = ConsumerFalso([MensajeFalso(b"basura", offset=7)])

    stats = kafka_mod.consumir_lote(consumer, deserializar_que_falla, bronze, reloj=RelojFalso())

    assert stats["no_deserializables"] == 1
    assert consumer.commits == [7]
    nombre, contenido = bronze.subidas[0]
    assert nombre == "_no_deserializables/com.citypass.reclamos.ReclamoCreado/0_7.avro"
    assert contenido == b"basura"


def test_errores_de_kafka_se_saltean_sin_commitear():
    bronze = ContenedorBronzeEscribibleFalso()
    consumer = ConsumerFalso([MensajeFalso(None, error="UNKNOWN_TOPIC_OR_PART")])

    stats = kafka_mod.consumir_lote(consumer, _deserializar_identidad, bronze, reloj=RelojFalso())

    assert stats["guardados"] == 0
    assert consumer.commits == []


def test_sin_particiones_asignadas_corta_por_tiempo():
    consumer = ConsumerFalso([], asignado=False)
    reloj = RelojFalso()

    kafka_mod.consumir_lote(consumer, _deserializar_identidad, ContenedorBronzeEscribibleFalso(), reloj=reloj)

    assert reloj.t < kafka_mod.TIEMPO_MAXIMO_SEGUNDOS


def test_tipos_avro_se_guardan_como_el_json_del_gateway():
    evento = _evento(
        "aaaaaaaa-1",
        createdAt=datetime(2026, 8, 14, 0, 25, 43, tzinfo=timezone.utc),
        monto=Decimal("12.34"),
        foto=b"\x00\x01",
    )
    bronze = ContenedorBronzeEscribibleFalso()
    consumer = ConsumerFalso([MensajeFalso(evento)])

    kafka_mod.consumir_lote(consumer, _deserializar_identidad, bronze, reloj=RelojFalso())

    data = json.loads(bronze.subidas[0][1])["data"]
    assert data == {"createdAt": 1786667143000, "monto": 12.34, "foto": "AAE="}


def test_config_del_consumer_sale_de_las_variables_de_entorno(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_CLIENT_SECRET", "secreto")
    monkeypatch.delenv("KAFKA_GROUP_ID", raising=False)

    config = kafka_mod.armar_config_consumer()

    assert config["bootstrap.servers"] == "broker:9092"
    assert config["enable.auto.commit"] is False
    assert config["group.id"].startswith("com.citypass.analitica")
    assert config["sasl.oauthbearer.client.id"] == "grupo8"


def test_config_indica_donde_estan_los_certificados(monkeypatch):
    """Sin esto, en Linux (Azure) la libreria busca los certificados en
    /etc/pki/... y no consigue el token de Login Federado."""
    import os
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KAFKA_CLIENT_SECRET", "secreto")

    config = kafka_mod.armar_config_consumer()

    assert os.path.isfile(config["https.ca.location"])
    assert os.path.isfile(config["ssl.ca.location"])


def test_config_falla_si_falta_el_secreto(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.delenv("KAFKA_CLIENT_SECRET", raising=False)

    with pytest.raises(KeyError):
        kafka_mod.armar_config_consumer()


def test_topicos_se_pueden_pisar_por_variable(monkeypatch):
    monkeypatch.setenv("KAFKA_TOPICS", " com.citypass.a.X , com.citypass.b.Y ")
    assert kafka_mod.leer_topicos() == ["com.citypass.a.X", "com.citypass.b.Y"]

    monkeypatch.delenv("KAFKA_TOPICS")
    assert kafka_mod.leer_topicos() == kafka_mod.TOPICOS_DEFAULT
