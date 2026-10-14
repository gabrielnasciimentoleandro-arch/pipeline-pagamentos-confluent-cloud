#!/usr/bin/env python3
"""
consumir_alertas.py — consome o tópico alertas.fraude e imprime os alertas.

Este consumidor usa a identidade de LEITURA (service account sa-leitor): é a
prova de que o pipeline tem duas identidades separadas — quem escreve não lê e
quem lê não escreve.

Lê valores no formato JSON_SR (JSON Schema Registry), por isso busca o schema
no Schema Registry para desserializar.

Exemplos:
  python consumidor/consumir_alertas.py --timeout 30
  python consumidor/consumir_alertas.py                # roda até Ctrl+C

Variáveis de ambiente (veja .env.example):
  BOOTSTRAP_SERVERS KAFKA_API_KEY KAFKA_API_SECRET SR_URL SR_API_KEY SR_API_SECRET
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass


def exigir(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        sys.exit(f"Variável de ambiente obrigatória não definida: {nome} (veja .env.example)")
    return valor


def main() -> int:
    p = argparse.ArgumentParser(description="Consumidor dos alertas de fraude.")
    p.add_argument("--topico", default="alertas.fraude")
    p.add_argument("--timeout", type=float, default=0, help="segundos até encerrar (0 = infinito)")
    p.add_argument("--grupo", default=os.getenv("GRUPO_CONSUMIDOR", "consumidor-alertas-01"))
    p.add_argument("--json", action="store_true", help="imprime o alerta cru em JSON")
    args = p.parse_args()

    sr_client = SchemaRegistryClient(
        {
            "url": exigir("SR_URL"),
            "basic.auth.user.info": f"{exigir('SR_API_KEY')}:{exigir('SR_API_SECRET')}",
        }
    )
    value_deserializer = JSONDeserializer(schema_registry_client=sr_client, from_dict=lambda obj, ctx: obj)

    consumer = Consumer(
        {
            "bootstrap.servers": exigir("BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": exigir("KAFKA_API_KEY"),
            "sasl.password": exigir("KAFKA_API_SECRET"),
            "group.id": args.grupo,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([args.topico])

    print(f"Ouvindo '{args.topico}' como '{args.grupo}'. Ctrl+C para sair.\n", flush=True)
    inicio = time.time()
    total = 0

    try:
        while True:
            if args.timeout and time.time() - inicio >= args.timeout:
                print(f"\nTimeout de {args.timeout}s atingido. {total} alerta(s) recebido(s).")
                return 0

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Erro ao consumir: {msg.error()}", file=sys.stderr)
                continue
            if msg.value() is None:
                # tombstone: o Flink removeu o alerta (a contagem caiu abaixo do limite)
                continue

            alerta = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if alerta is None:
                continue

            total += 1
            if args.json:
                print(json.dumps(alerta, ensure_ascii=False), flush=True)
                continue

            print(
                f"[ALERTA] {alerta.get('tipo_alerta')} | cartao ****{str(alerta.get('cartao_id'))[-4:]}"
                f" | titular {alerta.get('titular')} | {alerta.get('qtd_transacoes')} tx"
                f" | total R$ {alerta.get('valor_total')}"
                f" | janela {alerta.get('janela_inicio')} -> {alerta.get('janela_fim')}"
                f" | cidade da ultima tx: {alerta.get('cidade_ultima')}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.", flush=True)
        return 0
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
