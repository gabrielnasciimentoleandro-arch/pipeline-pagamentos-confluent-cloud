#!/usr/bin/env python3
"""
gerador_transacoes.py — dados sintéticos em tempo real para o Postgres de origem.

O CDC captura qualquer INSERT/UPDATE/DELETE no banco. Este script só escreve no
Postgres; quem transforma isso em evento no Kafka é o conector. Isso é proposital:
prova que o pipeline não depende de "produtor mágico" e sim do CDC.

Modos:
  normal  -> insere 1 transação a cada N segundos (ritmo de operação)
  rajada  -> insere 3 transações no MESMO cartão em poucos segundos (dispara a regra de fraude) e sai
  fraude  -> ritmo normal + uma rajada fraudulenta a cada X segundos (deixa rodando durante a gravação)

Exemplos:
  python scripts/gerador_transacoes.py --modo rajada
  python scripts/gerador_transacoes.py --modo normal --intervalo 2 --duracao 120
  python scripts/gerador_transacoes.py --modo fraude --a-cada 45

Credenciais via variáveis de ambiente (veja .env.example):
  PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD      (ou DATABASE_URL)
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime

import psycopg2

try:  # opcional: carrega .env se python-dotenv estiver instalado
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

# Um cartão fixo por conta: facilita reproduzir a fraude e ler o alerta depois.
CARTOES = {
    1: "4111111111111111",
    2: "4222222222222222",
    3: "4333333333333333",
    4: "4555555555555555",
    5: "4666666666666666",
}

ESTABELECIMENTOS = [
    "Padaria Central", "Mercado Bom Preço", "Posto Ipiranga", "Farmácia Saúde",
    "Restaurante Sabor", "Loja de Roupas", "Streaming Play", "Eletrônicos Online",
    "Cinema Estrela", "Transporte App",
]

CIDADES = [
    "Presidente Prudente", "São Paulo", "Campinas", "Curitiba", "Recife", "Miami",
]


def conectar():
    """Conecta usando DATABASE_URL ou as variáveis PG* padrão do libpq."""
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "pagamentos"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        connect_timeout=10,
    )


def valor_aleatorio() -> float:
    """Maioria das compras pequenas, cauda longa de valores altos."""
    r = random.random()
    if r < 0.70:
        return round(random.uniform(10, 300), 2)
    if r < 0.95:
        return round(random.uniform(300, 1500), 2)
    return round(random.uniform(1500, 5000), 2)


def inserir(cur, conta_id: int, cartao: str, valor: float, cidade: str, estabelecimento: str) -> int:
    cur.execute(
        """
        INSERT INTO public.transacoes
            (conta_id, cartao_id, valor, moeda, estabelecimento, cidade, status)
        VALUES (%s, %s, %s, 'BRL', %s, %s, %s)
        RETURNING id
        """,
        (conta_id, cartao, valor, estabelecimento, cidade, "APROVADA"),
    )
    tx_id = cur.fetchone()[0]
    agora = datetime.now().strftime("%H:%M:%S")
    print(
        f"[{agora}] tx #{tx_id:<5} cartao ****{cartao[-4:]} conta {conta_id} "
        f"R$ {valor:>8.2f} {estabelecimento} ({cidade})",
        flush=True,
    )
    return tx_id


def rajada(conn, conta_id: int, tam: int = 3) -> None:
    """Insere `tam` transações no mesmo cartão em poucos segundos (regra de fraude)."""
    cartao = CARTOES.get(conta_id, "4111111111111111")
    print(f"\n>>> RAJADA: {tam} transações no cartão ****{cartao[-4:]} (conta {conta_id})", flush=True)
    for i in range(tam):
        cidade = random.choice(CIDADES) if i == tam - 1 else "Presidente Prudente"
        with conn.cursor() as cur:
            inserir(cur, conta_id, cartao, valor_aleatorio(), cidade, random.choice(ESTABELECIMENTOS))
        conn.commit()
        if i < tam - 1:
            time.sleep(random.uniform(1.0, 3.0))  # bem dentro dos 60 segundos da janela
    print(">>> fim da rajada\n", flush=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Gerador de transações sintéticas (fonte do CDC).")
    p.add_argument("--modo", choices=["normal", "rajada", "fraude"], default="normal")
    p.add_argument("--intervalo", type=float, default=3.0, help="segundos entre transações no modo normal")
    p.add_argument("--duracao", type=float, default=0, help="segundos de execução (0 = infinito)")
    p.add_argument("--a-cada", type=float, default=45.0, help="no modo fraude, segundos entre rajadas")
    p.add_argument("--conta", type=int, default=1, help="conta usada nas rajadas de fraude")
    p.add_argument("--seed", type=int, default=None, help="semente para reproduzir a mesma sequência")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    conn = conectar()
    conn.autocommit = False
    inicio = time.time()

    try:
        if args.modo == "rajada":
            rajada(conn, args.conta)
            return 0

        proxima_rajada = inicio + args.a_cada if args.modo == "fraude" else None
        print(
            f"Gerando transações (modo={args.modo}, intervalo={args.intervalo}s"
            + (f", rajada a cada {args.a_cada}s" if proxima_rajada else "")
            + "). Ctrl+C para parar.",
            flush=True,
        )

        while True:
            conta = random.randint(1, len(CARTOES))
            with conn.cursor() as cur:
                inserir(
                    cur,
                    conta,
                    CARTOES[conta],
                    valor_aleatorio(),
                    random.choice(CIDADES),
                    random.choice(ESTABELECIMENTOS),
                )
            conn.commit()

            if proxima_rajada and time.time() >= proxima_rajada:
                rajada(conn, args.conta)
                proxima_rajada = time.time() + args.a_cada

            if args.duracao and time.time() - inicio >= args.duracao:
                print("Duração atingida. Encerrando.", flush=True)
                return 0

            # jitter: evita padrão artificial de "metrônomo"
            time.sleep(max(0.2, random.uniform(args.intervalo * 0.5, args.intervalo * 1.5)))
    except KeyboardInterrupt:
        print("\nInterrompido pelo usuário.", flush=True)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
