#!/usr/bin/env python3
"""no_ott do Pedro (UDP, vizinhos manuais) + Router de routing do Joao.

Uso: python3 no_ott_router.py <porta> <meu_ip> [ip_viz:porta ...] [opcoes]
  --servidor NOME      este no serve a stream "<meu_ip>/NOME" (envia 1 frame/s)
  --cliente IP/NOME    este no quer receber essa stream
  --ping               (servidor) envia PING de 5 em 5 s
  --bind IP            por omissao 0.0.0.0 (so mudar em testes locais)

No terminal do no:
  <Enter>     envia HELLO aos vizinhos (teste do Pedro)
  ping        servidor: envia um PING pelas rotas ativas
  tabela      mostra a tabela de rotas deste no
  cai <ip>    simula a queda de um vizinho
  sair

Exemplo (Cenario 1, PCs do CORE; o router "node" tambem corre o programa):
  Server : python3 no_ott_router.py 5000 10.0.0.20 10.0.0.1:5000 --servidor video1
  node   : python3 no_ott_router.py 5000 10.0.0.1 10.0.0.20:5000 10.0.1.20:5000 10.0.2.20:5000
  Client1: python3 no_ott_router.py 5000 10.0.1.20 10.0.1.1:5000 --cliente 10.0.0.20/video1
  Client2: python3 no_ott_router.py 5000 10.0.2.20 10.0.2.1:5000 --cliente 10.0.0.20/video1
"""
import argparse
import logging
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from router import Router  # noqa: E402

BUFFER = 65535

ap = argparse.ArgumentParser(usage=__doc__)
ap.add_argument("porta", type=int)
ap.add_argument("meu_ip")
ap.add_argument("vizinhos", nargs="*", help="ip:porta")
ap.add_argument("--servidor", action="append", default=[], metavar="NOME")
ap.add_argument("--cliente", action="append", default=[], metavar="IP/NOME")
ap.add_argument("--ping", action="store_true")
ap.add_argument("--bind", default="0.0.0.0")
args = ap.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s",
                    datefmt="%H:%M:%S")

DESTINOS = {}                         # id do vizinho (IP) -> (ip, porta)
for v in args.vizinhos:
    ip, porta = v.rsplit(":", 1)
    DESTINOS[ip] = (ip, int(porta))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.bind, args.porta))
lock = threading.RLock()              # o Router nao e thread-safe
a_correr = True


def enviar(ip, dados):
    try:
        sock.sendto(dados, DESTINOS.get(ip, (ip, args.porta)))
    except OSError as e:
        logging.warning("falha a enviar para %s: %s", ip, e)


def on_dados(stream, payload):
    print(f"\n[CLIENTE {args.meu_ip}] {stream} <- {payload!r}", flush=True)


STREAMS = [f"{args.meu_ip}/{n}" for n in args.servidor]
router = Router(args.meu_ip, enviar, lambda: list(DESTINOS),
                streams_servidor=STREAMS, on_dados=on_dados)


def receber():
    """Thread de rececao: HELLO e do Pedro; o resto e routing."""
    while a_correr:
        try:
            dados, addr = sock.recvfrom(BUFFER)
        except OSError:
            break
        if dados == b"HELLO":
            print(f"\n[RECV] HELLO de {addr[0]}:{addr[1]}")
            sock.sendto(b"HELLO_ACK", addr)
        elif dados == b"HELLO_ACK":
            print(f"\n[RECV] HELLO_ACK de {addr[0]}:{addr[1]}")
        else:
            with lock:
                router.on_receber(dados)


def ciclo():
    """Thread de tempo: timers do Router, frames (servidor) e PINGs."""
    prox_frame = prox_ping = 0.0
    n = 0
    while a_correr:
        with lock:
            router.tick()
            agora = time.monotonic()
            if STREAMS and agora >= prox_frame:
                prox_frame, n = agora + 1.0, n + 1
                for s in STREAMS:
                    router.enviar_dados(s, f"frame {n}".encode())
            if args.ping and STREAMS and agora >= prox_ping:
                prox_ping = agora + 5
                for s in STREAMS:
                    router.ping(s)
        time.sleep(0.2)


def main():
    global a_correr
    threading.Thread(target=receber, daemon=True).start()
    threading.Thread(target=ciclo, daemon=True).start()
    with lock:
        for s in args.cliente:
            router.pedir_stream(s)
    print(f"[NO {args.meu_ip}] porta {args.porta} vizinhos={list(DESTINOS.values())} "
          f"servidor={STREAMS} cliente={args.cliente}")
    try:
        while True:
            texto = input("> ").strip()
            if texto.lower() == "sair":
                break
            if texto == "ping":
                with lock:
                    for s in STREAMS:
                        router.ping(s)
            elif texto == "tabela":
                with lock:
                    print(router.t.dump() or "(vazia)")
            elif texto.startswith("cai "):
                ip = texto.split()[1]
                if ip in DESTINOS:
                    del DESTINOS[ip]
                    with lock:
                        router.on_vizinho_caiu(ip)
            else:                      # comportamento original do Pedro
                for d in DESTINOS.values():
                    sock.sendto((texto or "HELLO").encode(), d)
                    print(f"[SEND] {(texto or 'HELLO')!r} -> {d[0]}:{d[1]}")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        a_correr = False
        sock.close()
        print("\n[NO] terminado.")


if __name__ == "__main__":
    main()
