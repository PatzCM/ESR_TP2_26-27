#!/usr/bin/env python3
"""Demo de routing: no UDP minimo (vizinhos manuais) + Router do Joao.

Testa o modulo de routing sozinho, sem o bootstrapper do Pedro nem o video do
Patz: o servidor envia um "frame" de texto por segundo. Para o no completo usar ott.py.

Uso: python3 no_ott_router.py --id NOME [--port 5000] [--neighbors ID=IP:PORTA ...]
                              [--serve VIDEO] [--watch SERVIDOR/VIDEO] [--ping]
  --serve VIDEO        este no serve a stream "<id>/VIDEO" (pode repetir-se)
  --watch STREAM       este no quer receber STREAM (pode repetir-se)
  --ping               (servidor) envia um PING de 5 em 5 s
  --bind IP            por omissao 0.0.0.0 (so mudar em testes locais)
  --metric delay|hops  metrica de routing (por omissao: atraso medido + 1 ms por salto)

Consola (mesmos comandos do ott.py):
  routes        mostra a tabela de rotas deste no
  ping          servidor: envia um PING pelas rotas ativas
  down <id>     simula a queda de um vizinho
  quit          sair

Exemplo (Cenario 1 no CORE; o router "Node" tambem corre o programa):
  Server : python3 no_ott_router.py --id Server  --neighbors Node=10.0.0.1:5000 --serve video1
  Node   : python3 no_ott_router.py --id Node    --neighbors Server=10.0.0.20:5000 \\
                                     Client1=10.0.1.20:5000 Client2=10.0.2.20:5000
  Client1: python3 no_ott_router.py --id Client1 --neighbors Node=10.0.1.1:5000 --watch Server/video1
  Client2: python3 no_ott_router.py --id Client2 --neighbors Node=10.0.2.1:5000 --watch Server/video1
"""
import argparse
import logging
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "routing"))
from router import Router  # noqa: E402

ap = argparse.ArgumentParser(usage=__doc__)
ap.add_argument("--id", required=True)
ap.add_argument("--port", type=int, default=5000)
ap.add_argument("--neighbors", nargs="*", default=[], metavar="ID=IP:PORTA")
ap.add_argument("--serve", action="append", default=[], metavar="VIDEO")
ap.add_argument("--watch", action="append", default=[], metavar="STREAM")
ap.add_argument("--ping", action="store_true")
ap.add_argument("--bind", default="0.0.0.0")
ap.add_argument("--metric", choices=("delay", "hops"), default="delay")
args = ap.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s",
                    datefmt="%H:%M:%S")

ENDERECOS = {}                        # ID do vizinho -> (ip, porta): o "livro de enderecos"
for item in args.neighbors:
    nid, endereco = item.split("=", 1)
    ip, porta = endereco.rsplit(":", 1)
    ENDERECOS[nid] = (ip, int(porta))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((args.bind, args.port))
lock = threading.RLock()              # o Router nao e thread-safe
a_correr = True


def enviar(destino, dados):
    if destino not in ENDERECOS:
        return
    try:
        sock.sendto(dados, ENDERECOS[destino])
    except OSError as e:
        logging.warning("falha a enviar para %s: %s", destino, e)


def ao_receber_dados(stream, payload):
    print(f"\n[CLIENTE {args.id}] {stream} <- {payload!r}", flush=True)


STREAMS = [f"{args.id}/{v}" for v in args.serve]
router = Router(args.id, enviar, lambda: list(ENDERECOS),
                server_streams=STREAMS, on_data=ao_receber_dados, metric=args.metric)


def receber():
    while a_correr:
        try:
            dados, _ = sock.recvfrom(65535)
        except OSError:
            break
        with lock:
            router.on_receive(dados)


def temporizadores():
    """Timers do Router, um frame de texto por segundo (servidor) e PINGs."""
    prox_frame = prox_ping = 0.0
    n = 0
    while a_correr:
        with lock:
            router.tick()
            agora = time.monotonic()
            if STREAMS and agora >= prox_frame:
                prox_frame, n = agora + 1.0, n + 1
                for s in STREAMS:
                    router.send_data(s, f"frame {n}".encode())
            if args.ping and STREAMS and agora >= prox_ping:
                prox_ping = agora + 5
                for s in STREAMS:
                    router.ping(s)
        time.sleep(0.2)


def main():
    global a_correr
    threading.Thread(target=receber, daemon=True).start()
    threading.Thread(target=temporizadores, daemon=True).start()
    with lock:
        for s in args.watch:
            router.request_stream(s)
    print(f"[{args.id}] porta {args.port} vizinhos={ENDERECOS} serve={STREAMS} watch={args.watch}")
    try:
        while True:
            linha = input("> ").strip()
            if linha == "quit":
                break
            elif linha == "ping":
                with lock:
                    for s in STREAMS:
                        router.ping(s)
            elif linha == "routes":
                with lock:
                    print(router.dump() or "(vazia)")
            elif linha.startswith("down "):
                nid = linha.split()[1]
                if ENDERECOS.pop(nid, None):
                    with lock:
                        router.on_neighbor_down(nid)
            elif linha:
                print("comandos: routes | ping | down <id> | quit")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        a_correr = False
        sock.close()


if __name__ == "__main__":
    main()
