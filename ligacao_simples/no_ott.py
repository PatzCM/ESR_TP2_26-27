#!/usr/bin/env python3
"""Nó simultaneamente cliente e servidor, em UDP (full-duplex) - base da Etapa 1.

Uso: python3 no_ott.py <porta_local> [ip_vizinho:porta_vizinho ...]
Exemplo:
    no A:  python3 no_ott.py 5000 10.0.1.2:5000
    no B:  python3 no_ott.py 5000 10.0.0.1:5000

Uma thread recebe mensagens; o programa principal lê do teclado e envia
"HELLO" (ou texto escrito) a todos os vizinhos indicados.
"""
import socket
import sys
import threading

BUFFER = 4096

if len(sys.argv) < 2:
    print(__doc__)
    sys.exit(1)

PORTA = int(sys.argv[1])
VIZINHOS = []
for v in sys.argv[2:]:
    ip, porta = v.rsplit(":", 1)
    VIZINHOS.append((ip, int(porta)))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", PORTA))
a_correr = True


def receber():
    """Thread de receção: imprime tudo o que chega e responde aos HELLO."""
    while a_correr:
        try:
            dados, addr = sock.recvfrom(BUFFER)
        except OSError:
            break  # socket fechado
        msg = dados.decode("utf-8", errors="replace")
        print(f"\n[RECV] de {addr[0]}:{addr[1]} -> {msg!r}")
        if msg == "HELLO":
            sock.sendto(b"HELLO_ACK", addr)


def main():
    global a_correr
    threading.Thread(target=receber, daemon=True).start()
    print(f"[NÓ] à escuta em UDP porta {PORTA}; vizinhos: {VIZINHOS}")
    print("Escreve uma mensagem e Enter (vazio = HELLO, 'sair' termina).")

    try:
        while True:
            texto = input("> ").strip() or "HELLO"
            if texto.lower() == "sair":
                break
            for destino in VIZINHOS:
                sock.sendto(texto.encode("utf-8"), destino)
                print(f"[SEND] {texto!r} -> {destino[0]}:{destino[1]}")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        a_correr = False
        sock.close()
        print("\n[NÓ] terminado.")


if __name__ == "__main__":
    main()
