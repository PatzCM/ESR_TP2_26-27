#!/usr/bin/env python3
"""Servidor UDP simples (Etapa 0).
 
Uso: python3 servidor.py [porta]
Fica à escuta e responde a cada mensagem recebida (eco com prefixo).
"""
import socket
import sys
 
HOST = "0.0.0.0"           # escuta em todas as interfaces (necessário no CORE)
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
BUFFER = 4096              # tamanho máximo de datagrama lido
 
 
def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    print(f"[SERVIDOR] à escuta em UDP {HOST}:{PORT} (Ctrl+C para sair)")
 
    try:
        while True:
            dados, addr = sock.recvfrom(BUFFER)
            msg = dados.decode("utf-8", errors="replace")
            print(f"[SERVIDOR] recebi de {addr[0]}:{addr[1]} -> {msg!r}")
 
            resposta = f"ACK: {msg}"
            sock.sendto(resposta.encode("utf-8"), addr)
    except KeyboardInterrupt:
        print("\n[SERVIDOR] a terminar.")
    finally:
        sock.close()
 
 
if __name__ == "__main__":
    main()