#!/usr/bin/env python3
"""Cliente UDP simples (Etapa 0).
 
Uso: python3 cliente.py <ip_servidor> [porta] [mensagem]
Envia uma mensagem (por omissão "HELLO") e espera a resposta com timeout.
"""
import socket
import sys
 
if len(sys.argv) < 2:
    print("Uso: python3 cliente.py <ip_servidor> [porta] [mensagem]")
    sys.exit(1)
 
SERVER_IP = sys.argv[1]
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
MENSAGEM = sys.argv[3] if len(sys.argv) > 3 else "HELLO"
TIMEOUT = 3.0              # segundos à espera de resposta
BUFFER = 4096
 
 
def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT)
 
    try:
        sock.sendto(MENSAGEM.encode("utf-8"), (SERVER_IP, PORT))
        print(f"[CLIENTE] enviei {MENSAGEM!r} para {SERVER_IP}:{PORT}")
 
        dados, addr = sock.recvfrom(BUFFER)
        print(f"[CLIENTE] resposta de {addr[0]}:{addr[1]} -> {dados.decode('utf-8', errors='replace')!r}")
    except socket.timeout:
        # UDP não garante entrega: é preciso tratar a perda
        print(f"[CLIENTE] sem resposta ao fim de {TIMEOUT}s (pacote perdido ou servidor em baixo)")
    finally:
        sock.close()
 
 
if __name__ == "__main__":
    main()
 