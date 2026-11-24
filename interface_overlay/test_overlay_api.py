#!/usr/bin/env python3
"""Testes do overlay (Pedro) em localhost: campos das mensagens, API para o router,
deteção de quedas. Não precisa do CORE.

Uso (a partir da raiz do repositório):  python3 interface_overlay/test_overlay_api.py -v
O no_ott_mixed.py está em ../Mixed_testing/ e é encontrado a partir da localização deste ficheiro.
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Mixed_testing"))
import no_ott_mixed as ov  # noqa: E402

TOPOLOGIA = {
    "min_degree": 1,
    "nodes": {
        "S": {"ip": "127.0.0.1", "port": 7300},
        "N": {"ip": "127.0.0.1", "port": 7301},
        "C": {"ip": "127.0.0.1", "port": 7302},
    },
    "links": {"S": ["N"], "N": ["C"]},
}


def setUpModule():
    # temporizadores curtos para os testes (os valores reais estão no INTERFACES.md)
    ov.HELLO_INTERVAL = 0.2
    ov.DEAD_TIMEOUT = 1.0
    ov.REREGISTER_INTERVAL = 0.5
    ov.ACTIVE_TIMEOUT = 3.0
    ov.REGISTER_TIMEOUT = 0.5


def esperar(cond, limite=5.0):
    fim = time.time() + limite
    while time.time() < fim:
        if cond():
            return True
        time.sleep(0.05)
    return False


class TesteOverlay(unittest.TestCase):
    def setUp(self):
        self.nos = {}
        self.ficheiro = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump(TOPOLOGIA, self.ficheiro)
        self.ficheiro.close()

    def tearDown(self):
        for n in self.nos.values():
            n.fechar()
        os.unlink(self.ficheiro.name)

    def arrancar(self, nid, bootstrapper=False):
        n = ov.Node(nid, TOPOLOGIA["nodes"][nid]["port"])
        self.nos[nid] = n
        if bootstrapper:
            n.carregar_config(self.ficheiro.name)
        threading.Thread(target=n.receber, daemon=True).start()
        if not bootstrapper:
            n.boot_addr = ("127.0.0.1", TOPOLOGIA["nodes"]["S"]["port"])
            self.assertTrue(n.registo_inicial(), "o bootstrapper não respondeu")
        threading.Thread(target=n.manutencao, daemon=True).start()
        return n

    def rede(self):
        s = self.arrancar("S", bootstrapper=True)
        n = self.arrancar("N")
        c = self.arrancar("C")
        self.assertTrue(esperar(lambda: n.alive_neighbors() == ["C", "S"]))
        self.assertTrue(esperar(lambda: c.alive_neighbors() == ["N"]))
        return s, n, c

    # ---------------------------------------------------------------- mensagens
    def test_campos_das_mensagens(self):
        """REGISTER/NEIGHBORS/ERROR usam from, port, reason, neighbors (INTERFACES.md)."""
        self.arrancar("S", bootstrapper=True)
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2)
        dest = ("127.0.0.1", 7300)

        cli.sendto(json.dumps({"type": "REGISTER", "from": "N", "port": 7301}).encode(), dest)
        r = json.loads(cli.recvfrom(65535)[0])
        self.assertEqual((r["type"], r["from"], r["reason"]), ("NEIGHBORS", "S", "register"))
        self.assertEqual({v["id"] for v in r["neighbors"]}, {"S", "C"})
        self.assertIn("min_degree", r)

        cli.sendto(json.dumps({"type": "REGISTER", "from": "X", "port": 1}).encode(), dest)
        r = json.loads(cli.recvfrom(65535)[0])
        self.assertEqual((r["type"], r["from"]), ("ERROR", "S"))
        self.assertIn("reason", r)

        cli.sendto(json.dumps({"type": "NEED_NEIGHBOR", "from": "N", "exclude": ["S", "C"]}).encode(), dest)
        r = json.loads(cli.recvfrom(65535)[0])
        self.assertEqual((r["type"], r["reason"]), ("NEIGHBORS", "replacement"))
        cli.close()

    def test_mensagem_com_campos_antigos_nao_mata_o_no(self):
        s = self.arrancar("S", bootstrapper=True)
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.sendto(json.dumps({"type": "REGISTER", "id": "N", "porta": 1}).encode(), ("127.0.0.1", 7300))
        cli.close()
        time.sleep(0.3)
        n = self.arrancar("N")                      # o bootstrapper continua a responder
        self.assertTrue(esperar(lambda: "S" in n.alive_neighbors()))

    # ---------------------------------------------------------------- API
    def test_vizinhos_ativos_e_send(self):
        s, n, c = self.rede()
        self.assertEqual(s.alive_neighbors(), ["N"])
        self.assertFalse(n.send("Desconhecido", b"x"))
        self.assertTrue(n.send("C", json.dumps({"type": "MSG", "from": "N", "text": "ola"}).encode()))

    def test_router_recebe_dados_e_mensagens_de_routing(self):
        s, n, c = self.rede()
        recebidos = []
        lock = threading.RLock()

        def on_receive(data):
            self.assertTrue(lock._is_owned(), "on_receive tem de ser chamada com o lock do nó")
            recebidos.append(data)

        c.attach_router(on_receive, {"ANNOUNCE", "ACTIVATE"}, lock)
        pacote = b"DATA|Server1/video1|\xff\xd8\x00\x01" + bytes(range(256)) * 5   # binário, > 1 KB
        anuncio = json.dumps({"type": "ANNOUNCE", "from": "N", "stream": "S/v", "server": "S",
                              "metric": 2.3, "seq": 1}).encode()
        self.assertTrue(n.send("C", pacote))
        self.assertTrue(n.send("C", anuncio))
        self.assertTrue(esperar(lambda: len(recebidos) == 2))
        self.assertEqual(recebidos[0], pacote)           # chega intacto
        self.assertEqual(recebidos[1], anuncio)
        # HELLO continua a ser do overlay, não vai ao router
        time.sleep(0.5)
        self.assertEqual(len(recebidos), 2)

    def test_sem_router_ligado_dados_sao_ignorados(self):
        s, n, c = self.rede()
        n.send("C", b"DATA|x|yy")
        n.send("C", json.dumps({"type": "ANNOUNCE", "from": "N"}).encode())
        time.sleep(0.4)
        self.assertEqual(c.alive_neighbors(), ["N"])       # o nó não morreu

    # ---------------------------------------------------------------- quedas
    def test_queda_por_falta_de_hello(self):
        s, n, c = self.rede()
        quedas = []
        n.add_down_listener(quedas.append)
        c.fechar()                                          # morre sem enviar LEAVE
        self.assertTrue(esperar(lambda: quedas == ["C"], 4))
        self.assertEqual(n.alive_neighbors(), ["S"])

    def test_queda_por_leave(self):
        s, n, c = self.rede()
        quedas = []
        n.add_down_listener(quedas.append)
        t0 = time.time()
        c.terminar()
        self.assertTrue(esperar(lambda: quedas == ["C"], 2))
        self.assertLess(time.time() - t0, ov.DEAD_TIMEOUT)  # mais rápido do que o timeout

    def test_ouvinte_que_falha_nao_para_o_overlay(self):
        s, n, c = self.rede()
        quedas = []

        def mau(nid):
            raise RuntimeError("erro de propósito")

        n.add_down_listener(mau)
        n.add_down_listener(quedas.append)
        c.terminar()
        self.assertTrue(esperar(lambda: quedas == ["C"], 2))

    def test_queda_so_e_notificada_uma_vez(self):
        s, n, c = self.rede()
        quedas = []
        n.add_down_listener(quedas.append)
        c.terminar()
        time.sleep(2.0)
        self.assertEqual(quedas, ["C"])


if __name__ == "__main__":
    unittest.main()
