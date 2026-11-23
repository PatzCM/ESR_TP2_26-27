"""Testes sem rede real: topologias simuladas em memoria (Cenario 2 e Cenario final).
Correr (dentro de routing/):  python3 -m unittest -v test_routing
"""
import heapq
import itertools
import json
import os
import unittest
from collections import Counter

from router import Router

S1 = "S/v1"


class Rede:
    def __init__(self, links, servidores=None, streams=None, metric="delay"):
        self.t = 0.0
        self.links = {frozenset(l) for l in links}
        self.atraso = {}                       # frozenset(link) -> atraso num sentido (s)
        self.fila = []                         # heap (hora_entrega, n, destino, bytes)
        self._n = itertools.count()
        self.dados_enviados = Counter()
        self.recebidos = {}
        ids = {x for l in links for x in l}
        self.nos = {}
        for i in ids:
            self.recebidos[i] = []
            self.nos[i] = Router(
                i, (lambda d, b, i=i: self._env(i, d, b)), (lambda i=i: self._viz(i)),
                server_streams=(streams or {}).get(i, ()),
                on_data=(lambda s, p, i=i: self.recebidos[i].append(p)),
                clock=lambda: self.t, metric=metric)

    def _viz(self, i):
        return [x for l in self.links for x in l if i in l and x != i]

    def _env(self, src, dst, b):
        if frozenset((src, dst)) in self.links:
            if b.startswith(b"DATA|"):
                self.dados_enviados[(src, dst)] += 1
            hora = self.t + self.atraso.get(frozenset((src, dst)), 0.0)
            heapq.heappush(self.fila, (hora, next(self._n), dst, b))

    def drenar(self):
        """Entrega tudo o que ja chegou (hora de entrega <= agora)."""
        while self.fila and self.fila[0][0] <= self.t:
            _, _, dst, b = heapq.heappop(self.fila)
            self.nos[dst].on_receive(b)

    def correr(self, segundos, dt=0.005):
        """Avanca o tempo em passos pequenos (para medir atrasos)."""
        for _ in range(int(round(segundos / dt))):
            self.t += dt
            for r in self.nos.values():
                r.tick()
            self.drenar()

    def passo(self, dt=1.0, n=1):
        for _ in range(n):
            self.t += dt
            for r in self.nos.values():
                r.tick()
            self.drenar()

    def repor(self, a, b):
        self.links.add(frozenset((a, b)))

    def falha(self, a, b):
        self.links.discard(frozenset((a, b)))
        self.nos[a].on_neighbor_down(b)
        self.nos[b].on_neighbor_down(a)
        self.drenar()


def cenario2():
    # S - N1 ; N1 - N2 ; N1 - N3 ; N2 - N3 ; N2 - C1 ; N3 - C2
    links = [("S", "N1"), ("N1", "N2"), ("N1", "N3"), ("N2", "N3"), ("N2", "C1"), ("N3", "C2")]
    return Rede(links, streams={"S": [S1]})


class TestRouting(unittest.TestCase):
    def test_metricas_por_saltos(self):
        r = cenario2()
        r.passo(1, 3)
        m = {n: r.nos[n].t.obter(S1).metrica for n in ("N1", "N2", "N3", "C1", "C2")}
        self.assertEqual(m, {"N1": 1, "N2": 2, "N3": 2, "C1": 3, "C2": 3})

    def test_rotas_inativas_nao_geram_trafego(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["S"].send_data(S1, b"x")
        r.drenar()
        self.assertEqual(sum(r.dados_enviados.values()), 0)

    def test_replicacao_sem_duplicados(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.nos["C2"].request_stream(S1)
        r.drenar()
        r.nos["S"].send_data(S1, b"frame1")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [b"frame1"])
        self.assertEqual(r.recebidos["C2"], [b"frame1"])
        self.assertEqual(r.dados_enviados[("S", "N1")], 1)   # 1 so copia no troco comum
        self.assertEqual(r.dados_enviados[("N1", "N2")], 1)
        self.assertEqual(r.dados_enviados[("N1", "N3")], 1)
        self.assertEqual(r.dados_enviados[("N2", "N3")], 0)  # link redundante nao usado

    def test_desativar_cliente(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.nos["C2"].request_stream(S1)
        r.drenar()
        r.nos["C1"].stop_stream(S1)
        r.drenar()
        r.nos["S"].send_data(S1, b"f")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [])
        self.assertEqual(r.recebidos["C2"], [b"f"])
        self.assertEqual(r.dados_enviados[("N1", "N2")], 0)

    def test_failover_rota_alternativa(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.nos["C2"].request_stream(S1)
        r.drenar()
        r.falha("N1", "N2")                     # caminho de C1 cai
        r.nos["S"].send_data(S1, b"depois")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [b"depois"])   # via N1-N3-N2
        self.assertEqual(r.recebidos["C2"], [b"depois"])
        self.assertEqual(r.dados_enviados[("N3", "N2")], 1)

    def test_soft_state_expira_sem_refresh(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.drenar()
        r.nos["C1"].tick = lambda: None         # cliente morre sem avisar
        r.nos["C1"].on_receive = lambda b: None
        r.passo(1, 12)
        self.assertFalse(r.nos["N2"].t.obter(S1).ativa)

    def test_ping_regista_caminho(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.drenar()
        with self.assertLogs("router[C1]", level="INFO") as cm:
            r.nos["S"].ping(S1)
            r.drenar()
        self.assertIn("['S', 'N1', 'N2', 'C1']", cm.output[0])


    def test_anuncio_nao_volta_pela_origem_nem_repete(self):
        r = cenario2()
        enviados = []
        original = r._env
        r._env = lambda s, d, b: (enviados.append((s, d, json.loads(b)["type"])), original(s, d, b))
        for n in r.nos.values():                 # o Router guardou a lambda antiga: refazer
            n._send = (lambda d, b, i=n.id: r._env(i, d, b))
        r.passo(1, 1)                            # uma ronda de ANNOUNCE
        anuncios = [(s, d) for s, d, t in enviados if t == "ANNOUNCE"]
        self.assertNotIn(("N1", "S"), anuncios)              # nao volta pela origem
        self.assertEqual(len(anuncios), len(set(anuncios)))  # nada enviado 2x pelo mesmo link

    def test_dados_de_quem_nao_e_origem_sao_descartados(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.drenar()
        # N3 nao e a origem de N2 para S1: um pacote vindo dai seria um ciclo
        r.nos["N2"].on_receive(b"DATA|" + S1.encode() + b"|N3|loop")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [])
        self.assertEqual(r.nos["N2"].stats["data_dropped"], 1)

    def test_volta_ao_caminho_curto_quando_link_regressa(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.drenar()
        r.falha("N1", "N2")
        r.passo(1, 6)
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N3")
        self.assertEqual(r.nos["C1"].t.obter(S1).metrica, 4)
        r.repor("N1", "N2")
        r.passo(1, 6)                            # proxima ronda de ANNOUNCE recalcula
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N1")
        self.assertEqual(r.nos["C1"].t.obter(S1).metrica, 3)
        r.nos["S"].send_data(S1, b"f")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [b"f"])
        self.assertEqual(r.dados_enviados[("N3", "N2")], 0)  # N3 deixou de ser usado
        r.passo(1, 10)                           # o destino antigo em N3 expira (soft state)
        self.assertFalse(r.nos["N3"].t.obter(S1).ativa)


def cenario_final():
    """Topologia de Mixed_testing/cenarioF.json. Como na figura 1 do enunciado, S1 = n16
    e os clientes sao n17, n18, n19, n22, n23; o 2.o servidor (n20) e escolha nossa.
    Cada servidor tem 2 streams."""
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Mixed_testing", "cenarioF.json")
    with open(p) as f:
        cfg = json.load(f)
    links = [(a, b) for a, vs in cfg["links"].items() for b in vs]
    streams = {"n16": ["n16/video1", "n16/video2"], "n20": ["n20/video1", "n20/video2"]}
    return Rede(links, streams=streams)


class TestVariosServidores(unittest.TestCase):
    def test_tabelas_distinguem_cada_fluxo(self):
        r = cenario_final()
        r.passo(1, 3)
        for n in ("n5", "n10", "n23"):
            self.assertEqual(sorted(r.nos[n].t.rotas),
                             ["n16/video1", "n16/video2", "n20/video1", "n20/video2"])
        self.assertEqual(r.nos["n10"].t.obter("n16/video1").metrica, 1)
        self.assertEqual(r.nos["n1"].t.obter("n20/video2").metrica, 1)

    def test_clientes_com_streams_diferentes_em_simultaneo(self):
        r = cenario_final()
        r.passo(1, 3)
        pedidos = {"n17": "n16/video1", "n18": "n16/video2", "n19": "n20/video1",
                   "n22": "n16/video1", "n23": "n20/video1"}
        for c, s in pedidos.items():
            r.nos[c].request_stream(s)
        r.drenar()
        for s in ("n16/video1", "n16/video2", "n20/video1", "n20/video2"):
            r.nos[s.split("/")[0]].send_data(s, s.encode())
        r.drenar()
        for c, s in pedidos.items():
            self.assertEqual(r.recebidos[c], [s.encode()], c)
        # ninguem pediu n20/video2: nao ha trafego dessa stream
        self.assertFalse(r.nos["n1"].t.obter("n20/video2").ativa)
        # cada link leva no maximo 1 copia de cada stream
        total = sum(r.dados_enviados.values())
        self.assertLessEqual(max(r.dados_enviados.values()), 3)
        self.assertGreater(total, 0)

    def test_failover_no_cenario_final(self):
        r = cenario_final()
        r.passo(1, 3)
        r.nos["n23"].request_stream("n16/video1")
        r.drenar()
        path = []
        with self.assertLogs("router[n23]", level="INFO") as cm:
            r.nos["n16"].ping("n16/video1")
            r.drenar()
        path = [l for l in cm.output if "PING" in l][0]
        self.assertIn("'n10', 'n9', 'n5', 'n23'", path)      # caminho mais curto
        r.falha("n9", "n5")
        r.nos["n16"].send_data("n16/video1", b"depois")
        r.drenar()
        self.assertEqual(r.recebidos["n23"], [b"depois"])    # via n9-n6-n5



class TestMetricaAtraso(unittest.TestCase):
    """Etapa 2 / requisito "metricas da rede": custo = atraso medido + 1 ms por salto."""

    def test_sem_atraso_equivale_a_saltos(self):
        r = cenario2()
        r.correr(3)
        self.assertAlmostEqual(r.nos["C1"].t.obter(S1).metrica, 3, places=1)

    def test_mede_atraso_de_cada_ligacao(self):
        r = cenario2()
        r.atraso[frozenset(("N1", "N2"))] = 0.040           # 40 ms
        r.correr(4)
        # o simulador entrega em passos de 5 ms, por isso a medida sai por excesso
        self.assertAlmostEqual(r.nos["N2"].link_delay("N1"), 40, delta=6)
        self.assertAlmostEqual(r.nos["N2"].link_delay("N3"), 0, delta=2)
        self.assertIn("N1=", r.nos["N2"].dump())

    def test_evita_ligacao_lenta(self):
        r = cenario2()
        r.atraso[frozenset(("N1", "N2"))] = 0.050           # caminho curto mas lento
        r.correr(12)                                         # medir + 2 rondas de ANNOUNCE
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N3")   # S-N1-N3-N2-C1
        r.nos["C1"].request_stream(S1)
        r.correr(0.5)
        r.nos["S"].send_data(S1, b"f")
        r.correr(0.5)
        self.assertEqual(r.recebidos["C1"], [b"f"])
        self.assertEqual(r.dados_enviados[("N1", "N2")], 0)

    def test_reage_a_mudanca_das_condicoes(self):
        r = cenario2()
        r.nos["C1"].request_stream(S1)
        r.correr(8)
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N1")
        r.atraso[frozenset(("N1", "N2"))] = 0.050           # a ligacao fica lenta a meio
        r.correr(15)
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N3")
        self.assertEqual(r.nos["N2"].t.obter(S1).upstream, "N3")  # ACTIVATE seguiu o novo caminho
        del r.atraso[frozenset(("N1", "N2"))]                # volta ao normal
        r.correr(20)
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N1")

    def test_histerese_ignora_diferencas_pequenas(self):
        r = cenario2()
        r.correr(8)
        rota = r.nos["N2"].t.obter(S1)
        self.assertEqual(rota.origem, "N1")
        # N3 passa a parecer 0,1 ms melhor (ruido): nao deve trocar
        rota.candidatos["N3"] = (rota.metrica - 0.1, r.t)
        r.nos["N2"].t._escolher(rota)
        self.assertEqual(rota.origem, "N1")

    def test_modo_saltos_continua_disponivel(self):
        r = Rede([("S", "N1"), ("N1", "C")], streams={"S": [S1]}, metric="hops")
        r.passo(1, 3)
        self.assertEqual(r.nos["C"].t.obter(S1).metrica, 2)
        self.assertIsInstance(r.nos["C"].t.obter(S1).metrica, int)


if __name__ == "__main__":
    unittest.main()
