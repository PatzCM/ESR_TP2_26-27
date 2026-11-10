"""Testes sem rede real: topologia do Cenario 2 simulada em memoria.
Correr:  python3 -m unittest -v test_routing
"""
import unittest
from collections import Counter, deque

from router import Router

S1 = "S/v1"


class Rede:
    def __init__(self, links, servidores=None, streams=None):
        self.t = 0.0
        self.links = {frozenset(l) for l in links}
        self.fila = deque()
        self.dados_enviados = Counter()
        self.recebidos = {}
        ids = {x for l in links for x in l}
        self.nos = {}
        for i in ids:
            self.recebidos[i] = []
            self.nos[i] = Router(
                i, (lambda d, b, i=i: self._env(i, d, b)), (lambda i=i: self._viz(i)),
                streams_servidor=(streams or {}).get(i, ()),
                on_dados=(lambda s, p, i=i: self.recebidos[i].append(p)),
                relogio=lambda: self.t)

    def _viz(self, i):
        return [x for l in self.links for x in l if i in l and x != i]

    def _env(self, src, dst, b):
        if frozenset((src, dst)) in self.links:
            if b.startswith(b"DATA|"):
                self.dados_enviados[(src, dst)] += 1
            self.fila.append((dst, b))

    def drenar(self):
        while self.fila:
            dst, b = self.fila.popleft()
            self.nos[dst].on_receber(b)

    def passo(self, dt=1.0, n=1):
        for _ in range(n):
            self.t += dt
            for r in self.nos.values():
                r.tick()
            self.drenar()

    def falha(self, a, b):
        self.links.discard(frozenset((a, b)))
        self.nos[a].on_vizinho_caiu(b)
        self.nos[b].on_vizinho_caiu(a)
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
        r.nos["S"].enviar_dados(S1, b"x")
        r.drenar()
        self.assertEqual(sum(r.dados_enviados.values()), 0)

    def test_replicacao_sem_duplicados(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].pedir_stream(S1)
        r.nos["C2"].pedir_stream(S1)
        r.drenar()
        r.nos["S"].enviar_dados(S1, b"frame1")
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
        r.nos["C1"].pedir_stream(S1)
        r.nos["C2"].pedir_stream(S1)
        r.drenar()
        r.nos["C1"].parar_stream(S1)
        r.drenar()
        r.nos["S"].enviar_dados(S1, b"f")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [])
        self.assertEqual(r.recebidos["C2"], [b"f"])
        self.assertEqual(r.dados_enviados[("N1", "N2")], 0)

    def test_failover_rota_alternativa(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].pedir_stream(S1)
        r.nos["C2"].pedir_stream(S1)
        r.drenar()
        r.falha("N1", "N2")                     # caminho de C1 cai
        r.nos["S"].enviar_dados(S1, b"depois")
        r.drenar()
        self.assertEqual(r.recebidos["C1"], [b"depois"])   # via N1-N3-N2
        self.assertEqual(r.recebidos["C2"], [b"depois"])
        self.assertEqual(r.dados_enviados[("N3", "N2")], 1)

    def test_soft_state_expira_sem_refresh(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].pedir_stream(S1)
        r.drenar()
        r.nos["C1"].tick = lambda: None         # cliente morre sem avisar
        r.nos["C1"].on_receber = lambda b: None
        r.passo(1, 12)
        self.assertFalse(r.nos["N2"].t.obter(S1).ativa)

    def test_ping_regista_caminho(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].pedir_stream(S1)
        r.drenar()
        with self.assertLogs("router[C1]", level="INFO") as cm:
            r.nos["S"].ping(S1)
            r.drenar()
        self.assertIn("['S', 'N1', 'N2', 'C1']", cm.output[0])


if __name__ == "__main__":
    unittest.main()
