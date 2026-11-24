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
from tabela import PIOR, TabelaRotas, melhor_que

S1 = "S/v1"


class Rede:
    def __init__(self, links, servidores=None, streams=None):
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
                clock=lambda: self.t)

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
    def test_saltos_contados_em_cada_no(self):
        r = cenario2()
        r.passo(1, 3)
        m = {n: r.nos[n].t.obter(S1).saltos for n in ("N1", "N2", "N3", "C1", "C2")}
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
            n._enviar = (lambda d, b, i=n.id: r._env(i, d, b))
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
        self.assertEqual(r.nos["N2"].estatisticas["dados_descartados"], 1)

    def test_volta_ao_caminho_curto_quando_link_regressa(self):
        r = cenario2()
        r.passo(1, 3)
        r.nos["C1"].request_stream(S1)
        r.drenar()
        r.falha("N1", "N2")
        r.passo(1, 6)
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N3")
        self.assertEqual(r.nos["C1"].t.obter(S1).saltos, 4)
        r.repor("N1", "N2")
        r.passo(1, 6)                            # proxima ronda de ANNOUNCE recalcula
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N1")
        self.assertEqual(r.nos["C1"].t.obter(S1).saltos, 3)
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
        self.assertEqual(r.nos["n10"].t.obter("n16/video1").saltos, 1)
        self.assertEqual(r.nos["n1"].t.obter("n20/video2").saltos, 1)

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



class TestCriterios(unittest.TestCase):
    """Metrica definida com o docente: 1.o atraso (menor e melhor), 2.o saltos
    (desempate); se ambos forem iguais qualquer um serve."""

    def escolher(self, candidatos, atual=None):
        t = TabelaRotas(relogio=lambda: 0.0)
        r = t._get(S1)
        r.candidatos = {v: (m, 0.0) for v, m in candidatos.items()}
        r.origem = atual
        t._escolher(r)
        return r.origem

    def test_menor_atraso_ganha_mesmo_com_mais_saltos(self):
        self.assertEqual(self.escolher({"A": (10.0, 4), "B": (50.0, 1)}), "A")

    def test_atrasos_iguais_desempata_por_saltos(self):
        # 10,0 e 10,5 ms contam como iguais (diferenca < 2 ms): ganha o de menos saltos
        self.assertEqual(self.escolher({"A": (10.5, 2), "B": (10.0, 3)}), "A")

    def test_ambos_iguais_fica_o_atual(self):
        self.assertEqual(self.escolher({"A": (10.0, 2), "B": (10.0, 2)}, atual="B"), "B")
        self.assertEqual(self.escolher({"A": (10.0, 2), "B": (10.0, 2)}, atual="A"), "A")

    def test_ruido_na_medicao_nao_troca_de_rota(self):
        # o novo parece 0,5 ms mais rapido, com os mesmos saltos: e ruido, nao troca
        self.assertEqual(self.escolher({"A": (10.0, 2), "B": (9.5, 2)}, atual="A"), "A")

    def test_atraso_claramente_menor_troca_de_rota(self):
        self.assertEqual(self.escolher({"A": (60.0, 2), "B": (10.0, 3)}, atual="A"), "B")

    def test_menos_saltos_com_atraso_igual_troca_de_rota(self):
        self.assertEqual(self.escolher({"A": (10.0, 4), "B": (10.2, 2)}, atual="A"), "B")

    def test_melhor_que(self):
        self.assertTrue(melhor_que((5.0, 9), (50.0, 1)))     # atraso decide
        self.assertTrue(melhor_que((5.0, 1), (5.5, 2)))      # atraso igual: saltos
        self.assertFalse(melhor_que((5.0, 2), (5.0, 2)))     # igual: nao e melhor
        self.assertTrue(melhor_que((100.0, 9), PIOR))


class TestMetricaAtraso(unittest.TestCase):
    """O atraso e medido na rede (PROBE) e acumulado ao longo do caminho."""

    def test_sem_atraso_nas_ligacoes_decidem_os_saltos(self):
        r = cenario2()
        r.correr(3)
        rota = r.nos["C1"].t.obter(S1)
        self.assertEqual(rota.saltos, 3)
        self.assertLess(rota.atraso, 1.0)

    def test_mede_atraso_de_cada_ligacao(self):
        r = cenario2()
        r.atraso[frozenset(("N1", "N2"))] = 0.040           # 40 ms
        r.correr(4)
        # o simulador entrega em passos de 5 ms, por isso a medida sai por excesso
        self.assertAlmostEqual(r.nos["N2"].atraso_ligacao("N1"), 40, delta=6)
        self.assertAlmostEqual(r.nos["N2"].atraso_ligacao("N3"), 0, delta=2)
        self.assertIn("N1=", r.nos["N2"].dump())

    def test_atraso_acumulado_ao_longo_do_caminho(self):
        r = cenario2()
        r.atraso[frozenset(("S", "N1"))] = 0.020            # 20 ms logo no 1.o link
        r.correr(12)
        self.assertAlmostEqual(r.nos["C1"].t.obter(S1).atraso, 20, delta=6)
        self.assertEqual(r.nos["C1"].t.obter(S1).saltos, 3)    # sem ciclos
        self.assertIn("Atraso:", r.nos["C1"].dump())

    def test_evita_ligacao_lenta_mesmo_com_mais_saltos(self):
        r = cenario2()
        r.atraso[frozenset(("N1", "N2"))] = 0.050           # caminho curto mas lento
        r.correr(12)                                         # medir + 2 rondas de ANNOUNCE
        rota = r.nos["N2"].t.obter(S1)
        self.assertEqual(rota.origem, "N3")                  # S-N1-N3-N2: 3 saltos, ~0 ms
        self.assertEqual(rota.saltos, 3)
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
        self.assertEqual(r.nos["N2"].t.obter(S1).origem, "N1")    # atraso igual: menos saltos


class TestSemCiclos(unittest.TestCase):
    def test_anuncio_que_ja_passou_por_mim_e_ignorado(self):
        t = TabelaRotas(relogio=lambda: 0.0)
        t.anuncio(S1, "S", "A", 5.0, 1, seq=1, caminho=["S", "A"], eu="X")
        reenviar, _ = t.anuncio(S1, "S", "B", 0.0, 1, seq=1, caminho=["S", "X", "B"], eu="X")
        self.assertFalse(reenviar)
        self.assertNotIn("B", t.obter(S1).candidatos)
        self.assertEqual(t.obter(S1).origem, "A")

    def seguir_origens(self, r, no, servidor, limite=30):
        caminho = [no]
        while caminho[-1] != servidor:
            rota = r.nos[caminho[-1]].t.obter(f"{servidor}/video1")
            if rota is None or rota.origem is None:
                return None
            caminho.append(rota.origem)
            if len(caminho) > limite or len(set(caminho)) != len(caminho):
                self.fail(f"ciclo: {caminho}")
        return caminho

    def test_atrasos_aleatorios_e_falhas_nunca_criam_ciclos(self):
        import random
        for semente in range(5):
            rnd = random.Random(semente)
            r = cenario_final()
            for l in r.links:
                r.atraso[l] = rnd.choice([0.0, 0.0, 0.005, 0.020, 0.050])
            r.correr(12, dt=0.01)
            for _ in range(3):                           # 3 mudancas seguidas
                l = rnd.choice(sorted(r.links, key=sorted))
                r.atraso[l] = rnd.choice([0.0, 0.030, 0.080])
                if rnd.random() < 0.3:
                    r.links.discard(l)
                    for a, b in [tuple(l), tuple(l)[::-1]]:
                        r.nos[a].on_neighbor_down(b)
                r.correr(8, dt=0.01)
                # no 1.o segundo de cada ronda pode haver um ciclo passageiro (GRACA_RONDA);
                # os dados nao circulam porque so se aceitam da origem. Verificar depois.
                while not 2.0 <= r.t % 5 <= 4.0:
                    r.correr(0.1, dt=0.01)
                for no in ("n17", "n18", "n19", "n22", "n23"):
                    for servidor in ("n16", "n20"):
                        self.seguir_origens(r, no, servidor)


if __name__ == "__main__":
    unittest.main()
