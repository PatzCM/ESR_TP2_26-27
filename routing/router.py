"""Router do overlay (Modulo 2 - Joao): anuncios, ativacao, replicacao, failover.

Precisa de duas funcoes do modulo overlay (Pedro), ver INTERFACES.md seccao 3:
    send(to: str, data: bytes) -> None     # envia ao vizinho com ID `to`
    neighbors() -> list[str]               # IDs dos vizinhos que estao vivos
e o overlay chama estas funcoes no Router:
    on_receive(data: bytes)                # cada datagrama de routing/dados recebido
    on_neighbor_down(id: str)              # um vizinho caiu ou saiu
    tick()                                 # cerca de uma vez por segundo

Os nomes das funcoes publicas e dos campos das mensagens ficam em ingles
(decisao do grupo, INTERFACES.md seccao 1). O resto do modulo esta em portugues.

Mensagens de controlo: um objeto JSON, sempre com "type" e "from" (ID do emissor).
Dados: b"DATA|<stream>|<from>|<payload>"  (payload opaco, nunca e recodificado).

Metrica (definida com o docente): 1.o criterio o atraso (menor e melhor),
2.o criterio o numero de saltos, so para desempatar; se ambos forem iguais
qualquer um serve. O atraso de uma ligacao e medido por PROBE/PROBE_REPLY
(metade do RTT suavizado). Cada ANNOUNCE leva o atraso e os saltos acumulados
desde o servidor. A comparacao esta em tabela.py (melhor_que / _escolher).

O Router NAO e thread-safe: segurar o lock unico do no a volta de todas as chamadas.
"""
import json
import logging
import time

from tabela import LOCAL, TabelaRotas

# tipos de mensagem que o no deve entregar ao router (usado pelo ott.py)
ROUTING_TYPES = {"ANNOUNCE", "ACTIVATE", "DEACTIVATE", "PING", "PROBE", "PROBE_REPLY"}

ATRASO_DESCONHECIDO_MS = 0.0 # atraso assumido para uma ligacao ainda nao medida
ALFA_RTT = 0.3               # peso de uma nova amostra de RTT na media movel (EWMA)


class Router:
    def __init__(self, node_id, send, neighbors, server_streams=(), on_data=None,
                 clock=time.monotonic, announce_period=5.0, refresh_period=2.0,
                 probe_period=1.0):
        self.id = node_id
        self._enviar = send
        self._vizinhos = neighbors
        self.on_data = on_data                # fn(stream, payload) -> cliente deste no
        self.agora = clock
        self.periodo_anuncio = announce_period
        self.periodo_refresh = refresh_period
        self.periodo_probe = probe_period
        # rota expira apos 3 ANNOUNCE falhados (15 s), destino apos 3 ACTIVATE falhados (6 s)
        self.t = TabelaRotas(3 * announce_period, 3 * refresh_period, clock)
        self.srtt = {}                        # vizinho -> RTT suavizado (ms)
        self.log = logging.getLogger(f"router[{node_id}]")
        self.streams_servidor = list(server_streams)
        for s in self.streams_servidor:
            self.t.criar_local(s, node_id)
        self._seq = int(time.time())          # nao recomeca em 0 se o servidor reiniciar
        self._ult_anuncio = float("-inf")
        self._ult_refresh = float("-inf")
        self._ult_probe = float("-inf")
        self.estatisticas = {"dados_recebidos": 0, "dados_enviados": 0, "dados_descartados": 0}

    # ---------- API para o modulo de streaming (Patz) ----------
    def request_stream(self, stream):
        """Cliente: "quero receber esta stream"."""
        self.t.ativar(stream, LOCAL)
        self._reconciliar(stream)

    def stop_stream(self, stream):
        """Cliente: "deixa de ma enviar"."""
        self.t.desativar(stream, LOCAL)
        self._reconciliar(stream)

    def send_data(self, stream, payload):
        """Servidor: injeta um pacote de video no overlay."""
        self._replicar(stream, None, payload)

    def ping(self, stream):
        """Servidor: teste de percurso pelas rotas ativas (Etapa 3)."""
        self._propagar_ping(stream, None, [])

    def dump(self):
        """Tabela de rotas em texto (consola do no, comando `routes`)."""
        texto = self.t.dump()
        if self.srtt:
            ligacoes = ", ".join(f"{v}={self.atraso_ligacao(v):.2f}ms" for v in sorted(self.srtt))
            texto = (texto + "\n" if texto else "") + f"atraso das ligacoes (RTT/2): {ligacoes}"
        return texto

    # ---------- metrica ----------
    def atraso_ligacao(self, vizinho):
        """Atraso num sentido estimado ate um vizinho, em ms."""
        rtt = self.srtt.get(vizinho)
        return ATRASO_DESCONHECIDO_MS if rtt is None else rtt / 2

    # ---------- eventos vindos do overlay (Pedro) ----------
    def on_receive(self, data):
        if data.startswith(b"DATA|"):
            partes = data.split(b"|", 3)
            if len(partes) == 4:
                self.estatisticas["dados_recebidos"] += 1
                self._replicar(partes[1].decode(), partes[2].decode(), partes[3])
            return
        try:
            m = json.loads(data)
            tipo, emissor = m["type"], m["from"]
            if tipo == "PROBE":
                self._ctrl(emissor, "PROBE_REPLY", t=m["t"])
                return
            if tipo == "PROBE_REPLY":
                self._on_probe_reply(emissor, float(m["t"]))
                return
            stream = m["stream"]
        except (ValueError, KeyError, TypeError):
            return  # UDP: lixo e ignorado
        try:
            if tipo == "ANNOUNCE":
                self._on_announce(emissor, m)
            elif tipo == "ACTIVATE":
                if self.t.ativar(stream, emissor):
                    self.log.info("%s: %s ativou-a", stream, emissor)
                self._reconciliar(stream)
            elif tipo == "DEACTIVATE":
                self.t.desativar(stream, emissor)
                self.log.info("%s: %s desativou-a", stream, emissor)
                self._reconciliar(stream)
            elif tipo == "PING":
                self._propagar_ping(stream, emissor, list(m.get("path", [])))
        except (KeyError, TypeError, ValueError) as e:
            self.log.warning("%s invalido de %s: %r", tipo, emissor, e)

    def on_neighbor_down(self, node_id):
        self.log.info("vizinho %s em baixo", node_id)
        self.srtt.pop(node_id, None)
        for s in self.t.remover_vizinho(node_id):
            self._reconciliar(s)

    def tick(self):
        agora = self.agora()
        if agora - self._ult_probe >= self.periodo_probe:
            self._ult_probe = agora
            for v in self._vizinhos():            # o nosso relogio volta no PROBE_REPLY: sem sincronizacao
                self._ctrl(v, "PROBE", t=round(agora * 1000, 3))
        if self.streams_servidor and agora - self._ult_anuncio >= self.periodo_anuncio:
            self._ult_anuncio = agora
            self._seq += 1
            for s in self.streams_servidor:
                self._inundar({"type": "ANNOUNCE", "from": self.id, "stream": s,
                               "server": self.id, "delay": 0, "hops": 0,
                               "path": [self.id], "seq": self._seq})
        if agora - self._ult_refresh >= self.periodo_refresh:
            self._ult_refresh = agora
            for r in self.t.rotas.values():       # soft state: renova a ativacao a montante
                if r.ativa and r.upstream:
                    self._ctrl(r.upstream, "ACTIVATE", stream=r.stream)
        for s in self.t.expirar():
            self._reconciliar(s)

    # ---------- internos ----------
    @staticmethod
    def _codificar(msg):
        return json.dumps(msg, separators=(",", ":")).encode()

    def _ctrl(self, destino, tipo, **campos):
        self._enviar(destino, self._codificar({"type": tipo, "from": self.id, **campos}))

    def _inundar(self, msg, excepto=None):
        dados = self._codificar(msg)
        for v in self._vizinhos():
            if v != excepto:                      # nunca de volta pela ligacao de onde veio
                self._enviar(v, dados)

    def _on_probe_reply(self, emissor, enviado_ms):
        amostra = self.agora() * 1000 - enviado_ms
        if amostra < -1:                          # nao e um carimbo nosso: ignorar
            return
        amostra = max(amostra, 0.0)               # o arredondamento do carimbo pode dar -0.0
        antigo = self.srtt.get(emissor)
        self.srtt[emissor] = amostra if antigo is None else (1 - ALFA_RTT) * antigo + ALFA_RTT * amostra

    def _on_announce(self, emissor, m):
        antes = self.t.obter(m["stream"])
        origem_antiga = antes.origem if antes else None
        caminho = list(m.get("path", []))
        reenviar, metrica = self.t.anuncio(m["stream"], m["server"], emissor,
                                           float(m["delay"]), int(m["hops"]), int(m["seq"]),
                                           atraso_ligacao=self.atraso_ligacao(emissor),
                                           caminho=caminho, eu=self.id)
        if reenviar:
            self._inundar({"type": "ANNOUNCE", "from": self.id, "stream": m["stream"],
                           "server": m["server"], "delay": round(metrica[0], 3),
                           "hops": metrica[1], "path": caminho + [self.id], "seq": m["seq"]},
                          excepto=emissor)
        rota = self.t.obter(m["stream"])
        if rota.origem != origem_antiga:
            if rota.origem is None:
                self.log.info("%s: sem rota", m["stream"])
            else:
                self.log.info("%s: melhor rota agora via %s (atraso %.2f ms, %d saltos)",
                              m["stream"], rota.origem, rota.atraso, rota.saltos)
        self._reconciliar(m["stream"])

    def _reconciliar(self, stream):
        """Garante que a ativacao a montante corresponde a melhor origem atual."""
        r = self.t.obter(stream)
        if not r or r.local:
            return
        if r.ativa:
            if r.origem is not None and r.origem != r.upstream:
                if r.upstream:
                    self._ctrl(r.upstream, "DEACTIVATE", stream=stream)
                self._ctrl(r.origem, "ACTIVATE", stream=stream)
                self.log.info("%s ativada via %s", stream, r.origem)
                r.upstream = r.origem
        elif r.upstream:
            self._ctrl(r.upstream, "DEACTIVATE", stream=stream)
            r.upstream = None

    def _replicar(self, stream, emissor, payload):
        destinos = self.t.destinos_dados(stream, emissor)
        if not destinos:
            if emissor is not None:
                self.estatisticas["dados_descartados"] += 1   # rota inativa ou nao veio da origem
            return
        pacote = b"DATA|" + stream.encode() + b"|" + self.id.encode() + b"|" + payload
        for d in destinos:
            if d == LOCAL:
                if self.on_data:
                    self.on_data(stream, payload)
            else:
                self._enviar(d, pacote)
                self.estatisticas["dados_enviados"] += 1
        self.log.debug("DATA %s de %s -> %s", stream, emissor or "(servidor local)", destinos)

    def _propagar_ping(self, stream, emissor, caminho):
        caminho = caminho + [self.id]
        destinos = self.t.destinos_dados(stream, emissor)
        self.log.info("PING %s caminho=%s -> %s", stream, caminho, destinos)
        for d in destinos:
            if d != LOCAL:
                self._ctrl(d, "PING", stream=stream, path=caminho)
