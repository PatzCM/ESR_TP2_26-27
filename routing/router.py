"""Router do overlay (Modulo 2): anuncios, ativacao, replicacao, failover.

Depende apenas de duas funcoes fornecidas pelo modulo overlay (Pedro):
    enviar(destino_ip: str, dados: bytes) -> None     # UDP
    vizinhos() -> list[str]                           # IPs dos vizinhos atuais
e de chamadas SUAS para o Router:
    on_receber(dados: bytes)      # cada datagrama recebido (de qualquer vizinho)
    on_vizinho_caiu(ip: str)
    tick()                        # chamar ~1x/segundo

Mensagens de controlo: JSON numa linha, com "t" e "de" (ID do no emissor).
Dados: b"DATA|<stream>|<de>|<payload>"  (payload binario, nao e re-codificado).
"""
import json
import logging
import time

from tabela import LOCAL, TabelaRotas

TIPOS_CONTROLO = {"ANNOUNCE", "ACTIVATE", "DEACTIVATE", "PING"}


class Router:
    def __init__(self, meu_id, enviar, vizinhos, streams_servidor=(), on_dados=None,
                 relogio=time.monotonic, periodo_anuncio=5.0, periodo_refresh=2.0):
        self.id = meu_id
        self._enviar = enviar
        self._vizinhos = vizinhos
        self.on_dados = on_dados              # fn(stream, payload) -> cliente
        self.agora = relogio
        self.periodo_anuncio = periodo_anuncio
        self.periodo_refresh = periodo_refresh
        self.t = TabelaRotas(3 * periodo_anuncio, 3 * periodo_refresh, relogio)
        self.log = logging.getLogger(f"router[{meu_id}]")
        self.streams_servidor = list(streams_servidor)
        for s in self.streams_servidor:
            self.t.criar_local(s, meu_id)
        self._seq = int(time.time())          # nao recomeca em 0 se o servidor reiniciar
        self._ult_anuncio = float("-inf")
        self._ult_refresh = float("-inf")

    # ---------- API para a aplicacao ----------
    def pedir_stream(self, stream):
        """Cliente: quer receber a stream."""
        self.t.ativar(stream, LOCAL)
        self._reconciliar(stream)

    def parar_stream(self, stream):
        self.t.desativar(stream, LOCAL)
        self._reconciliar(stream)

    def enviar_dados(self, stream, payload):
        """Servidor (Patz): injeta um pacote na rede overlay."""
        self._replicar(stream, None, payload)

    def ping(self, stream):
        """Servidor: teste de percurso (Etapa 3)."""
        self._propagar_ping(stream, None, [])

    # ---------- eventos vindos do overlay ----------
    def on_receber(self, dados):
        if dados.startswith(b"DATA|"):
            partes = dados.split(b"|", 3)
            if len(partes) == 4:
                self._replicar(partes[1].decode(), partes[2].decode(), partes[3])
            return
        try:
            m = json.loads(dados)
            t, de = m["t"], m["de"]
        except (ValueError, KeyError, TypeError):
            return  # UDP: lixo e ignorado
        if t == "ANNOUNCE":
            self._on_announce(de, m)
        elif t == "ACTIVATE":
            self.t.ativar(m["stream"], de)
            self._reconciliar(m["stream"])
        elif t == "DEACTIVATE":
            self.t.desativar(m["stream"], de)
            self._reconciliar(m["stream"])
        elif t == "PING":
            self._propagar_ping(m["stream"], de, m["caminho"])

    def on_vizinho_caiu(self, ip):
        self.log.info("vizinho %s caiu", ip)
        for s in self.t.remover_vizinho(ip):
            self._reconciliar(s)

    def tick(self):
        agora = self.agora()
        if self.streams_servidor and agora - self._ult_anuncio >= self.periodo_anuncio:
            self._ult_anuncio = agora
            self._seq += 1
            for s in self.streams_servidor:
                self._flood({"t": "ANNOUNCE", "de": self.id, "stream": s,
                             "servidor": self.id, "metrica": 0, "seq": self._seq})
        if agora - self._ult_refresh >= self.periodo_refresh:
            self._ult_refresh = agora
            for r in self.t.rotas.values():       # soft state: renova a ativacao a montante
                if r.ativa and r.upstream:
                    self._ctrl(r.upstream, "ACTIVATE", stream=r.stream)
        for s in self.t.expirar():
            self._reconciliar(s)

    # ---------- internos ----------
    def _ctrl(self, destino, tipo, **campos):
        m = {"t": tipo, "de": self.id, **campos}
        self._enviar(destino, json.dumps(m, separators=(",", ":")).encode())

    def _flood(self, msg, excepto=None):
        for v in self._vizinhos():
            if v != excepto:
                self._enviar(v, json.dumps(msg, separators=(",", ":")).encode())

    def _on_announce(self, de, m):
        reenviar, custo = self.t.anuncio(m["stream"], m["servidor"], de, m["metrica"], m["seq"])
        if reenviar:
            self._flood({"t": "ANNOUNCE", "de": self.id, "stream": m["stream"],
                         "servidor": m["servidor"], "metrica": custo, "seq": m["seq"]},
                        excepto=de)
        self._reconciliar(m["stream"])

    def _reconciliar(self, stream):
        """Garante que o pedido de ativacao a montante corresponde a origem atual."""
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

    def _replicar(self, stream, de, payload):
        destinos = self.t.destinos_dados(stream, de)
        if not destinos:
            return
        pacote = b"DATA|" + stream.encode() + b"|" + self.id.encode() + b"|" + payload
        for d in destinos:
            if d == LOCAL:
                if self.on_dados:
                    self.on_dados(stream, payload)
            else:
                self._enviar(d, pacote)

    def _propagar_ping(self, stream, de, caminho):
        caminho = caminho + [self.id]
        destinos = self.t.destinos_dados(stream, de)
        self.log.info("PING %s caminho=%s -> %s", stream, caminho, destinos)
        for d in destinos:
            if d != LOCAL:
                self._ctrl(d, "PING", stream=stream, caminho=caminho)
