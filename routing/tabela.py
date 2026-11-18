"""Tabela de rotas do overlay (Modulo 2 - routing).

Logica pura: nao usa sockets. O tempo e injetavel (relogio) para testes.
IDs de nos = IP (str). IDs de stream = "<ip_servidor>/<nome>".
"""
import math
import time

LOCAL = "LOCAL"  # destino especial: a aplicacao cliente deste proprio no


class Rota:
    def __init__(self, stream, servidor=None):
        self.stream = stream
        self.servidor = servidor
        self.candidatos = {}      # vizinho -> (custo, ultima_vez_visto)
        self.destinos = {}        # vizinho (ou LOCAL) -> ultima_vez_visto
        self.origem = None        # vizinho a montante (None no servidor)
        self.upstream = None      # vizinho a quem ja enviamos ACTIVATE
        self.seq = -1             # ultima ronda de anuncio vista
        self.melhor = math.inf    # melhor custo visto nessa ronda
        self.local = False        # True se este no e o servidor da stream

    @property
    def metrica(self):
        if self.local:
            return 0
        if self.origem is None:
            return None
        return self.candidatos[self.origem][0]

    @property
    def ativa(self):
        return bool(self.destinos)

    @property
    def estado(self):
        return "ativa" if self.ativa else "inativa"

    def __repr__(self):
        return (f"{{Fluxo: {self.stream}, Origem: {self.origem}, "
                f"Metrica: {self.metrica}, Destinos: {sorted(self.destinos)}, "
                f"Estado: {self.estado}}}")


class TabelaRotas:
    def __init__(self, timeout_rota=15.0, timeout_destino=6.0, relogio=time.monotonic):
        self.rotas = {}
        self.timeout_rota = timeout_rota
        self.timeout_destino = timeout_destino
        self.agora = relogio

    # ---------- acesso ----------
    def obter(self, stream):
        return self.rotas.get(stream)

    def _get(self, stream):
        if stream not in self.rotas:
            self.rotas[stream] = Rota(stream)
        return self.rotas[stream]

    def criar_local(self, stream, servidor):
        r = self._get(stream)
        r.servidor = servidor
        r.local = True
        return r

    # ---------- escolha da origem ----------
    def _escolher(self, r):
        """Escolhe a origem de menor custo; mantem a atual em caso de empate
        (evita oscilacoes). Devolve True se a origem mudou."""
        antiga = r.origem
        if r.candidatos:
            melhor = min(c for c, _ in r.candidatos.values())
            if antiga in r.candidatos and r.candidatos[antiga][0] == melhor:
                nova = antiga
            else:
                nova = min(v for v, (c, _) in r.candidatos.items() if c == melhor)
        else:
            nova = None
        r.origem = nova
        return nova != antiga

    # ---------- anuncios ----------
    def anuncio(self, stream, servidor, vizinho, metrica_vizinho, seq):
        """Processa um ANNOUNCE recebido de `vizinho`.
        Devolve (reenviar, custo_a_anunciar).
        Reenvia se for a 1a copia desta ronda (seq) ou se melhora o custo."""
        r = self._get(stream)
        if r.local or seq < r.seq:
            return False, r.metrica
        custo = metrica_vizinho + 1
        r.servidor = servidor
        r.candidatos[vizinho] = (custo, self.agora())
        self._escolher(r)
        reenviar = False
        if seq > r.seq:
            r.seq, r.melhor, reenviar = seq, custo, True
        elif custo < r.melhor:
            r.melhor, reenviar = custo, True
        return reenviar, r.metrica

    # ---------- ativacao ----------
    def ativar(self, stream, destino):
        """Regista `destino` como consumidor. True se a rota passou de inativa a ativa."""
        r = self._get(stream)
        ja_ativa = r.ativa
        r.destinos[destino] = self.agora()
        return not ja_ativa

    def desativar(self, stream, destino):
        """True se a rota ficou inativa por causa desta remocao."""
        r = self.rotas.get(stream)
        if not r or destino not in r.destinos:
            return False
        del r.destinos[destino]
        return not r.ativa

    # ---------- falhas / expiracao ----------
    def remover_vizinho(self, vizinho):
        afetadas = []
        for r in self.rotas.values():
            if vizinho in r.candidatos or vizinho in r.destinos:
                r.candidatos.pop(vizinho, None)
                r.destinos.pop(vizinho, None)
                self._escolher(r)
                afetadas.append(r.stream)
        return afetadas

    def expirar(self):
        agora = self.agora()
        afetadas = []
        for r in self.rotas.values():
            mudou = False
            for v, (_, t) in list(r.candidatos.items()):
                if agora - t > self.timeout_rota:
                    del r.candidatos[v]
                    mudou = True
            for d, t in list(r.destinos.items()):
                if d != LOCAL and agora - t > self.timeout_destino:
                    del r.destinos[d]
                    mudou = True
            if mudou:
                self._escolher(r)
                afetadas.append(r.stream)
        return afetadas

    # ---------- encaminhamento ----------
    def destinos_dados(self, stream, de):
        """Para quem replicar um pacote de dados vindo de `de`.
        So se a rota estiver ativa e `de` for a origem esperada (anti-ciclos)."""
        r = self.rotas.get(stream)
        if not r or not r.ativa or r.origem != de:
            return []
        return list(r.destinos)

    def dump(self):
        return "\n".join(repr(r) for r in self.rotas.values())
