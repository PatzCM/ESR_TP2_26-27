"""Tabela de rotas do overlay (Modulo 2 - routing).

Logica pura: nao usa sockets. O tempo e injetavel (relogio) para testes.
IDs de nos = nome (str, ex. "Node1"). IDs de stream = "<servidor>/<video>" (ex. "Server1/video1").

Metrica de uma rota = (atraso em ms, numero de saltos), comparada assim:
  1.o criterio: menor atraso.
  2.o criterio: se os atrasos forem iguais, menos saltos.
  Se ambos forem iguais, qualquer um serve: fica a origem atual (nao oscila).
Dois atrasos contam como iguais se a diferenca for <= max(TOLERANCIA_MS,
TOLERANCIA_REL x o menor), porque o atraso medido nunca e exatamente igual.
"""
import math
import time

LOCAL = "LOCAL"  # destino especial: a aplicacao cliente deste proprio no

TOLERANCIA_MS = 2.0    # atrasos a menos de 2 ms um do outro contam como iguais...
TOLERANCIA_REL = 0.1   # ...ou a menos de 10% do menor (o que for maior)
PIOR = (math.inf, math.inf)
GRACA_RONDA = 1.0      # s: no inicio de uma ronda a origem atual ainda vale, a espera da sua copia


def atrasos_iguais(a, b):
    return abs(a - b) <= max(TOLERANCIA_MS, TOLERANCIA_REL * min(a, b))


def melhor_que(m1, m2):
    """True se a metrica m1 = (atraso, saltos) e estritamente melhor do que m2."""
    if m2[0] == math.inf:
        return m1[0] != math.inf
    if atrasos_iguais(m1[0], m2[0]):
        return m1[1] < m2[1]              # 2.o criterio: saltos (desempate)
    return m1[0] < m2[0]                  # 1.o criterio: atraso


class Rota:
    def __init__(self, stream, servidor=None):
        self.stream = stream
        self.servidor = servidor
        self.candidatos = {}      # vizinho -> ((atraso_ms, saltos), ultima_vez_visto)
        self.caminhos = {}        # vizinho -> caminho do servidor ate esse vizinho
        self.seq_cand = {}        # vizinho -> ronda (seq) do ultimo anuncio dele
        self.inicio_ronda = -math.inf
        self.destinos = {}        # vizinho (ou LOCAL) -> ultima_vez_visto
        self.origem = None        # vizinho a montante (None no servidor)
        self.upstream = None      # vizinho a quem ja enviamos ACTIVATE
        self.seq = -1             # ultima ronda de anuncio vista
        self.melhor = PIOR        # melhor metrica vista nessa ronda
        self.local = False        # True se este no e o servidor da stream

    @property
    def metrica(self):
        """(atraso_ms, saltos) pela origem atual; (0, 0) no servidor."""
        if self.local:
            return (0.0, 0)
        if self.origem is None:
            return None
        return self.candidatos[self.origem][0]

    @property
    def atraso(self):
        return None if self.metrica is None else self.metrica[0]

    @property
    def saltos(self):
        return None if self.metrica is None else self.metrica[1]

    @property
    def ativa(self):
        return bool(self.destinos)

    @property
    def estado(self):
        return "ativa" if self.ativa else "inativa"

    def __repr__(self):
        return (f"{{Fluxo: {self.stream}, Origem: {self.origem}, "
                f"Atraso: {'-' if self.atraso is None else f'{self.atraso:.2f} ms'}, "
                f"Saltos: {'-' if self.saltos is None else self.saltos}, Destinos: {sorted(self.destinos)}, "
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
    def _validos(self, r):
        """Candidatos que podem ser origem: so os que anunciaram na ronda atual
        (informacao antiga pode formar ciclos depois de uma mudanca na rede).
        Excecoes: a origem atual continua valida no inicio da ronda (GRACA_RONDA)
        e, se nenhuma copia da ronda chegou (ex. anuncio perdido), mantem-se."""
        frescos = {v: c for v, c in r.candidatos.items()
                   if r.seq_cand.get(v, r.seq) >= r.seq}
        atual = r.origem
        if atual in r.candidatos and atual not in frescos:
            if not frescos or self.agora() - r.inicio_ronda <= GRACA_RONDA:
                frescos[atual] = r.candidatos[atual]
        return frescos

    def _escolher(self, r):
        """Escolhe a origem: 1.o menor atraso, 2.o menos saltos (entre atrasos iguais).
        Mantem a atual se nenhuma for estritamente melhor. Devolve True se mudou."""
        antiga = r.origem
        validos = self._validos(r)
        if not validos:
            r.origem = None
            return antiga is not None
        menor_atraso = min(m[0] for m, _ in validos.values())
        # candidatos com atraso "igual" ao menor; entre eles ganha o de menos saltos
        empatados = {v: m for v, (m, _) in validos.items()
                     if atrasos_iguais(m[0], menor_atraso)}
        menos_saltos = min(m[1] for m in empatados.values())
        escolhida = min(v for v, m in empatados.items() if m[1] == menos_saltos)
        if antiga in validos and not melhor_que(validos[escolhida][0], validos[antiga][0]):
            escolhida = antiga                # empate: qualquer um serve, fica a atual
        r.origem = escolhida
        return escolhida != antiga

    # ---------- anuncios ----------
    def anuncio(self, stream, servidor, vizinho, atraso_vizinho, saltos_vizinho, seq,
                atraso_ligacao=0.0, caminho=(), eu=None):
        """Processa um ANNOUNCE recebido de `vizinho`.
        metrica por esse vizinho = (atraso anunciado + atraso medido da ligacao,
                                    saltos anunciados + 1)
        `caminho` = nos por onde o anuncio passou desde o servidor. Se ja passou
        por mim (`eu`), essa rota daria um ciclo: o vizinho deixa de ser candidato.
        Devolve (reenviar, metrica_desta_copia). A copia e reenviada com a SUA
        metrica, nunca com uma rota antiga guardada.
        Reenvia se for a 1.a copia desta ronda (seq) ou se for melhor do que a vista."""
        r = self._get(stream)
        if r.local or seq < r.seq:
            return False, None
        if eu is not None and eu in caminho:
            if r.candidatos.pop(vizinho, None) is not None:
                r.caminhos.pop(vizinho, None)
                r.seq_cand.pop(vizinho, None)
                self._escolher(r)
            return False, None
        m = (atraso_vizinho + atraso_ligacao, saltos_vizinho + 1)
        r.servidor = servidor
        r.candidatos[vizinho] = (m, self.agora())
        r.caminhos[vizinho] = list(caminho)
        r.seq_cand[vizinho] = seq
        reenviar = False
        if seq > r.seq:
            r.seq, r.melhor, reenviar = seq, m, True
            r.inicio_ronda = self.agora()
        elif melhor_que(m, r.melhor):
            r.melhor, reenviar = m, True
        self._escolher(r)
        return reenviar, m

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
                r.caminhos.pop(vizinho, None)
                r.seq_cand.pop(vizinho, None)
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
                    r.caminhos.pop(v, None)
                    r.seq_cand.pop(v, None)
                    mudou = True
            for d, t in list(r.destinos.items()):
                if d != LOCAL and agora - t > self.timeout_destino:
                    del r.destinos[d]
                    mudou = True
            # tambem quando acaba a graca da ronda e a origem atual ficou antiga
            if self._escolher(r) or mudou:
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
