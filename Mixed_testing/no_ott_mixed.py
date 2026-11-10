#!/usr/bin/env python3
"""Nó overlay OTT (Etapa 1) - estratégia mista: bootstrapper + vizinhos manuais. UDP.

MODOS DE ARRANQUE
  Nó que é o bootstrapper (ex: o servidor), lê a topologia de um ficheiro:
      python3 no_ott.py --id Server --config cenario1.json

  Nó normal, só precisa de conhecer o bootstrapper:
      python3 no_ott.py --id Node --bootstrapper 10.0.0.20:5000

  Nó normal com fallback manual (usado se o bootstrapper não responder):
      python3 no_ott.py --id Node --bootstrapper 10.0.0.20:5000 \
                        --vizinhos Client1=10.0.1.20:5000 Client2=10.0.2.20:5000

  Modo totalmente manual (sem bootstrapper):
      python3 no_ott.py --id Node --vizinhos Server=10.0.0.20:5000 Client1=10.0.1.20:5000

MENSAGENS (JSON sobre UDP; o campo "type" identifica a mensagem)
  REGISTER      nó -> bootstrapper   {"type","id","porta"}      (também serve de "estou vivo")
  NEIGHBORS     bootstrapper -> nó   {"type","motivo","vizinhos":[{"id","ip","port"}],"min_degree"}
  NEED_NEIGHBOR nó -> bootstrapper   {"type","id","excluir":[ids]}  (pedir substituto)
  ERROR         bootstrapper -> nó   {"type","motivo"}
  HELLO         nó <-> vizinho       {"type","id","porta"}      (keep-alive)
  LEAVE         nó -> todos          {"type","id"}
  MSG           nó -> vizinhos       {"type","id","texto"}     (apenas para testes)
"""
import argparse
import json
import socket
import sys
import threading
import time

BUFFER = 4096
HELLO_INTERVAL = 2.0        # período dos keep-alives
DEAD_TIMEOUT = 6.0          # sem HELLO durante este tempo -> vizinho caído
REREGISTER_INTERVAL = 5.0   # re-registo periódico no bootstrapper
ACTIVE_TIMEOUT = 15.0       # bootstrapper: sem REGISTER durante este tempo -> nó inativo
REGISTER_RETRIES = 3        # tentativas de REGISTER no arranque (UDP pode perder pacotes)
REGISTER_TIMEOUT = 2.0      # espera por resposta em cada tentativa


def parse_addr(texto):
    """'10.0.0.1:5000' -> ('10.0.0.1', 5000)"""
    ip, porta = texto.rsplit(":", 1)
    return ip, int(porta)


class Node:
    def __init__(self, node_id, porta):
        self.id = node_id
        self.porta = porta
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", porta))
        self.lock = threading.RLock()
        self.parar = threading.Event()

        # id -> {"ip","port","estado": pendente|ativo|caido,"last","origem": config|manual|substituto|aprendido}
        self.neighbors = {}
        self.min_degree = 1
        self.ultimo_pedido = 0.0

        self.boot_addr = None            # endereço do bootstrapper (nós normais)
        self.registado = threading.Event()
        self.erro_registo = None

        self.config = None               # só no nó bootstrapper
        self.registry = {}               # só no bootstrapper: id -> {"ip","last"}

    # ------------------------------------------------------------ utilitários
    def log(self, tag, texto):
        print(f"{time.strftime('%H:%M:%S')} [{self.id}] [{tag}] {texto}", flush=True)

    def fatal(self, texto):
        self.log("ERRO", texto)
        sys.exit(1)

    def enviar(self, msg, addr):
        try:
            self.sock.sendto(json.dumps(msg).encode("utf-8"), addr)
        except OSError as e:
            self.log("ERRO", f"falha a enviar {msg.get('type')} para {addr}: {e}")

    # ------------------------------------------------------------ vizinhos
    def add_neighbor(self, nid, ip, port, origem):
        with self.lock:
            if nid == self.id:
                return False
            if nid in self.neighbors:
                if origem in ("config", "manual"):
                    self.neighbors[nid]["ip"] = ip
                    self.neighbors[nid]["port"] = port
                return False
            self.neighbors[nid] = {"ip": ip, "port": port, "estado": "pendente",
                                   "last": None, "origem": origem}
        self.log("VIZINHO", f"novo vizinho {nid} ({ip}:{port}) [{origem}]")
        return True

    def adicionar_manuais(self, lista):
        for item in lista:
            nid, resto = item.split("=", 1)
            ip, port = parse_addr(resto)
            self.add_neighbor(nid, ip, port, "manual")

    def mostrar_vizinhos(self):
        with self.lock:
            if not self.neighbors:
                print("(sem vizinhos)")
                return
            print(f"{'ID':<10} {'ENDEREÇO':<22} {'ESTADO':<10} ORIGEM")
            for nid, n in sorted(self.neighbors.items()):
                print(f"{nid:<10} {n['ip'] + ':' + str(n['port']):<22} {n['estado']:<10} {n['origem']}")

    # ------------------------------------------------------------ bootstrapper
    def carregar_config(self, caminho):
        """Lê e VALIDA a topologia: IDs existentes, simetria, grau mínimo, conectividade."""
        with open(caminho, encoding="utf-8") as f:
            cfg = json.load(f)
        nodes, links = cfg["nodes"], cfg["links"]
        min_degree = cfg.get("min_degree", 1)

        if self.id not in nodes:
            self.fatal(f"o ID '{self.id}' não existe em 'nodes' do ficheiro {caminho}")

        adj = {n: set() for n in nodes}
        avisados = set()
        for a, lista in links.items():
            if a not in nodes:
                self.fatal(f"'links' refere o nó desconhecido '{a}'")
            for b in lista:
                if b not in nodes:
                    self.fatal(f"'links' refere o nó desconhecido '{b}'")
                if a == b:
                    continue
                par = frozenset((a, b))
                if a not in links.get(b, []) and par not in avisados:
                    self.log("CONFIG", f"AVISO: ligação {a}-{b} só declarada num sentido; corrigida")
                    avisados.add(par)
                adj[a].add(b)
                adj[b].add(a)

        for n, viz in adj.items():
            if len(viz) < min_degree:
                self.log("CONFIG", f"AVISO: {n} tem {len(viz)} vizinho(s), abaixo do mínimo ({min_degree})")

        inicio = next(iter(nodes))
        vistos, fila = {inicio}, [inicio]
        while fila:
            x = fila.pop()
            for y in adj[x]:
                if y not in vistos:
                    vistos.add(y)
                    fila.append(y)
        if len(vistos) != len(nodes):
            self.fatal(f"topologia não é conexa; inalcançáveis a partir de {inicio}: {sorted(set(nodes) - vistos)}")

        self.config = {"nodes": nodes, "adj": adj, "min_degree": min_degree}
        self.min_degree = min_degree
        for v in sorted(adj[self.id]):
            self.add_neighbor(v, nodes[v]["ip"], nodes[v]["port"], "config")
        self.log("CONFIG", f"{len(nodes)} nós, configuração válida; sou o bootstrapper")

    def ativo(self, nid):
        if nid == self.id:
            return True
        reg = self.registry.get(nid)
        return bool(reg) and time.time() - reg["last"] < ACTIVE_TIMEOUT

    def entrada(self, nid):
        n = self.config["nodes"][nid]
        return {"id": nid, "ip": n["ip"], "port": n["port"]}

    def escolher_substituto(self, nid, excluir):
        """Escolhe um nó ativo que ainda não seja vizinho; prefere os mais ligados (menos folhas)."""
        adj = self.config["adj"]
        with self.lock:
            excl = set(excluir) | {nid}
            cand = [c for c in self.config["nodes"] if c not in excl and self.ativo(c)]
            if not cand:
                return None
            cand.sort(key=lambda c: (-len(adj[c]), c))
            return self.entrada(cand[0])

    def on_register(self, msg, addr):
        if not self.config:
            return
        nid = msg["id"]
        if nid not in self.config["nodes"]:
            self.log("REGISTO", f"rejeitado '{nid}' (ID desconhecido) de {addr[0]}")
            self.enviar({"type": "ERROR", "motivo": f"ID '{nid}' desconhecido na configuração"}, addr)
            return
        with self.lock:
            reg = self.registry.get(nid)
            ja_ativo = self.ativo(nid) and reg is not None
            if nid == self.id or (ja_ativo and reg["ip"] != addr[0]):
                duplicado = True
            else:
                duplicado = False
                self.registry[nid] = {"ip": addr[0], "last": time.time()}
                vizinhos = [self.entrada(v) for v in sorted(self.config["adj"][nid])]
        if duplicado:
            self.log("REGISTO", f"rejeitado '{nid}' de {addr[0]} (ID já ativo noutra máquina)")
            self.enviar({"type": "ERROR", "motivo": f"ID '{nid}' já está ativo"}, addr)
            return
        if not ja_ativo:
            self.log("REGISTO", f"{nid} registado a partir de {addr[0]}")
        self.enviar({"type": "NEIGHBORS", "motivo": "register", "vizinhos": vizinhos,
                     "min_degree": self.config["min_degree"]}, addr)

    def on_need_neighbor(self, msg, addr):
        if not self.config:
            return
        nid = msg["id"]
        entr = self.escolher_substituto(nid, msg.get("excluir", []))
        if entr:
            self.log("REPARAÇÃO", f"substituto para {nid}: {entr['id']}")
        else:
            self.log("REPARAÇÃO", f"sem substituto disponível para {nid}")
        self.enviar({"type": "NEIGHBORS", "motivo": "substituto",
                     "vizinhos": [entr] if entr else [],
                     "min_degree": self.config["min_degree"]}, addr)

    # ------------------------------------------------------------ nó normal
    def send_register(self):
        self.enviar({"type": "REGISTER", "id": self.id, "porta": self.porta}, self.boot_addr)

    def registo_inicial(self):
        """Tenta registar-se REGISTER_RETRIES vezes. Devolve True se o bootstrapper respondeu."""
        for i in range(1, REGISTER_RETRIES + 1):
            self.log("REGISTO", f"REGISTER para {self.boot_addr[0]}:{self.boot_addr[1]} (tentativa {i}/{REGISTER_RETRIES})")
            self.send_register()
            if self.registado.wait(REGISTER_TIMEOUT):
                break
        if self.erro_registo:
            self.fatal(f"bootstrapper recusou o registo: {self.erro_registo}")
        return self.registado.is_set()

    def on_neighbors(self, msg, addr):
        motivo = msg.get("motivo", "register")
        origem = "config" if motivo == "register" else "substituto"
        vizinhos = msg.get("vizinhos", [])
        for v in vizinhos:
            self.add_neighbor(v["id"], v["ip"], v["port"], origem)
        self.min_degree = msg.get("min_degree", self.min_degree)
        if motivo == "register":
            self.registado.set()
        elif not vizinhos:
            self.log("REPARAÇÃO", "o bootstrapper não tem substituto disponível")

    def on_error(self, msg, addr):
        self.log("ERRO", f"bootstrapper: {msg.get('motivo')}")
        self.erro_registo = msg.get("motivo", "erro")
        self.registado.set()

    # ------------------------------------------------------------ keep-alive
    def on_hello(self, msg, addr):
        nid = msg["id"]
        if nid == self.id:
            return
        with self.lock:
            if nid not in self.neighbors:
                # ligação só declarada do outro lado (assimetria): aprende o vizinho
                self.add_neighbor(nid, addr[0], msg.get("porta", addr[1]), "aprendido")
            n = self.neighbors[nid]
            n["last"] = time.time()
            if n["estado"] != "ativo":
                n["estado"] = "ativo"
                self.log("VIZINHO", f"{nid} ATIVO")

    def on_leave(self, msg, addr):
        nid = msg["id"]
        with self.lock:
            n = self.neighbors.get(nid)
            if n and n["estado"] != "caido":
                n["estado"] = "caido"
                self.log("VIZINHO", f"{nid} saiu (LEAVE) -> CAÍDO")
            if self.config:
                self.registry.pop(nid, None)

    def on_msg(self, msg, addr):
        self.log("MSG", f"de {msg.get('id')}: {msg.get('texto')!r}")

    def verificar_vizinhos(self):
        agora = time.time()
        with self.lock:
            for nid, n in self.neighbors.items():
                if n["estado"] == "ativo" and agora - n["last"] > DEAD_TIMEOUT:
                    n["estado"] = "caido"
                    self.log("VIZINHO", f"{nid} sem HELLO há {DEAD_TIMEOUT:.0f}s -> CAÍDO")

            caidos = [i for i, n in self.neighbors.items() if n["estado"] == "caido"]
            validos = sum(1 for n in self.neighbors.values() if n["estado"] != "caido")
            if not caidos or validos >= self.min_degree:
                return
            if agora - self.ultimo_pedido < REREGISTER_INTERVAL:
                return
            self.ultimo_pedido = agora
            excluir = list(self.neighbors.keys())

        self.log("REPARAÇÃO", f"{validos} vizinho(s) válidos < mínimo {self.min_degree}; a pedir substituto")
        if self.config:
            entr = self.escolher_substituto(self.id, excluir)
            if entr:
                self.add_neighbor(entr["id"], entr["ip"], entr["port"], "substituto")
            else:
                self.log("REPARAÇÃO", "sem substituto disponível")
        elif self.boot_addr:
            self.enviar({"type": "NEED_NEIGHBOR", "id": self.id, "excluir": excluir}, self.boot_addr)
        else:
            self.log("REPARAÇÃO", "modo manual: sem bootstrapper para pedir substituto")

    def manutencao(self):
        """Thread: HELLO periódicos, deteção de falhas e re-registo."""
        ultimo_reg = 0.0
        while not self.parar.is_set():
            with self.lock:
                alvos = [(n["ip"], n["port"]) for n in self.neighbors.values()]
            for a in alvos:
                self.enviar({"type": "HELLO", "id": self.id, "porta": self.porta}, a)
            self.verificar_vizinhos()
            if self.boot_addr and time.time() - ultimo_reg >= REREGISTER_INTERVAL:
                self.send_register()
                ultimo_reg = time.time()
            self.parar.wait(HELLO_INTERVAL)

    # ------------------------------------------------------------ receção
    def receber(self):
        handlers = {
            "REGISTER": self.on_register,
            "NEED_NEIGHBOR": self.on_need_neighbor,
            "NEIGHBORS": self.on_neighbors,
            "ERROR": self.on_error,
            "HELLO": self.on_hello,
            "LEAVE": self.on_leave,
            "MSG": self.on_msg,
        }
        while not self.parar.is_set():
            try:
                dados, addr = self.sock.recvfrom(BUFFER)
            except OSError:
                break
            try:
                msg = json.loads(dados.decode("utf-8"))
                handler = handlers.get(msg["type"])
            except (ValueError, KeyError, TypeError):
                self.log("AVISO", f"mensagem inválida de {addr[0]}:{addr[1]}")
                continue
            if handler is None:
                self.log("AVISO", f"tipo desconhecido {msg['type']!r} de {addr[0]}")
                continue
            try:
                handler(msg, addr)
            except Exception as e:  # um pacote mal formado não pode matar o nó
                self.log("ERRO", f"a tratar {msg['type']} de {addr[0]}: {e!r}")

    # ------------------------------------------------------------ consola / saída
    def consola(self):
        print("Comandos: vizinhos | registo | msg <texto> | sair")
        try:
            while True:
                try:
                    linha = input("> ").strip()
                except EOFError:
                    self.parar.wait()   # sem terminal interativo: continua a correr
                    break
                if linha == "sair":
                    break
                elif linha == "vizinhos":
                    self.mostrar_vizinhos()
                elif linha == "registo" and self.config:
                    with self.lock:
                        for nid in sorted(self.config["nodes"]):
                            print(f"{nid:<10} {'ATIVO' if self.ativo(nid) else 'inativo'}")
                elif linha.startswith("msg "):
                    texto = linha[4:]
                    with self.lock:
                        alvos = [(n["ip"], n["port"]) for n in self.neighbors.values()
                                 if n["estado"] == "ativo"]
                    for a in alvos:
                        self.enviar({"type": "MSG", "id": self.id, "texto": texto}, a)
                    print(f"enviado a {len(alvos)} vizinho(s) ativo(s)")
                elif linha:
                    print("comando desconhecido")
        except KeyboardInterrupt:
            pass
        finally:
            self.terminar()

    def terminar(self):
        msg = {"type": "LEAVE", "id": self.id}
        with self.lock:
            alvos = [(n["ip"], n["port"]) for n in self.neighbors.values()]
        for a in alvos:
            self.enviar(msg, a)
        if self.boot_addr:
            self.enviar(msg, self.boot_addr)
        self.parar.set()
        self.sock.close()
        print(f"\n[{self.id}] terminado.")


def main():
    ap = argparse.ArgumentParser(description="Nó overlay OTT (UDP)")
    ap.add_argument("--id", required=True, help="identificador do nó (ex: Node2)")
    ap.add_argument("--porta", type=int, default=5000, help="porta UDP local (por omissão 5000)")
    ap.add_argument("--bootstrapper", metavar="IP:PORTA", help="endereço do bootstrapper")
    ap.add_argument("--config", metavar="FICHEIRO", help="ficheiro de topologia (este nó é o bootstrapper)")
    ap.add_argument("--vizinhos", nargs="*", default=[], metavar="ID=IP:PORTA",
                    help="vizinhos manuais (fallback ou modo manual)")
    args = ap.parse_args()

    if args.config and args.bootstrapper:
        ap.error("--config e --bootstrapper são mutuamente exclusivos")

    node = Node(args.id, args.porta)
    if args.config:
        node.carregar_config(args.config)          # antes de receber pedidos
    threading.Thread(target=node.receber, daemon=True).start()

    if args.config:
        node.adicionar_manuais(args.vizinhos)
    elif args.bootstrapper:
        node.boot_addr = parse_addr(args.bootstrapper)
        if not node.registo_inicial():
            node.log("REGISTO", "bootstrapper sem resposta; fallback para vizinhos manuais")
            node.adicionar_manuais(args.vizinhos)
            if not args.vizinhos:
                node.log("REGISTO", "sem vizinhos manuais; continuo a tentar o bootstrapper")
    else:
        if not args.vizinhos:
            node.log("AVISO", "sem bootstrapper nem vizinhos: nó isolado")
        node.adicionar_manuais(args.vizinhos)

    threading.Thread(target=node.manutencao, daemon=True).start()
    node.consola()


if __name__ == "__main__":
    main()
