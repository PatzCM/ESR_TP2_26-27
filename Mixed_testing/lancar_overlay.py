#!/usr/bin/env python3
"""Lança, pára e inspeciona todos os nós overlay do CORE de uma só vez.

Corre na máquina anfitriã (fora dos nós), como root, com a simulação do CORE
a executar. Usa o 'vcmd' do CORE para executar comandos dentro de cada nó.

  sudo python3 lancar_overlay.py start  cenario2.json     # arranca tudo
  sudo python3 lancar_overlay.py status cenario2.json     # quem está vivo
  sudo python3 lancar_overlay.py logs   cenario2.json Node1        # últimas linhas
  sudo python3 lancar_overlay.py logs   cenario2.json Node1 -f     # seguir em direto
  sudo python3 lancar_overlay.py stop   cenario2.json Node2        # mata um nó (queda brusca)
  sudo python3 lancar_overlay.py stop   cenario2.json              # pára todos

Opções úteis:
  --imprimir              só mostra os comandos, não executa nada
  --sessao /tmp/pycore.1  pasta da sessão (por omissão a mais recente)
  --bootstrapper Server   ID do nó que faz de bootstrapper (por omissão Server)
  --mapa Node=node        quando o nome no CORE difere do ID no JSON (Cenário 1)
"""
import argparse
import glob
import json
import os
import shlex
import shutil
import subprocess
import sys
import time

AQUI = os.path.dirname(os.path.abspath(__file__))


def sessao_mais_recente():
    dirs = [d for d in glob.glob("/tmp/pycore.*") if os.path.isdir(d)]
    return max(dirs, key=os.path.getmtime) if dirs else None


class Lancador:
    def __init__(self, args):
        self.args = args
        with open(args.cenario, encoding="utf-8") as f:
            self.nodes = json.load(f)["nodes"]
        if args.bootstrapper not in self.nodes:
            sys.exit(f"ERRO: bootstrapper '{args.bootstrapper}' não existe em {args.cenario}")
        self.mapa = dict(m.split("=", 1) for m in args.mapa)
        self.cenario = os.path.abspath(args.cenario)
        self.script = os.path.abspath(args.script)
        self.logs = os.path.abspath(args.logs)
        self.sessao = args.sessao or sessao_mais_recente()
        if not self.sessao and not args.imprimir:
            sys.exit("ERRO: não encontrei /tmp/pycore.* (a simulação está a correr? usa --sessao)")
        if not os.path.isfile(self.script):
            sys.exit(f"ERRO: não encontrei {self.script} (usa --script)")
        if not args.imprimir and not shutil.which(args.vcmd):
            sys.exit(f"ERRO: '{args.vcmd}' não encontrado (corre como root no PC onde está o CORE, "
                     "ou usa --imprimir)")

    # ----------------------------------------------------------- utilitários
    def nome_core(self, nid):
        return self.mapa.get(nid, nid)

    def pidfile(self, nid):
        return os.path.join(self.logs, f"{nid}.pid")

    def logfile(self, nid):
        return os.path.join(self.logs, f"{nid}.log")

    def selecionar(self, escolhidos):
        if not escolhidos:
            return list(self.nodes)
        for n in escolhidos:
            if n not in self.nodes:
                sys.exit(f"ERRO: nó '{n}' não existe no cenário. Nós: {', '.join(self.nodes)}")
        return escolhidos

    def no_cmd(self, nid, comando):
        """Executa 'comando' (shell) dentro do nó. Devolve (código, saída)."""
        alvo = os.path.join(self.sessao or "/tmp/pycore.X", self.nome_core(nid))
        cmd = [self.args.vcmd, "-c", alvo, "--", "sh", "-c", comando]
        if self.args.imprimir:
            print(" ".join(shlex.quote(c) for c in cmd))
            return 0, ""
        r = subprocess.run(cmd, capture_output=True, text=True)
        return r.returncode, (r.stdout + r.stderr).strip()

    # ----------------------------------------------------------- ações
    def comando_no(self, nid):
        n = self.nodes[nid]
        base = f"python3 {shlex.quote(self.script)} --id {nid} --porta {n['port']}"
        if nid == self.args.bootstrapper:
            return f"{base} --config {shlex.quote(self.cenario)}"
        b = self.nodes[self.args.bootstrapper]
        return f"{base} --bootstrapper {b['ip']}:{b['port']}"

    def start(self, escolhidos):
        os.makedirs(self.logs, exist_ok=True)
        ordem = self.selecionar(escolhidos)
        # o bootstrapper primeiro, para os outros se poderem registar logo
        ordem.sort(key=lambda n: n != self.args.bootstrapper)
        for nid in ordem:
            run = (f"nohup {self.comando_no(nid)} </dev/null >{shlex.quote(self.logfile(nid))} 2>&1 & "
                   f"echo $! > {shlex.quote(self.pidfile(nid))}")
            cod, out = self.no_cmd(nid, run)
            print(f"{'OK  ' if cod == 0 else 'ERRO'} {nid:<8} (nó CORE '{self.nome_core(nid)}') {out}")
            if nid == self.args.bootstrapper and not self.args.imprimir:
                time.sleep(1.0)
        print(f"\nLogs em {self.logs}/<nó>.log  (ex: tail -f {self.logfile(ordem[-1])})")

    def vivo(self, nid):
        cod, _ = self.no_cmd(nid, f"kill -0 $(cat {shlex.quote(self.pidfile(nid))}) 2>/dev/null")
        return cod == 0

    def status(self, escolhidos):
        for nid in self.selecionar(escolhidos):
            print(f"{nid:<8} {'a correr' if self.vivo(nid) else 'parado'}")

    def stop(self, escolhidos):
        for nid in self.selecionar(escolhidos):
            pf = shlex.quote(self.pidfile(nid))
            cod, _ = self.no_cmd(nid, f"kill $(cat {pf}) 2>/dev/null && rm -f {pf}")
            print(f"{nid:<8} {'terminado (SIGTERM, sem LEAVE)' if cod == 0 else 'já não estava a correr'}")

    def mostrar_logs(self, nid, seguir):
        self.selecionar([nid])
        cmd = ["tail", "-f" if seguir else "-n", self.logfile(nid)] if seguir else \
              ["tail", "-n", "40", self.logfile(nid)]
        if self.args.imprimir:
            print(" ".join(cmd))
            return
        try:
            subprocess.run(cmd)
        except KeyboardInterrupt:
            pass


def main():
    ap = argparse.ArgumentParser(description="Lança os nós overlay no CORE de uma só vez",
                                 formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog=__doc__)
    ap.add_argument("acao", choices=["start", "stop", "status", "logs"])
    ap.add_argument("cenario", help="ficheiro JSON do cenário (ex: cenario2.json)")
    ap.add_argument("nos", nargs="*", help="nós a que se aplica (por omissão todos)")
    ap.add_argument("-f", dest="seguir", action="store_true", help="logs: seguir em direto")
    ap.add_argument("--script", default=os.path.join(AQUI, "no_ott.py"), help="caminho do no_ott.py")
    ap.add_argument("--logs", default=os.path.join(AQUI, "logs"), help="pasta dos logs e pids")
    ap.add_argument("--sessao", help="pasta da sessão CORE (por omissão a mais recente em /tmp/pycore.*)")
    ap.add_argument("--bootstrapper", default="Server", help="ID do bootstrapper (por omissão Server)")
    ap.add_argument("--mapa", nargs="*", default=[], metavar="ID=NOME_CORE",
                    help="ID do JSON -> nome do nó no CORE, se forem diferentes")
    ap.add_argument("--vcmd", default="vcmd", help="comando vcmd do CORE")
    ap.add_argument("--imprimir", action="store_true", help="só mostrar os comandos")
    args = ap.parse_args()

    lanc = Lancador(args)
    if args.acao == "start":
        lanc.start(args.nos)
    elif args.acao == "stop":
        lanc.stop(args.nos)
    elif args.acao == "status":
        lanc.status(args.nos)
    else:
        if len(args.nos) != 1:
            sys.exit("ERRO: 'logs' precisa de exatamente um nó")
        lanc.mostrar_logs(args.nos[0], args.seguir)


if __name__ == "__main__":
    main()
