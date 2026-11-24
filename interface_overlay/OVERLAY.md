# Módulo 1 — Overlay (Pedro)

Constrói e mantém a rede de nós: quem são os vizinhos de cada nó, se estão vivos, e transporta bytes entre eles. Não interpreta o vídeo nem as rotas: entrega-os ao router (João) tal como chegam. O contrato com os outros módulos está em [`INTERFACES.md`](../INTERFACES.md).

| Ficheiro | O quê |
|---|---|
| `Mixed_testing/no_ott_mixed.py` | Nó overlay: bootstrapper + vizinhos manuais, HELLO, LEAVE, API para o router |
| `Mixed_testing/lancar_overlay.py` | Arranca, pára e inspeciona todos os nós do CORE |
| `Mixed_testing/cenario1.json`, `cenario2.json`, `cenarioF.json` | Topologias do overlay |
| `interface_overlay/test_overlay_api.py` | 9 testes em localhost (campos das mensagens, API, deteção de quedas) |
| `interface_overlay/OVERLAY.md` | Este ficheiro |
| `interface_overlay/TESTES_OVERLAY.md` | Guia de testes do módulo |

## Mensagens do overlay

JSON sobre UDP, um objeto por datagrama. Todas têm `type` e `from` (ID do nó que envia).

| `type` | Sentido | Campos além de `type` e `from` |
|---|---|---|
| `REGISTER` | nó → bootstrapper | `port`. Repete-se de 5 em 5 s e serve de "estou vivo" |
| `NEIGHBORS` | bootstrapper → nó | `reason` (`"register"` ou `"replacement"`), `neighbors` (lista de `{id, ip, port}`), `min_degree` |
| `NEED_NEIGHBOR` | nó → bootstrapper | `exclude` (IDs que o nó já conhece) |
| `ERROR` | bootstrapper → nó | `reason` |
| `HELLO` | nó ↔ vizinho | `port`. De 2 em 2 s |
| `LEAVE` | nó → vizinhos e bootstrapper | — |
| `MSG` | nó → vizinhos | `text` (só para testes) |

Exemplos:
```json
{"type": "REGISTER", "from": "Node1", "port": 5000}
{"type": "NEIGHBORS", "from": "Server", "reason": "register", "neighbors": [{"id": "Node2", "ip": "10.0.1.2", "port": 5000}], "min_degree": 1}
{"type": "HELLO", "from": "Node1", "port": 5000}
```

Tudo o que não for JSON do overlay é entregue ao router: datagramas que começam por `DATA|` e mensagens JSON cujo `type` esteja nos tipos de routing (`ANNOUNCE`, `ACTIVATE`, `DEACTIVATE`, `PING`, `PROBE`, `PROBE_REPLY`).

## Funções disponíveis

| Função | Quem usa | O que faz |
|---|---|---|
| `node.send(to, data)` | router | Envia `data` (bytes) ao vizinho com ID `to`. Devolve `True` se enviou, `False` se `to` não é vizinho ou o envio falhou |
| `node.alive_neighbors()` | router | Lista ordenada de IDs dos vizinhos ativos (já responderam a HELLO e não caíram) |
| `node.add_down_listener(fn)` | router | Regista `fn(id)`, chamada quando um vizinho ativo cai (6 s sem HELLO ou `LEAVE`). Chamada uma só vez por queda, fora do lock da tabela de vizinhos. Um `fn` que lance exceção não afeta o overlay |
| `node.attach_router(on_receive, types, lock=None)` | integração | Liga o router: `DATA\|…` e as mensagens com `type` em `types` vão para `on_receive(data)`, chamada com `lock` tomado (o `RLock` único do nó). Sem router ligado, esses datagramas são ignorados |
| `node.fechar()` | integração | Pára as threads e liberta a porta, sem enviar `LEAVE` |
| `node.terminar()` | integração | Envia `LEAVE` aos vizinhos e ao bootstrapper e fecha |

O buffer de receção é de 65535 bytes (o contrato pede pelo menos 2048; um datagrama de vídeo tem cerca de 1,4 KB). O vídeo passa intacto: o overlay não o descodifica.

## Como ligar ao `ott.py`

```python
# OttNode.__init__ (depois de criar self.node, self.router e self.lock)
self.node.attach_router(self.router.on_receive, ROUTING_TYPES, self.lock)
self.node.add_down_listener(self._vizinho_caiu)

def send(self, to, data):        # o que o overlay dá ao router
    return self.node.send(to, data)

def neighbors(self):
    return self.node.alive_neighbors()

def _vizinho_caiu(self, nid):
    with self.lock:
        self.router.on_neighbor_down(nid)
```

Com isto, o ciclo de receção próprio do `ott.py` deixa de ser necessário (a thread `Node.receber` já separa os datagramas) e o `tick_loop` só precisa de chamar `router.tick()`, sem comparar listas de vizinhos para detetar quedas. Se mantiverem o `tick_loop` antigo **e** registarem o ouvinte, `on_neighbor_down` é chamada duas vezes: usem só um dos dois.

Ordem dos locks: o lock do router (`self.lock`) é sempre tomado **antes** do lock interno do overlay, nunca ao contrário. O overlay respeita-o chamando `on_receive` e os ouvintes fora do seu próprio lock.

## Funcionamento resumido

1. O bootstrapper lê o JSON da topologia e valida-o (IDs, simetria, grau mínimo, conectividade).
2. Um nó novo envia `REGISTER` (3 tentativas de 2 s) e recebe `NEIGHBORS`. Sem resposta, usa `--neighbors`.
3. Os vizinhos trocam `HELLO` de 2 em 2 s. Sem HELLO durante 6 s, o vizinho cai.
4. Se um nó ficar com menos de `min_degree` vizinhos, envia `NEED_NEIGHBOR` e recebe `NEIGHBORS` com `reason: "replacement"`.
5. O bootstrapper esquece um nó ao fim de 15 s sem `REGISTER`.

Linha de comandos: `--id`, `--port` (ou `--porta`), `--bootstrapper IP:PORTA`, `--config FICHEIRO`, `--neighbors ID=IP:PORTA ...` (ou `--vizinhos`).

## Limitações

- A deteção de queda por timeout demora 6 s, e é isso que faz o vídeo congelar cerca de 5 s quando um nó morre sem `LEAVE`.
- O bootstrapper é um ponto único de falha para a entrada de nós novos (o overlay já formado continua a funcionar).
- O bootstrapper rejeita um `REGISTER` do mesmo ID vindo de outro IP enquanto o nó está ativo. Num router com várias interfaces, o IP de origem pode mudar se a rota para o bootstrapper mudar.
- Não há retransmissão: perdas de `REGISTER`, `HELLO` ou mensagens do router só se corrigem pela repetição periódica.

## Testar

```bash
python3 interface_overlay/test_overlay_api.py -v     # a partir da raiz; 9 testes, ~6 s, sem CORE
```
