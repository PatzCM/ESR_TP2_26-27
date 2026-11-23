# Routing (João): como funciona e onde encaixa

## Onde está no projeto

Cada máquina corre o `ott.py`, que junta os três módulos num só processo:

```
  Streaming (Patz)   servidor: send_data(stream, pacote)      cliente: on_data(stream, pacote)
                                  │                                        ▲
                                  ▼                                        │
  Routing (TU)       Router ── decide para quem vai cada pacote e cada mensagem de controlo
                                  │ send(vizinho, bytes)                   ▲ on_receive(bytes)
                                  ▼                                        │
  Overlay (Pedro)    sabe quem são os vizinhos e os seus IPs; deteta quando um cai
                                  │                                        ▲
                                  └──────────── socket UDP :5000 ──────────┘
```

O router **não abre sockets nem conhece IPs**. Só fala em nomes (`Node1`, `Client2`):
- **recebe do overlay:** `send(to, data)` para enviar e `neighbors()` para saber quem está vivo;
- **o overlay chama-o:** `on_receive(data)` para cada datagrama de routing ou de vídeo, `on_neighbor_down(id)` quando um vizinho cai, e `tick()` várias vezes por segundo;
- **o streaming chama-o:** `send_data` (servidor), `request_stream` e `stop_stream` (cliente). Recebe o vídeo pela função `on_data`.

## A vida de uma stream, passo a passo

Exemplo com o Cenário 2: `Server – Node1 – Node2 – Client1`, `Node1 – Node3 – Client2`, e um link extra `Node2 – Node3`.

| # | O que acontece | Mensagem | Código |
|---|---|---|---|
| 1 | De 1 em 1 s cada nó mede o atraso até cada vizinho | `PROBE` → `PROBE_REPLY` | `tick()`, `_on_probe_reply()` |
| 2 | De 5 em 5 s o servidor anuncia as suas streams | `ANNOUNCE` (metric 0, seq novo) | `tick()` |
| 3 | Cada nó soma o custo da ligação (atraso + 1 ms), guarda quem lho enviou como candidato e reenvia aos outros vizinhos (só a 1.ª cópia ou uma melhor, nunca para trás) | `ANNOUNCE` | `_on_announce()` → `tabela.anuncio()` |
| 4 | A origem da rota passa a ser o candidato mais barato. A rota existe, mas está **inativa**: não passa vídeo | — | `tabela._escolher()` |
| 5 | O Client1 quer ver: envia ACTIVATE à sua origem (Node2), que o reenvia à dele (Node1), e assim até ao servidor | `ACTIVATE` | `request_stream()`, `_reconciliar()` |
| 6 | Cada nó pelo caminho regista quem pediu em **destinos**: a rota fica **ativa** | — | `tabela.ativar()` |
| 7 | O servidor envia vídeo; cada nó manda uma cópia a cada destino. No Node1 a stream divide-se em duas (Node2 e Node3); na ligação Server–Node1 vai uma só cópia | `DATA\|stream\|from\|…` | `send_data()`, `_replicar()` |
| 8 | Um pacote que não vem da origem esperada é deitado fora (evita ciclos) | — | `tabela.destinos_dados()` |
| 9 | O cliente renova o ACTIVATE de 2 em 2 s. Se deixar de o fazer, ao fim de 6 s o ramo desliga-se sozinho | `ACTIVATE` | `tick()`, `tabela.expirar()` |
| 10 | O cliente sai e o ramo é desligado até onde ninguém mais precisa dele | `DEACTIVATE` | `stop_stream()` |

**PING (Etapa 3):** o servidor envia um `PING` pelas rotas ativas. Cada nó junta o seu nome a `path` e escreve-o no log, por exemplo `PING Server/video1 caminho=['Server', 'Node1', 'Node2', 'Client1']`.

## Quando a rede muda

| Acontecimento | O que o router faz |
|---|---|
| O Node2 cai | O overlay chama `on_neighbor_down("Node2")`. O router apaga-o da tabela, escolhe outra origem (Node3) e envia `ACTIVATE` por lá. O vídeo volta pelo novo caminho. |
| Um link fica lento | O PROBE mede o novo atraso. No ANNOUNCE seguinte o custo sobe e a rota muda, se a alternativa for mais de 10% melhor (histerese contra oscilações). |
| O link volta ao normal | O ANNOUNCE seguinte volta a escolher o caminho bom e o ramo antigo expira. |
| O servidor desaparece | Sem ANNOUNCE durante 15 s, a rota expira. |

## A tabela de rotas (o que o enunciado pede)

O comando `routes` na consola do `ott.py` mostra, por exemplo no Node1:
```
{Fluxo: Server/video1, Origem: Server, Metrica: 1.05, Destinos: ['Node2', 'Node3'], Estado: ativa}
atraso das ligacoes (RTT/2): Node2=0.05ms, Node3=0.04ms, Server=0.05ms
```

| Campo do enunciado | No código (`tabela.py`, classe `Rota`) |
|---|---|
| Fluxo | `stream` (`"Servidor/video"`) |
| Origem | `origem`: o vizinho de onde chega o vídeo |
| Custo / Métrica | `metrica`: custo pela origem |
| Destinos | `destinos`: os vizinhos (ou `LOCAL`, o cliente deste nó) que pediram |
| Estado | `estado`: ativa se houver destinos |
| (extra) | `candidatos`: todos os vizinhos que anunciaram, com o custo de cada um, o que dá rotas alternativas imediatas |

## Os teus ficheiros

| Ficheiro | Para que serve |
|---|---|
| `routing/tabela.py` | A tabela: guardar candidatos, escolher a origem, ativar, desativar, expirar. Sem rede, por isso é fácil de testar. |
| `routing/router.py` | As mensagens (ANNOUNCE, ACTIVATE, DEACTIVATE, PING, PROBE) e o reenvio dos pacotes de vídeo. |
| `routing/test_routing.py` | 19 testes com a rede simulada em memória (Cenário 2 e cenário final, atraso por link). |
| `routing/README.md` | Decisões técnicas e limitações, para o relatório. |
| `no_ott_router.py` | Demo só do routing, sem bootstrapper nem vídeo real. |

## Ver a funcionar

```bash
cd routing && python3 -m unittest -v test_routing    # cada teste tem o nome do que prova
python3 tests/test_integration_local.py              # na raiz: 6 nós reais, mata o Node2 a meio
```
No CORE: `routes` em cada nó, `ping` no servidor, e um link com atraso (duplo clique no link → delay) para ver a rota mudar.

## O que ainda depende de ti

1. Fazer as provas no CORE (tabelas, PING, falha, atraso) e guardar prints e números para o relatório.
2. Comparar `--metric delay` com `--metric hops` num cenário com um link lento: é uma boa secção de "testes e resultados".
3. Combinar com o Pedro uma deteção de falhas mais rápida: hoje são 6 s sem HELLO, e é isso que faz o vídeo congelar cerca de 5 s.
