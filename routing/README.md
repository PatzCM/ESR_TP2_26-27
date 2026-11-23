# Módulo 2 — Routing (João)

Descobre por onde cada stream deve passar, mantém a tabela de rotas, ativa só os ramos que têm clientes, replica os pacotes e muda de caminho quando a rede muda. Não abre sockets: recebe `send()` e `neighbors()` do overlay (Pedro) e é chamado por ele (`on_receive`, `on_neighbor_down`, `tick`). A interface completa está em [`INTERFACES.md`](../INTERFACES.md), secções 2 e 3.

| Ficheiro | O quê |
|---|---|
| `tabela.py` | Tabela de rotas, lógica pura (sem rede, relógio injetável) |
| `router.py` | Mensagens ANNOUNCE / ACTIVATE / DEACTIVATE / PING e encaminhamento de `DATA` |
| `test_routing.py` | 19 testes com a rede simulada em memória (Cenário 2 e Cenário final), com atraso configurável por ligação |
| `../no_ott_router.py` | Demo só de routing em UDP (vizinhos manuais, "frames" de texto) |

## Decisões técnicas

**Métrica: atraso medido + 1 ms por salto** (o enunciado pede "métricas da rede"). De 1 em 1 s cada nó envia um `PROBE` a cada vizinho com o seu próprio relógio, e o vizinho devolve-o em `PROBE_REPLY`. O RTT é suavizado com uma média móvel exponencial (α = 0,3), e o atraso da ligação é RTT/2. Como cada nó só compara o seu próprio relógio, não é preciso relógios sincronizados. Ao receber um ANNOUNCE, cada nó soma à métrica o custo da ligação por onde ele chegou: atraso + 1 ms. Sem atraso nas ligações, o custo fica igual ao número de saltos. Quando há ligações lentas (por exemplo com atraso configurado no CORE), é o atraso que decide. A penalização de 1 ms por salto desempata a favor do caminho mais curto quando os atrasos são parecidos.

Para não oscilar com o ruído das medições há **histerese**: um nó só troca de origem se a alternativa for mais de 10% melhor. Com `--metric hops` usa-se só o número de saltos, sem sondas, o que serve para comparar as duas métricas no relatório.

**Descoberta: flooding controlado a partir do servidor.** De 5 em 5 s cada servidor envia um ANNOUNCE por stream com um `seq` novo. Um nó só reenvia se for a primeira cópia desse `seq` ou se trouxer um custo melhor do que o que já viu nessa ronda, e nunca o devolve ao vizinho de onde veio. Assim cada ronda termina e não há tempestades de mensagens. O `seq` começa no relógio do sistema para não recomeçar em 0 se o servidor reiniciar.

**Tabela de rotas** (uma entrada por stream):

| Campo | Significado |
|---|---|
| stream / servidor | `Server1/video1` / `Server1` |
| candidatos | vizinho → (custo, último anúncio) |
| origem | vizinho a montante escolhido (menor custo; em empate mantém o atual para não oscilar) |
| métrica | custo pela origem |
| destinos | vizinhos (ou `LOCAL`, o cliente deste nó) que pediram a stream |
| estado | ativa se tiver pelo menos um destino |

**Ativação: caminho inverso, soft state.** O cliente envia ACTIVATE à sua origem; cada nó regista o destino e, se a rota passou a ativa, envia ACTIVATE à sua própria origem, até ao servidor. O pedido é renovado de 2 em 2 s; um destino que não é renovado em 6 s expira. Se um cliente morrer sem enviar DEACTIVATE, o ramo desliga-se sozinho.

**Replicação: árvore por stream.** Um pacote é enviado uma vez por destino ativo, por isso num tronco comum segue uma única cópia e só se divide onde os caminhos se separam. Rotas inativas não geram tráfego.

**Anti-ciclos.** Um pacote `DATA` só é aceite se vier da origem atual da rota; o resto é descartado e contado em `estatisticas["dados_descartados"]`.

**Atualização dinâmica.** Quando o overlay avisa que um vizinho caiu, as suas entradas saem da tabela, a origem é recalculada e o ACTIVATE segue pelo novo caminho (DEACTIVATE para o antigo). Uma rota que deixa de receber ANNOUNCE expira em 15 s. Quando um caminho melhor volta a existir, ou uma ligação fica mais lenta ou mais rápida, a ronda seguinte de ANNOUNCE escolhe o novo caminho.

**Vários servidores e streams.** O ID da stream inclui o servidor (`Server1/video1`), por isso streams com o mesmo nome em servidores diferentes nunca se misturam. Cada servidor anuncia todas as suas streams na mesma ronda.

**Registo de percursos.** O PING segue as rotas ativas e acumula a lista de nós em `path`; cada nó escreve o percurso no log. Com `logging.DEBUG` cada `DATA` reencaminhado também fica no log.

## Limitações

- A recuperação depende do overlay: um vizinho só é dado como em baixo após 6 s sem HELLO, por isso o vídeo para cerca de 5 s quando um nó cai.
- Um caminho mais curto só é adotado na ronda de ANNOUNCE seguinte (até 5 s).
- A métrica não mede a largura de banda nem as perdas, só o atraso. Um link congestionado só é evitado se a fila fizer subir o RTT.
- A média móvel demora alguns segundos a acompanhar uma mudança de atraso (cerca de 10 s para uma descida de 50 ms para 0). Somado ao período de ANNOUNCE, o desvio para um caminho melhor demora 10 a 15 s.
- O `PROBE` é enviado a todos os vizinhos de 1 em 1 s: cerca de 100 bytes/s por ligação.
- O `Router` não é thread-safe: cada nó tem de usar um único lock à volta de todas as chamadas.

## Testar

```bash
cd routing && python3 -m unittest -v test_routing      # 19 testes, < 1 s
python3 tests/test_integration_local.py                # 3 módulos, 6 nós, ~35 s (na raiz)
```

No CORE, a consola do `ott.py` tem `routes` (tabela do nó e atraso medido de cada ligação) e `ping` (no servidor). Para testar a métrica, configura atraso num link do CORE (duplo clique no link → delay) e vê a rota mudar com `routes`.
