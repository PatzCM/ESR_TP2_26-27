# Módulo 2 — Routing (João)

Descobre por onde cada stream deve passar, mantém a tabela de rotas, ativa só os ramos que têm clientes, replica os pacotes e muda de caminho quando a rede muda. Não abre sockets: recebe `send()` e `neighbors()` do overlay (Pedro) e é chamado por ele (`on_receive`, `on_neighbor_down`, `tick`). A interface completa está em [`INTERFACES.md`](../INTERFACES.md), secções 2 e 3.

| Ficheiro | O quê |
|---|---|
| `tabela.py` | Tabela de rotas, lógica pura (sem rede, relógio injetável) |
| `router.py` | Mensagens ANNOUNCE / ACTIVATE / DEACTIVATE / PING e encaminhamento de `DATA` |
| `test_routing.py` | 27 testes com a rede simulada em memória (Cenário 2 e Cenário final), com atraso configurável por ligação |
| `../no_ott_router.py` | Demo só de routing em UDP (vizinhos manuais, "frames" de texto) |

## Decisões técnicas

**Métrica (definida com o docente): 1.º atraso, 2.º número de saltos.**
- **1.º critério, atraso:** ganha o menor. De 1 em 1 s cada nó envia um `PROBE` a cada vizinho com o seu próprio relógio, e o vizinho devolve-o em `PROBE_REPLY`. O RTT é suavizado com uma média móvel exponencial (α = 0,3), e o atraso da ligação é RTT/2. Como cada nó só compara o seu próprio relógio, não é preciso relógios sincronizados. Cada `ANNOUNCE` leva o atraso acumulado desde o servidor (`delay`), e cada nó soma-lhe o atraso da ligação por onde ele chegou.
- **2.º critério, saltos:** só desempata quando os atrasos são iguais. Cada nó soma 1 a `hops`.
- **Ambos iguais:** qualquer um serve, por isso fica a origem atual e a rota não oscila.
- **O que conta como "igual":** o atraso medido nunca é exatamente igual, por isso dois atrasos contam como iguais se diferirem no máximo 2 ms ou 10% (o que for maior). Isto também evita trocar de rota por causa do ruído das medições. Sem atraso configurado nas ligações (como no CORE por omissão), todos os caminhos têm atraso "igual" e decide o número de saltos.

A comparação está em `tabela.py` (`melhor_que` e `_escolher`).

**Sem ciclos.** Com o atraso como 1.º critério, uma rota antiga (por exemplo medida antes de haver PROBE, com "0 ms") podia parecer melhor do que a verdadeira e criar ciclos entre nós. Há três proteções:
1. O `ANNOUNCE` leva o caminho percorrido (`path`). Um nó ignora os anúncios que já passaram por ele e apaga esse vizinho dos candidatos.
2. Cada cópia é reenviada com a sua própria métrica, nunca com uma rota guardada de uma ronda antiga.
3. Só contam os candidatos que anunciaram na ronda atual (o mesmo `seq`), como no protocolo DSDV. A origem atual tem 1 s de tolerância no início de cada ronda, à espera da sua cópia, e mantém-se se nenhuma cópia chegar (anúncio perdido).

Num teste com 30 redes aleatórias (atrasos e falhas aleatórios no cenário final), nunca houve ciclos fora desse primeiro segundo de cada ronda. Durante esse segundo pode haver um ciclo passageiro, mas os dados não andam às voltas, porque um nó só aceita vídeo vindo da sua origem.

**Tabela de rotas** (uma entrada por stream):

| Campo | Significado |
|---|---|
| stream / servidor | `Server1/video1` / `Server1` |
| candidatos | vizinho → ((atraso, saltos), último anúncio), mais o `path` e o `seq` de cada um |
| origem | vizinho a montante escolhido (menor atraso; desempate por saltos; em empate total mantém o atual) |
| atraso / saltos | métrica pela origem |
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
cd routing && python3 -m unittest -v test_routing      # 27 testes, ~5 s
python3 tests/test_integration_local.py                # 3 módulos, 6 nós, ~35 s (na raiz)
```

No CORE, a consola do `ott.py` tem `routes` (tabela do nó e atraso medido de cada ligação) e `ping` (no servidor). Para testar a métrica, configura atraso num link do CORE (duplo clique no link → delay) e vê a rota mudar com `routes`.
