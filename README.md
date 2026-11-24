# ESR — TP2: Serviço Over the Top para entrega de multimédia

Universidade do Minho · Engenharia de Serviços em Rede · 2026/2027

Protótipo de um serviço OTT: um conjunto de nós forma uma rede overlay aplicacional sobre UDP, descobre os melhores caminhos desde os servidores de vídeo até aos clientes e entrega o vídeo replicando-o só onde os caminhos se separam. Tudo é testado no emulador CORE.

| Módulo | Responsável | Pasta | Etapas do enunciado |
|---|---|---|---|
| 1. Overlay: entrada, saída e manutenção de vizinhos | Pedro | `Mixed_testing/` | 0 e 1 |
| 2. Routing: rotas, métricas, replicação, falhas | João | `routing/` | 2 e 3 |
| 3. Streaming: servidor e cliente de vídeo | Patz | `streaming/` | 4 |
| Integração: um nó completo | todos | `ott.py` | — |

O contrato entre os módulos (mensagens, funções, temporizadores) está em **[`INTERFACES.md`](INTERFACES.md)**. É o primeiro ficheiro a ler.

---

## Estrutura do projeto

```
.
├── ott.py                     ★ programa de cada nó: overlay + routing + streaming
├── INTERFACES.md              ★ contrato entre módulos (mensagens, funções, timers)
├── README.md                  este ficheiro
│
├── Mixed_testing/             Módulo 1 — Overlay (Pedro)
│   ├── no_ott_mixed.py        nó overlay: bootstrapper + vizinhos manuais, HELLO, LEAVE
│   ├── lancar_overlay.py      arranca/pára/inspeciona todos os nós do CORE (vcmd)
│   ├── cenario1.json          topologia overlay do Cenário 1 (4 nós)
│   ├── cenario2.json          topologia overlay do Cenário 2 (6 nós)
│   └── cenarioF.json          topologia overlay do Cenário final (figura 1)
│
├── routing/                   Módulo 2 — Routing (João)
│   ├── tabela.py              tabela de rotas (lógica pura, sem rede)
│   ├── router.py              ANNOUNCE/ACTIVATE/DEACTIVATE/PING/PROBE, replicação
│   ├── test_routing.py        27 testes com rede simulada (atraso por ligação, sem ciclos)
│   └── README.md              decisões técnicas e limitações do routing
│
├── streaming/                 Módulo 3 — Streaming (Patz)
│   ├── video_stream.py        lê o .Mjpeg frame a frame
│   ├── packet.py              formato do pacote de vídeo; fragmenta e reconstrói
│   ├── transport.py           UdpTransport (testes) e OverlayTransport (via router)
│   ├── server.py              StreamSender: envia streams a 20 frames/s
│   ├── client.py              Receiver: buffer e reprodução (janela ou --no-gui)
│   ├── test_local.py          testes sem rede (pedaços baralhados, 2% de perdas)
│   ├── videos/movie.Mjpeg     vídeo de teste do livro
│   └── README.md              detalhes do módulo de streaming
│
├── topology/                  Cenários do CORE (.xml)
│   ├── Cenario_1.xml          4 nós: servidor, nó intermédio, 2 clientes
│   ├── Cenario_2.xml          6 nós: servidor, 3 intermédios, 2 clientes
│   └── Cenario_final.xml      rede complexa da figura 1 do enunciado
│
├── tests/
│   └── test_integration_local.py   6 nós ott.py reais em 127.0.0.1, com falha de um nó
│
├── no_ott_router.py           demo só de routing (UDP, vizinhos manuais, frames de texto)
├── exemplos/no_ott.py         Etapa 0/1: nó UDP full-duplex com HELLO
└── ligacao_simples/           Etapa 0: cliente/servidor UDP de eco
```

Os ficheiros com ★ são os da solução final. `exemplos/`, `ligacao_simples/` e `no_ott_router.py` são passos intermédios, úteis para a avaliação intermédia e para testar cada módulo isoladamente.

---

## Como tudo funciona

### Arquitetura de um nó

Cada máquina do overlay corre o mesmo programa, `ott.py`, com um único socket UDP na porta 5000. As opções decidem o papel do nó: bootstrapper, servidor, cliente ou simplesmente intermédio.

```
                         ott.py (um processo por nó)
 ┌──────────────────────────────────────────────────────────────────────┐
 │  Streaming (Patz)        StreamSender ──send_data──┐   Receiver ◄─┐  │
 │                                                     ▼              │  │
 │  Routing (João)          Router: tabela de rotas, ANNOUNCE,   on_data │
 │                          ACTIVATE, replicação, PROBE ─────────┘      │
 │                                 ▲ on_receive        │ send(to, data)  │
 │  Overlay (Pedro)         Node: vizinhos, HELLO, REGISTER, LEAVE       │
 │                                 ▲                   ▼                 │
 │                    ciclo de receção único    socket UDP :5000         │
 └──────────────────────────────────────────────────────────────────────┘
```

- **Ciclo de receção** (`OttNode.receive_loop`): separa cada datagrama. `DATA|…` e as mensagens de routing vão para o `Router`; as restantes (REGISTER, HELLO, …) vão para o overlay.
- **Thread de tempo** (`OttNode.tick_loop`, de 0,2 em 0,2 s): chama `router.tick()` e avisa o router quando um vizinho desaparece (`on_neighbor_down`).
- **Lock único:** o `Router` não é thread-safe, por isso todas as chamadas passam pelo mesmo `RLock`.

### 1. Construção do overlay (Pedro)

Estratégia mista: bootstrapper com recurso aos vizinhos manuais.

1. O **bootstrapper** (normalmente o servidor) lê a topologia de um JSON (`cenarioX.json`) e valida-a: IDs, simetria das ligações, grau mínimo e conectividade.
2. Um **nó novo** envia `REGISTER` ao bootstrapper e recebe `NEIGHBORS` com a sua lista de vizinhos. Se não houver resposta (3 tentativas de 2 s), usa os vizinhos dados em `--neighbors`.
3. Os vizinhos trocam `HELLO` de 2 em 2 s. Sem HELLO durante 6 s, o vizinho é dado como em baixo. Um nó que sai de forma controlada envia `LEAVE`.
4. Se um nó ficar com menos vizinhos do que `min_degree`, pede um substituto ao bootstrapper (`NEED_NEIGHBOR`).
5. Os nós voltam a registar-se de 5 em 5 s. O bootstrapper esquece um nó ao fim de 15 s sem REGISTER.

### 2. Construção das rotas (João): Estratégia 1 do enunciado

Exemplo com o Cenário 2: `Server – Node1 – {Node2, Node3}`, `Node2 – Node3`, `Node2 – Client1`, `Node3 – Client2`.

1. **Medição das ligações.** De 1 em 1 s cada nó envia `PROBE` a cada vizinho e mede o RTT pelo `PROBE_REPLY`. O atraso de uma ligação é RTT/2, suavizado por uma média móvel exponencial.
2. **Anúncio.** De 5 em 5 s o servidor envia `ANNOUNCE` (stream, servidor, `delay = 0`, `hops = 0`, `path = [servidor]`, `seq` novo) a todos os vizinhos.
3. **Inundação controlada.** Cada nó soma a `delay` o atraso medido da ligação por onde o anúncio chegou, soma 1 a `hops`, junta-se a `path` e guarda o vizinho como candidato. Só reenvia o anúncio se for a primeira cópia desse `seq` ou se trouxer uma métrica melhor. Nunca o devolve a quem lho enviou e ignora anúncios que já passaram por ele (evita ciclos).
4. **Escolha da origem** (métrica definida com o docente): **1.º menor atraso; 2.º, entre atrasos iguais, menos saltos; se ambos forem iguais, fica a atual.** Atrasos a menos de 2 ms ou 10% um do outro contam como iguais, para não trocar de rota por ruído. Só contam candidatos da ronda atual (como no DSDV).
5. **Rotas inativas.** Até haver clientes, as rotas existem mas estão inativas e não há tráfego de dados.

Tabela de rotas do Node1 com os dois clientes ativos (comando `routes`):
```
{Fluxo: Server/video1, Origem: Server, Atraso: 0.00 ms, Saltos: 1, Destinos: ['Node2', 'Node3'], Estado: ativa}
```

### 3. Ativação e entrega do vídeo

1. O cliente (`--watch Server/video1`) envia `ACTIVATE` à sua origem. Cada nó regista quem pediu (destino) e, se a rota estava inativa, envia `ACTIVATE` à sua própria origem, até chegar ao servidor (percurso inverso).
2. O `ACTIVATE` é renovado de 2 em 2 s (soft state). Um destino sem renovação durante 6 s é removido, por isso um cliente que morre sem avisar deixa de receber tráfego sozinho.
3. O servidor (`StreamSender`) lê o `movie.Mjpeg`, parte cada frame em pacotes de até 1300 bytes com cabeçalho próprio (`OTTV`, servidor, stream, seq, fragmento, timestamp) e entrega-os ao router com `send_data`.
4. Cada router envia **uma cópia por destino ativo**: num tronco comum vai uma só cópia, e o pacote só é replicado onde os caminhos se separam. Um pacote que não vem da origem esperada é descartado, o que evita ciclos.
5. O cliente (`Receiver`) junta os fragmentos, deteta frames perdidos ou fora de ordem, faz buffer e reproduz a 20 frames/s.
6. **PING** (Etapa 3): o servidor envia `PING` pelas rotas ativas. Cada nó acrescenta o seu nome a `path` e escreve-o no log.

### 4. Alterações na rede

| Acontecimento | Reação |
|---|---|
| Um vizinho cai ou sai | O overlay deteta-o (6 s sem HELLO, ou LEAVE) → `on_neighbor_down` → o router remove-o da tabela, escolhe outra origem e envia `ACTIVATE` pelo novo caminho e `DEACTIVATE` pelo antigo |
| Uma ligação fica mais lenta | O PROBE mede o novo atraso → no ANNOUNCE seguinte o atraso dessa rota sobe → a rota muda para a alternativa mais rápida, mesmo com mais saltos |
| Um caminho melhor volta a existir | No ANNOUNCE seguinte é escolhido de novo |
| Um servidor deixa de anunciar | A rota expira ao fim de 15 s |
| Um cliente desaparece | O destino expira ao fim de 6 s; os ramos sem clientes desligam-se |

### 5. Vários servidores e várias streams

O ID de uma stream inclui o servidor (`Server1/video1`, `Server2/video1`), por isso cada fluxo tem a sua entrada na tabela e as suas próprias rotas. Um servidor oferece várias streams repetindo `--serve`. Cada cliente escolhe uma com `--watch`.

---

## Como correr

### Testes sem CORE
```bash
cd routing && python3 -m unittest -v test_routing && cd ..   # routing, < 1 s
python3 streaming/test_local.py                               # streaming, sem rede
python3 tests/test_integration_local.py                       # 3 módulos, 6 nós, ~35 s
```

### No CORE
Abrir o cenário (`topology/*.xml`), iniciar a sessão e, em cada nó (Shell do nó, na pasta do repositório):

```bash
# Cenário 1 (IPs de Mixed_testing/cenario1.json)
python3 ott.py --id Server  --config Mixed_testing/cenario1.json --serve video1=streaming/videos/movie.Mjpeg
python3 ott.py --id Node    --bootstrapper 10.0.0.20:5000
python3 ott.py --id Client1 --bootstrapper 10.0.0.20:5000 --watch Server/video1
python3 ott.py --id Client2 --bootstrapper 10.0.0.20:5000 --watch Server/video1 --no-gui
```

Opções úteis: `--serve NOME=FICHEIRO` (pode repetir-se), `--watch SERVIDOR/NOME`, `--neighbors ID=IP:PORTA` (recurso), `--no-gui`, `--fps`, `--buffer`. Ver `python3 ott.py --help`.

Consola dos nós sem vídeo: `neighbors` (vizinhos), `routes` (tabela de rotas e atraso medido de cada ligação), `ping` (no servidor) e `quit`.

Para testar a métrica: no CORE, duplo clique num link → *delay* (ex. 50 ms) e ver a rota mudar com `routes`.

---

## Requisitos mínimos do guião: estado atual

| # | Requisito mínimo (enunciado) | Parte | Estado |
|---|---|---|---|
| 1 | Um nó overlay é uma aplicação executada manualmente numa máquina da rede | Integração | ✅ `ott.py` |
| 2 | Os nós podem ser criados e destruídos dinamicamente | Overlay | ✅ REGISTER/LEAVE, deteção por HELLO |
| 3 | Cada nó estabelece e mantém ligações (UDP) com X vizinhos | Overlay | ⚠️ funciona, mas `min_degree` = 1 nos cenários |
| 4 | Ao arrancar, o nó conhece pelo menos um nó (bootstrapper) que lhe dá a lista de vizinhos | Overlay | ✅ `--bootstrapper`, com recurso a `--neighbors` |
| 5 | Tabela de rotas por fluxo (Fluxo, Custo, Origem, Destinos, Estado) | Routing | ✅ `routing/tabela.py` |
| 6 | Rotas alternativas e vários clientes em simultâneo | Routing | ✅ testado em simulação e localmente; ⚠️ falta no CORE |
| 7 | Rede monitorizada; rotas atualizadas com entrada/saída de nós e mudanças nas condições da rede | Routing + Overlay | ✅ PROBE + ANNOUNCE periódico; ⚠️ recuperação lenta (~5 s) |
| 8 | Os nós reenviam todos os dados de acordo com a tabela | Routing | ✅ replicação por destino ativo, anti-ciclos |
| 9 | Pelo menos 2 servidores, cada um com pelo menos 2 streams | Streaming + Integração | ⚠️ suportado (`--serve` repetido), falta configurar e testar no CORE |
| 10 | Métricas da rede na construção das rotas | Routing | ✅ 1.º atraso medido, 2.º saltos (desempate), como definido com o docente |
| — | Etapa 3: PING que regista o caminho | Routing | ✅ |
| — | Etapa 4: servidor lê o vídeo e envia pacotes numerados; cliente reproduz numa janela | Streaming | ✅ |
| — | Cenários de teste 1, 2 e 3 | Todos | ⚠️ XML e JSON prontos, faltam os testes no CORE |

✅ cumprido · ⚠️ falta alguma coisa para garantir o mínimo (detalhe abaixo)

---

## O que melhorar em cada parte

🔴 = necessário para garantir os mínimos · 🟡 = melhora a nota · 🟢 = opcional

### Módulo 1 — Overlay (Pedro)

- 🔴 **Mais vizinhos por nó (requisito 3).** Todos os cenários têm `"min_degree": 1`. O enunciado sugere pelo menos N vizinhos (ex. N = 3). Subir para 2 nos cenários 2 e final, e garantir no JSON que os nós intermédios têm pelo menos 2 ligações. Sem isto pode não haver rota alternativa quando um nó cai (requisito 6).
- 🔴 **Mudanças de nomes** (secção 6 do `INTERFACES.md`): `id` → `from`, `porta` → `port`, `motivo` → `reason`, `vizinhos` → `neighbors`, `excluir` → `exclude`, `texto` → `text`, e `"substituto"` → `"replacement"`. Acrescentar `from` a NEIGHBORS e ERROR. Depois atualizar o `ott.py`, que usa `n["estado"] == "ativo"`.
- 🔴 **Cenário final.** O `cenarioF.json` não tem papéis atribuídos e põe os routers n1–n10 no overlay. O enunciado aconselha a correr a aplicação só em sistemas terminais. Ou se justifica a escolha no relatório (ou com o docente), ou se reduz o overlay aos PCs mais alguns nós intermédios, como na figura 1 (S1 = n16; clientes n17, n18, n19, n22, n23).
- 🟡 **Deteção de falhas mais rápida.** Um vizinho só é dado como em baixo ao fim de 6 s sem HELLO, e é isso que faz o vídeo congelar cerca de 5 s quando um nó cai. Baixar para HELLO de 1 s e queda após 3 s, ou deixar o router declarar a queda após 3 PROBE sem resposta.
- 🟢 **Bootstrapper como ponto único de falha.** Se cair, os nós novos só entram com `--neighbors`. Basta documentá-lo como limitação.

### Módulo 2 — Routing (João)

Os requisitos 5, 6, 8 e 10 e a Etapa 3 estão implementados e têm 27 testes automáticos. Falta:

- 🔴 **Provar no CORE (requisitos 6, 7 e 10)** e guardar os resultados para o relatório:
  1. a tabela de rotas (`routes`) em cada nó, nos 3 cenários;
  2. a saída do PING com o caminho, comparada com o melhor caminho esperado;
  3. a replicação: no Cenário 2, com 2 clientes, só uma cópia na ligação comum (ver os logs ou capturar com `tcpdump`);
  4. uma falha: matar um nó intermédio e medir quanto tempo o cliente fica sem vídeo;
  5. a métrica: sem atraso, mostrar que ganha o caminho com menos saltos; com 50 ms num link desse caminho, mostrar a rota a passar para o caminho mais longo mas mais rápido.
- 🟡 **Perdas na métrica.** Hoje só se mede o atraso. Contar os PROBE sem resposta e penalizar o custo (ex. atraso × (1 + 10 × taxa de perda)) faz a rota evitar também ligações com perdas.
- 🟡 **Reação mais rápida a mudanças de atraso.** Um caminho melhor só é adotado no ANNOUNCE seguinte (até 5 s), e a média móvel demora cerca de 10 s a acompanhar uma descida grande. Pode baixar-se o período de ANNOUNCE para 2–3 s ou subir `ALFA_RTT`, à custa de mais tráfego ou de mais oscilação.
- 🟢 Listar no cliente as streams conhecidas (as que estão na tabela de rotas), para o utilizador escolher sem saber os nomes.

### Módulo 3 — Streaming (Patz)

A Etapa 4 está implementada (leitura do `movie.Mjpeg`, pacotes numerados e fragmentados, deteção de perdas e reprodução com buffer). Falta:

- 🔴 **2 servidores × 2 streams no CORE (requisito 9).** O código já o suporta (`--serve video1=… --serve video2=…` em dois servidores diferentes). Falta pô-lo a correr no cenário final, com clientes a ver streams diferentes ao mesmo tempo. Se houver só um ficheiro de vídeo, pode servir-se o mesmo ficheiro com nomes diferentes, mas fica mais convincente com vídeos distintos.
- 🟡 **Atualizar `streaming/README.md`**: ainda diz que `server_id` é o IP e que o `OverlayTransport` está por fazer, e está em inglês.
- 🟡 **Medições no cliente** para o relatório: frames recebidos por segundo, frames perdidos e atraso extremo a extremo (o pacote já leva `timestamp_ms`). Escrevê-las num log de 1 em 1 s.

### Integração (todos)

- 🔴 **`lancar_overlay.py` arranca só o overlay.** Lança o `no_ott_mixed.py` com `--porta`, e não o `ott.py`. Deve passar a lançar o `ott.py` (`--port`, e `--serve`/`--watch` por nó, lidos por exemplo de um campo `"role"` no JSON do cenário). Sem isto, o cenário final obriga a abrir cerca de 20 terminais à mão.
- 🔴 **Correr os 3 cenários de ponta a ponta** com o plano de validação da checklist: Cenário 1 (uma rota e vídeo), Cenário 2 (vários caminhos, 2 clientes, replicação), Cenário 3 (2 servidores, 2 streams cada, vários clientes, rotas alternativas e falhas).
- 🟡 **Arrumar a raiz:** mover `exemplos/`, `ligacao_simples/` e `no_ott_router.py` para `etapas/`, e renomear `Mixed_testing/` para `overlay/` (atualizando os caminhos no `ott.py` e no `INTERFACES.md`).

### Relatório e entrega

- 🔴 Formato artigo, **máximo 12 páginas**, LNCS recomendado. Estrutura: Introdução · Arquitetura da solução · Especificação dos protocolos (formato das mensagens e interações) · Implementação · Testes e resultados · Conclusões e trabalho futuro.
- O `INTERFACES.md` serve de base à especificação dos protocolos, e o `routing/README.md` e o `streaming/README.md` às secções de implementação.
- Diagramas úteis: arranque de um nó (REGISTER → NEIGHBORS → HELLO), ANNOUNCE → ACTIVATE → DATA, e falha de um nó com mudança de rota.
- Submissão no Blackboard: `ESR-TP2-PLx-Gy-Rel.pdf` e `ESR-TP2-PLx-Gy-Codigo.zip`.

### Avaliação (do enunciado)

| Item | Peso | Onde está |
|---|---|---|
| Topologia da rede overlay | 10% | `Mixed_testing/`, `topology/` |
| Gestão dos fluxos de dados | 25% | `routing/` |
| Protocolo aplicacional de controlo | 15% | `INTERFACES.md` (mensagens) |
| Protocolo de streaming | 10% | `streaming/` |
| Solução final integrada | 30% | `ott.py`, testes no CORE, relatório, defesa |
| Avaliação intermédia | 5% + 5% | — |
