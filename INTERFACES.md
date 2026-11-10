# Interfaces Overlay <-> Routing <-> Streaming

Decisoes: **UDP**, **ID de no = IP** (string, o IP "principal" do no), porta fixa (ex.: 5000).

## ID de stream
`"<ip_servidor>/<nome>"`, ex.: `10.0.0.20/video1`. Nao pode conter `|`.

## Formato dos datagramas
- Controlo: JSON numa linha, campos `t` (tipo) e `de` (ID do emissor).
  Routing usa `t` in {ANNOUNCE, ACTIVATE, DEACTIVATE, PING}.
  Overlay (Pedro) usa os seus (REGISTER, HELLO, ...). Quem recebe despacha pelo `t`.
- Dados: `b"DATA|<stream>|<de>|<payload>"`  (payload binario, opaco para o routing).

> Nao usar o IP de origem do `recvfrom` como identidade: os routers CORE tem varias
> interfaces. Usar sempre o campo `de`.

## O que o Pedro fornece ao Router (construtor)
- `enviar(destino_ip: str, dados: bytes)`  -> `sock.sendto(dados, (destino_ip, PORTA))`
- `vizinhos() -> list[str]`                 -> IPs dos vizinhos atuais

## O que o Pedro chama no Router
- `router.on_receber(dados: bytes)`  para cada datagrama de routing/dados recebido
- `router.on_vizinho_caiu(ip)`       quando deteta que um vizinho saiu
- `router.tick()`                    ~1x por segundo

## O que o Patz usa
- Servidor: `router.enviar_dados(stream, payload_bytes)` por cada pacote de video
- Cliente:  `Router(..., on_dados=fn(stream, payload))`, `router.pedir_stream(stream)`, `router.parar_stream(stream)`
- Um datagrama UDP deve caber em ~64 KB (ideal < 1400 B para evitar fragmentacao IP).

## Timers (parametrizaveis)
anuncio do servidor 5 s | refresh de ACTIVATE 2 s | expira destino 6 s | expira rota 15 s
