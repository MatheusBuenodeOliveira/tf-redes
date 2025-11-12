# tf-redes

Implementação em Python de um roteador simples UDP para o trabalho final de Fundamentos de Redes.

Como usar

1. Copie `roteadores.txt.example` para `roteadores.txt` e edite os IPs dos vizinhos (um por linha).
2. Inicie o roteador informando o IP deste nó:

```bash
python router.py --ip 192.168.1.2
```

Comandos interativos do roteador:
- send <dest> <mensagem>  -- envia mensagem de texto para destino
- table                   -- exibe tabela de roteamento
- neighbors               -- exibe vizinhos e estado
- quit                   -- encerra o roteador

Observações
- O socket UDP usa a porta 6000 conforme especificado.
- O anúncio de rotas é enviado a cada 10s. Vizinho sem updates por 15s é considerado inativo.
- Split Horizon DESATIVADO (a pedido): anúncios incluem todas as rotas, mesmo as aprendidas do próprio vizinho.

Mapeamento para o enunciado (PDF):

- Parte 1 — Inicialização e Tabela de Roteamento
	- O arquivo `roteadores.txt` contém um IP por linha. Copie `roteadores.txt.example` para `roteadores.txt` e edite os IPs.
	- Ao iniciar, o roteador lê esse arquivo e insere entradas na tabela com métrica 1 e próximo salto = vizinho (veja `Router._load_neighbors` e `Router.__init__`).

- Parte 2 — Atualização de Rotas
	- O roteador envia anúncios a cada 10s (veja `Router._periodic_tasks`).
	- Ao receber anúncios ('*'), a função `Router.process_message` aplica as regras de atualização (novas rotas, melhoria de métrica, retirada de rotas) e, em caso de mudança, reenvia a tabela imediatamente.

- Parte 3 — Detecção de Falhas
	- O roteiro atualiza `last_seen` ao receber qualquer mensagem UDP de um vizinho (`Router._recv_loop`). Se um vizinho não enviar atualizações por 15s, ele é marcado como inativo e as rotas via ele são removidas (`Router._periodic_tasks`).

- Parte 4 — Protocolo de Comunicação
	- Mensagens suportadas:
		- Anúncio de roteador: `@<meu_ip>` (enviado ao entrar na rede) — implementado em `_send_announce` e processado em `process_message`.
		- Anúncio de rotas: `*<ip>;<métrica>*...` — construção em `_build_advertisement_for_neighbor` e parsing em `parse_advert`.
		- Mensagem de texto: `!orig;dest;mensagem` — parsing em `parse_text_message` e encaminhamento em `process_message`/`send_text`.

- Parte 6 — Envio de Mensagens entre os roteadores
	- Use o comando interativo `send <dest> <mensagem>` no prompt do roteador para enviar mensagens de texto. O roteador roteia a mensagem usando a tabela local.

Como rodar os testes

- Testes unitários (parsing):

```bash
/workspaces/tf-redes/.venv/bin/python -m pytest tests/test_router_parsing.py -q
```

- Teste de integração (simula dois roteadores em memória sem bindar sockets):

```bash
/workspaces/tf-redes/.venv/bin/python -m pytest tests/test_integration.py -q
```

Observações para desenvolvimento

- Para instanciar um `Router` em testes sem que ele tente bindar a porta 6000, use `Router(<ip>, neighbors_file="roteadores.txt", bind_socket=False)`. Isso permite chamar `process_message` diretamente em testes.
- O arquivo `router.py` contém comentários que mapeiam cada trecho do código para as Partes do enunciado (procure por "Parte X" no código).
