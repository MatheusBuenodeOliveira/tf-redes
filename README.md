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
- Split Horizon é aplicado: não reenviamos rotas para o vizinho de onde foram aprendidas.
