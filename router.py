#!/usr/bin/env python3
"""
Router UDP simples (versão final, limpa e única).

Implementa o protocolo do trabalho conforme o PDF de especificação.

Mapeamento rápido (referência às "Partes" do enunciado):
- Parte 1 (Inicialização e Tabela de Roteamento): leitura de `roteadores.txt` e
    criação da tabela inicial em `Router._load_neighbors` e `Router.__init__`.
- Split Horizon: originalmente implementado em `_build_advertisement_for_neighbor`.
    ATENÇÃO: Split Horizon DESATIVADO por solicitação — anúncios incluem rotas
    independentemente do próximo salto.
- Parte 2 (Atualização de Rotas): envio periódico em `_periodic_tasks` (a cada
    10s) e processamento de anúncios em `process_message` (tokens '*' e lógica de
    atualização/remoção). Quando a tabela muda, `_broadcast_table` envia a tabela
    imediatamente aos vizinhos.
- Parte 3 (Detecção de Falhas): `last_seen` atualizada em `_recv_loop`; vizinhos
    são considerados inativos após 15s dentro de `_periodic_tasks` e rotas por
    eles são removidas.
- Parte 4 (Protocolo de Comunicação): formatos das mensagens (`@`, `*`, `!`) são
    tratados por `parse_advert`, `parse_text_message`, `_send_announce` e
    `process_message`. Mensagens UDP enviadas para porta 6000.
- Parte 6 (Envio de Mensagens entre roteadores): `send_text` e encaminhamento
    em `process_message` quando uma mensagem '!' não é para este nó.

Os comentários próximos às funções explicam qual parte do enunciado cada bloco
implementa. Todas as mensagens usam UDP/porta 6000.
"""
import argparse
import socket
import threading
import time
from typing import Dict, Tuple, Set, List

PORT = 6000
ADVERT_INTERVAL = 10
NEIGHBOR_TIMEOUT = 15


def parse_advert(msg: str) -> List[Tuple[str, int]]:
    # Parte 4: função que interpreta a mensagem de anúncio de rotas
    # Formato: *<ip>;<metric>*<ip>;<metric> ...
    items: List[Tuple[str, int]] = []
    if not msg:
        return items
    tokens = [t for t in msg.split("*") if t]
    for tok in tokens:
        try:
            ip_part, metric_part = tok.split(";", 1)
            metric = int(metric_part)
            items.append((ip_part, metric))
        except Exception:
            continue
    return items


def parse_text_message(msg: str) -> Tuple[str, str, str]:
    # Parte 6: parsing de mensagens de texto do formato '!orig;dest;mensagem'
    if not msg or not msg.startswith("!"):
        raise ValueError("Formato inválido: não começa com '!'")
    rest = msg[1:]
    parts = rest.split(";", 2)
    if len(parts) != 3:
        raise ValueError("Formato inválido de mensagem de texto")
    return parts[0], parts[1], parts[2]


class Router:
    def __init__(self, my_ip: str, neighbors_file: str = "roteadores.txt", bind_socket: bool = True):
        # Parte 1: inicialização do roteador e tabela de roteamento
        # `my_ip`: IP deste roteador. O arquivo `neighbors_file` deve conter
        # os IPs dos vizinhos diretos (um por linha). Esses vizinhos são
        # inseridos na tabela com métrica=1 e próximo salto igual ao próprio
        # vizinho.
        self.my_ip = my_ip
        self.neighbors_file = neighbors_file
        self.neighbors: Set[str] = set()
        self.routing_table: Dict[str, Tuple[int, str]] = {}
        self.last_seen: Dict[str, float] = {}
        self.last_advertised_by_neighbor: Dict[str, Set[str]] = {}
        self.sock = None
        if bind_socket:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(("0.0.0.0", PORT))
            self.sock.settimeout(1.0)
        self.lock = threading.Lock()
        self.running = True
        self._load_neighbors()
        for n in self.neighbors:
            if n != self.my_ip:
                self.routing_table[n] = (1, n)

    def _load_neighbors(self):
        # Parte 1: leitura do arquivo `roteadores.txt` (vizinho por linha)
        try:
            with open(self.neighbors_file, "r", encoding="utf-8") as f:
                for line in f:
                    ip = line.strip()
                    if ip:
                        self.neighbors.add(ip)
        except FileNotFoundError:
            print(f"Arquivo '{self.neighbors_file}' não encontrado. Nenhum vizinho carregado.")

    def start(self):
        self._send_announce()
        if self.sock:
            threading.Thread(target=self._recv_loop, daemon=True).start()
        threading.Thread(target=self._periodic_tasks, daemon=True).start()
        try:
            self._cli_loop()
        except KeyboardInterrupt:
            print("\nEncerrando...")
        finally:
            self.running = False
            time.sleep(0.5)

    def _sendto(self, msg: str, ip: str):
        if not self.sock:
            raise RuntimeError("Socket não inicializado para envio")
        try:
            self.sock.sendto(msg.encode("utf-8"), (ip, PORT))
        except Exception as e:
            print(f"Erro enviando para {ip}: {e}")

    def _send_announce(self):
        # Parte 4 - Mensagem de Anúncio de Roteador (@<meu_ip>)
        # Envia anúncio inicial para todos os vizinhos configurados.
        msg = f"@{self.my_ip}"
        for n in list(self.neighbors):
            if n == self.my_ip:
                continue
            try:
                self._sendto(msg, n)
            except RuntimeError:
                pass

    def _build_advertisement_for_neighbor(self, neighbor: str) -> str:
        # Parte 2: constrói a mensagem de anúncio para um vizinho específico.
        # Split Horizon DESATIVADO: não filtramos rotas cujo próximo salto é
        # o próprio vizinho.
        parts: List[str] = []
        with self.lock:
            for dest, (metric, next_hop) in self.routing_table.items():
                if dest == self.my_ip:
                    # não anunciamos rotas para nós mesmos
                    continue
                # if next_hop == neighbor:
                #     # Split Horizon (DESATIVADO): condicional comentada para
                #     # permitir anúncio de rotas aprendidas do próprio vizinho
                #     continue
                parts.append(f"*{dest};{metric}")
        return "".join(parts)

    def _periodic_tasks(self):
        # Parte 2 & Parte 3: tarefas periódicas
        # - envia anúncios a cada 10 segundos (ADVERT_INTERVAL)
        # - verifica tempo desde último update de vizinhos e detecta
        #   vizinhos inativos após NEIGHBOR_TIMEOUT
        last_advert_time = 0.0
        while self.running:
            now = time.time()
            if now - last_advert_time >= ADVERT_INTERVAL:
                for n in list(self.neighbors):
                    if n == self.my_ip:
                        continue
                    advert = self._build_advertisement_for_neighbor(n)
                    if advert:
                        try:
                            self._sendto(advert, n)
                        except RuntimeError:
                            pass
                last_advert_time = now
            with self.lock:
                for n, last in list(self.last_seen.items()):
                    # Parte 3: se não vimos o vizinho por mais de NEIGHBOR_TIMEOUT
                    # segundos, consideramos inativo e removemos rotas através dele
                    if now - last > NEIGHBOR_TIMEOUT:
                        print(f"Vizinho {n} considerado inativo (sem updates por {NEIGHBOR_TIMEOUT}s)")
                        # remover rotas cujo próximo salto é o vizinho inativo
                        removed = [d for d, (_, nh) in list(self.routing_table.items()) if nh == n]
                        for d in removed:
                            del self.routing_table[d]
                        # limpar relógios/estado do vizinho
                        del self.last_seen[n]
                        if n in self.last_advertised_by_neighbor:
                            del self.last_advertised_by_neighbor[n]
                        # avisar demais vizinhos sobre a alteração
                        for neigh in list(self.neighbors):
                            if neigh == self.my_ip:
                                continue
                            try:
                                advert = self._build_advertisement_for_neighbor(neigh)
                                if advert:
                                    self._sendto(advert, neigh)
                            except RuntimeError:
                                pass
            time.sleep(1)

    def _recv_loop(self):
        while self.running and self.sock:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Erro no socket: {e}")
                continue
            msg = data.decode("utf-8", errors="replace").strip()
            src_ip = addr[0]
            # Parte 3: atualizar timestamp de último contato com o vizinho
            with self.lock:
                self.last_seen[src_ip] = time.time()
            if not msg:
                continue
            # delegar o processamento (testável via chamadas diretas a process_message)
            self.process_message(msg, src_ip)

    def _broadcast_table(self):
        for n in list(self.neighbors):
            if n == self.my_ip:
                continue
            advert = self._build_advertisement_for_neighbor(n)
            if advert:
                try:
                    self._sendto(advert, n)
                except RuntimeError:
                    pass

    def process_message(self, msg: str, src_ip: str):
        # processa mensagens conforme Parte 4 (protocolos @, * e !) e Parte 2/6
        if msg.startswith("@"):
            # Parte 4 - Anúncio de roteador: @<ip>
            # Quando um roteador entra na rede ele anuncia seu IP para que os
            # vizinhos o adicionem com métrica 1 e próximo salto = endereço
            # do remetente (src_ip).
            announced_ip = msg[1:].strip()
            if announced_ip and announced_ip != self.my_ip:
                with self.lock:
                    self.routing_table[announced_ip] = (1, src_ip)
                print(f"Recebido anuncio @ de {src_ip}: adicionar rota para {announced_ip}")
                # enviar imediatamente a tabela atualizada para vizinhos (Parte 2.5)
                self._broadcast_table()
        elif msg.startswith("*"):
            tokens = [t for t in msg.split("*") if t]
            advertised_dests: Set[str] = set()
            changed = False
            with self.lock:
                for tok in tokens:
                    try:
                        ip_part, metric_part = tok.split(";", 1)
                        advertised_dests.add(ip_part)
                        recv_metric = int(metric_part)
                        new_metric = recv_metric + 1
                    except Exception:
                        continue
                    if ip_part == self.my_ip:
                        continue
                    if ip_part not in self.routing_table:
                        self.routing_table[ip_part] = (new_metric, src_ip)
                        changed = True
                    else:
                        cur_metric, cur_nh = self.routing_table[ip_part]
                        if new_metric < cur_metric:
                            self.routing_table[ip_part] = (new_metric, src_ip)
                            changed = True
                        elif cur_nh == src_ip and new_metric != cur_metric:
                            self.routing_table[ip_part] = (new_metric, src_ip)
                            changed = True
                prev = self.last_advertised_by_neighbor.get(src_ip, set())
                withdrawn = prev - advertised_dests
                for d in withdrawn:
                    if d in self.routing_table and self.routing_table[d][1] == src_ip:
                        del self.routing_table[d]
                        changed = True
                self.last_advertised_by_neighbor[src_ip] = advertised_dests
            if changed:
                # Parte 2: quando tabela muda, imprimir e avisar vizinhos
                print("Tabela atualizada a partir de atualização recebida:")
                self.print_table()
                # reenviar imediatamente (propagar mudança) — observa Split Horizon
                self._broadcast_table()
        elif msg.startswith("!"):
            # Parte 6: Mensagem de texto no formato '!orig;dest;mensagem'
            try:
                origin, dest, text = msg[1:].split(";", 2)
            except Exception:
                print(f"Mensagem de texto inválida recebida de {src_ip}: {msg}")
                return
            if dest == self.my_ip:
                # mensagem destinada a este roteador: imprimir e indicar entrega
                print(f"Mensagem recebida para mim: de {origin} para {dest}: {text}")
                print("Chegou ao destino.")
            else:
                # caso contrário, roteamos com base na tabela de roteamento
                with self.lock:
                    entry = self.routing_table.get(dest)
                if not entry:
                    print(f"Não há rota para {dest}. Mensagem descartada.")
                else:
                    next_hop = entry[1]
                    print(f"Repassando mensagem {origin}->{dest} para {next_hop}")
                    try:
                        self._sendto(msg, next_hop)
                    except RuntimeError:
                        pass
        else:
            print(f"Mensagem desconhecida de {src_ip}: {msg}")

    def print_table(self):
        with self.lock:
            print("Tabela de roteamento:")
            print("Destino\tMétrica\tPróximo Salto")
            for dest, (metric, next_hop) in sorted(self.routing_table.items()):
                print(f"{dest}\t{metric}\t{next_hop}")

    def print_neighbors(self):
        with self.lock:
            print("Vizinhos configurados:")
            for n in sorted(self.neighbors):
                last = self.last_seen.get(n)
                status = "ativo" if last and time.time() - last <= NEIGHBOR_TIMEOUT else "inativo"
                print(f"{n}\t{status}")

    def _cli_loop(self):
        print("Comandos: send <dest> <mensagem>, table, neighbors, quit")
        while self.running:
            try:
                line = input("> ").strip()
            except EOFError:
                break
            if not line:
                continue
            parts = line.split(" ", 2)
            cmd = parts[0].lower()
            if cmd == "send":
                if len(parts) < 3:
                    print("Uso: send <dest> <mensagem>")
                    continue
                dest = parts[1]
                message = parts[2]
                self.send_text(self.my_ip, dest, message)
            elif cmd == "table":
                self.print_table()
            elif cmd == "neighbors":
                self.print_neighbors()
            elif cmd in ("quit", "exit"):
                break
            else:
                print("Comando desconhecido")

    def send_text(self, origin: str, dest: str, message: str):
        msg = f"!{origin};{dest};{message}"
        if dest == self.my_ip:
            print("Mensagem destino é este roteador. Entregando localmente.")
            print(f"{origin} -> {dest}: {message}")
            return
        with self.lock:
            entry = self.routing_table.get(dest)
        if not entry:
            print(f"Não há rota para {dest}. Mensagem não enviada.")
            return
        next_hop = entry[1]
        print(f"Enviando mensagem para {dest} via {next_hop}")
        try:
            self._sendto(msg, next_hop)
        except RuntimeError:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True, help="IP deste roteador (ex: 192.168.1.2)")
    parser.add_argument("--neighbors", default="roteadores.txt", help="Arquivo com vizinhos (um IP por linha)")
    args = parser.parse_args()
    r = Router(args.ip, neighbors_file=args.neighbors)
    r.start()


if __name__ == "__main__":
    main()
