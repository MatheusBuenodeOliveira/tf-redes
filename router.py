#!/usr/bin/env python3
"""
Router UDP simples que implementa o protocolo do trabalho final.

Funcionalidades:
- Lê neighbors de `roteadores.txt` (um IP por linha).
- Mantém tabela de roteamento (dest, metric, next_hop).
- Envia anúncios de rota a cada 10s (Split Horizon aplicado por vizinho).
- Detecta vizinhos inativos após 15s e descarta rotas aprendidas por eles.
- Permite enviar/encaminhar mensagens de texto no formato: !orig;dest;msg
- Envia anúncio inicial de entrada na rede: @<meu_ip>

Uso:
  python router.py --ip 192.168.1.5

Haverá um loop de entrada interativa com comandos: send, table, neighbors, quit

"""
import argparse
import socket
import threading
import time
from typing import Dict, Tuple, Set, List

PORT = 6000
ADVERT_INTERVAL = 10
NEIGHBOR_TIMEOUT = 15


class Router:
    def __init__(self, my_ip: str, neighbors_file: str = "roteadores.txt"):
        self.my_ip = my_ip
        self.neighbors_file = neighbors_file
        self.neighbors: Set[str] = set()

        # routing_table[dest] = (metric, next_hop)
        self.routing_table: Dict[str, Tuple[int, str]] = {}

        # last seen time for each neighbor (when we received any message)
        self.last_seen: Dict[str, float] = {}

        # what each neighbor last advertised (set of destinations), used to detect withdrawn routes
        self.last_advertised_by_neighbor: Dict[str, Set[str]] = {}

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", PORT))
        self.sock.settimeout(1.0)

        self.lock = threading.Lock()
        self.running = True

        self._load_neighbors()

        # initialize routing table with direct neighbors
        for n in self.neighbors:
            if n != self.my_ip:
                self.routing_table[n] = (1, n)

    def _load_neighbors(self):
        try:
            with open(self.neighbors_file, "r", encoding="utf-8") as f:
                for line in f:
                    ip = line.strip()
                    if ip:
                        self.neighbors.add(ip)
        except FileNotFoundError:
            print(f"Arquivo '{self.neighbors_file}' não encontrado. Nenhum vizinho carregado.")

    def start(self):
        # send initial announce
        self._send_announce()

        # start receiver thread
        t_recv = threading.Thread(target=self._recv_loop, daemon=True)
        t_recv.start()

        # start periodic advert thread
        t_adv = threading.Thread(target=self._periodic_tasks, daemon=True)
        t_adv.start()

        # interactive CLI in main thread
        try:
            self._cli_loop()
        except KeyboardInterrupt:
            print("\nEncerrando...")
        finally:
            self.running = False
            time.sleep(0.5)

    def _sendto(self, msg: str, ip: str):
        try:
            self.sock.sendto(msg.encode("utf-8"), (ip, PORT))
        except Exception as e:
            print(f"Erro enviando para {ip}: {e}")

    def _send_announce(self):
        msg = f"@{self.my_ip}"
        for n in list(self.neighbors):
            if n == self.my_ip:
                continue
            self._sendto(msg, n)

    def _build_advertisement_for_neighbor(self, neighbor: str) -> str:
        # Apply Split Horizon: do not include routes learned via `neighbor`
        parts: List[str] = []
        with self.lock:
            for dest, (metric, next_hop) in self.routing_table.items():
                if dest == self.my_ip:
                    continue
                if next_hop == neighbor:
                    continue  # split horizon
                parts.append(f"*{dest};{metric}")
        return "".join(parts)

    def _periodic_tasks(self):
        last_advert_time = 0.0
        while self.running:
            now = time.time()
            if now - last_advert_time >= ADVERT_INTERVAL:
                # send advertisement to each neighbor (applying split horizon per neighbor)
                for n in list(self.neighbors):
                    if n == self.my_ip:
                        continue
                    advert = self._build_advertisement_for_neighbor(n)
                    if advert:
                        self._sendto(advert, n)
                last_advert_time = now

            # neighbor timeout detection
            to_remove: List[str] = []
            with self.lock:
                for n, last in list(self.last_seen.items()):
                    if now - last > NEIGHBOR_TIMEOUT:
                        print(f"Vizinho {n} considerado inativo (sem updates por {NEIGHBOR_TIMEOUT}s)")
                        # remove routes whose next_hop == n
                        removed = [d for d, (_, nh) in list(self.routing_table.items()) if nh == n]
                        for d in removed:
                            del self.routing_table[d]
                        # clear neighbor state but keep in configured neighbors list
                        del self.last_seen[n]
                        if n in self.last_advertised_by_neighbor:
                            del self.last_advertised_by_neighbor[n]
                        # inform others immediately
                        self._broadcast_table()
            time.sleep(1)

    def _recv_loop(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Erro no socket: {e}")
                continue

            msg = data.decode("utf-8", errors="replace").strip()
            src_ip = addr[0]
            # update last seen
            with self.lock:
                self.last_seen[src_ip] = time.time()

            if not msg:
                continue

            if msg.startswith("@"):
                announced_ip = msg[1:].strip()
                if announced_ip and announced_ip != self.my_ip:
                    with self.lock:
                        # neighbor informs of its own address: add route with metric 1
                        self.routing_table[announced_ip] = (1, src_ip)
                    print(f"Recebido anuncio @ de {src_ip}: adicionar rota para {announced_ip}")
                    self._broadcast_table()

            elif msg.startswith("*"):
                # advertisement: sequence of *IP;metric tokens
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
                            # new route learned
                            self.routing_table[ip_part] = (new_metric, src_ip)
                            changed = True
                        else:
                            cur_metric, cur_nh = self.routing_table[ip_part]
                            # if better metric found, update
                            if new_metric < cur_metric:
                                self.routing_table[ip_part] = (new_metric, src_ip)
                                changed = True
                            # if the route was via the same neighbor, update metric if changed
                            elif cur_nh == src_ip and new_metric != cur_metric:
                                self.routing_table[ip_part] = (new_metric, src_ip)
                                changed = True

                    # detect withdrawn routes from this neighbor: those previously advertised but now missing
                    prev = self.last_advertised_by_neighbor.get(src_ip, set())
                    withdrawn = prev - advertised_dests
                    for d in withdrawn:
                        # only remove if we learned it via this neighbor
                        if d in self.routing_table and self.routing_table[d][1] == src_ip:
                            del self.routing_table[d]
                            changed = True

                    self.last_advertised_by_neighbor[src_ip] = advertised_dests

                if changed:
                    print("Tabela atualizada a partir de atualização recebida:")
                    self.print_table()
                    # if table changed, immediately announce
                    self._broadcast_table()

            elif msg.startswith("!"):
                # text message: !orig;dest;message
                try:
                    _, rest = msg[0], msg[1:]
                    origin, dest, text = rest.split(";", 2)
                except Exception:
                    print(f"Mensagem de texto inválida recebida de {src_ip}: {msg}")
                    continue

                if dest == self.my_ip:
                    print(f"Mensagem recebida para mim: de {origin} para {dest}: {text}")
                    print("Chegou ao destino.")
                else:
                    # forward according to routing table
                    with self.lock:
                        entry = self.routing_table.get(dest)
                    if not entry:
                        print(f"Não há rota para {dest}. Mensagem descartada.")
                    else:
                        next_hop = entry[1]
                        print(f"Repassando mensagem {origin}->{dest} para {next_hop}")
                        self._sendto(msg, next_hop)

            else:
                # unknown message type
                print(f"Mensagem desconhecida de {src_ip}: {msg}")

    def _broadcast_table(self):
        # send full table to all neighbors (split horizon applied in per-neighbor builder)
        for n in list(self.neighbors):
            if n == self.my_ip:
                continue
            advert = self._build_advertisement_for_neighbor(n)
            if advert:
                self._sendto(advert, n)

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
        self._sendto(msg, next_hop)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", required=True, help="IP deste roteador (ex: 192.168.1.2)")
    parser.add_argument("--neighbors", default="roteadores.txt", help="Arquivo com vizinhos (um IP por linha)")
    args = parser.parse_args()

    r = Router(args.ip, neighbors_file=args.neighbors)
    r.start()


if __name__ == "__main__":
    main()
