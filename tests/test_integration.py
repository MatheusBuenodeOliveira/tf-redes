import time


from router import Router


def test_two_routers_exchange_routes():
    # Criar dois roteadores em memória (sem usar sockets reais nos testes):
    r1 = Router("1.1.1.1", bind_socket=False)
    r2 = Router("2.2.2.2", bind_socket=False)

    # configurar vizinhança manualmente (cada um conhece o outro)
    r1.neighbors = {"2.2.2.2"}
    r2.neighbors = {"1.1.1.1"}

    # mockar _sendto para entregar diretamente à outra instância
    def send_from_r1(msg, ip):
        # ip será "2.2.2.2" — chamar process_message de r2
        r2.process_message(msg, "1.1.1.1")

    def send_from_r2(msg, ip):
        r1.process_message(msg, "2.2.2.2")

    r1._sendto = send_from_r1
    r2._sendto = send_from_r2

    # r1 tem uma rota direta para 3.3.3.3 (simulando outro vizinho)
    r1.routing_table["3.3.3.3"] = (1, "3.3.3.3")

    # Anúncio inicial de r1 deve ser recebido por r2 (adicionar rota para 1.1.1.1)
    r1._send_announce()
    assert "1.1.1.1" in r2.routing_table
    assert r2.routing_table["1.1.1.1"][0] == 1

    # Agora r1 anuncia sua tabela; r2 deve aprender rota para 3.3.3.3 com métrica 2
    r1._broadcast_table()
    # pequena espera para propagação (simulação síncrona aqui, mas mantemos janela)
    time.sleep(0.01)
    assert "3.3.3.3" in r2.routing_table
    metric, nh = r2.routing_table["3.3.3.3"]
    assert metric == 2
    assert nh == "1.1.1.1"
