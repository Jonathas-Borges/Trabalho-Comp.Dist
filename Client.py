import socket
import time

class Cliente:
    def __init__(self, saldo_cliente, tipo_transacao, valor_operacao):
        self.saldo_cliente = saldo_cliente
        self.tipo_transacao = tipo_transacao
        self.valor_operacao = valor_operacao
        self.data_operacao = None

    def OpClient(self):
        # Configurações do cliente
        SERVER_ADDRESS = "IP"
        SERVER_PORT = 20001
        BUFFER_SIZE = 1024

        # Cria um socket UDP
        UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

        # Define o tipo de transação
        tipo_transacao = "D" if self.tipo_transacao == "D" else "C"
        
        # Constrói a mensagem
        mensagem = "{},{},{},{}".format(self.data_operacao, self.saldo_cliente, tipo_transacao, self.valor_operacao)

        # Envia ao servidor usando o socket UDP criado
        bytes_to_send = str.encode(mensagem)
        server_address_port = (SERVER_ADDRESS, SERVER_PORT)
        print("\nSolicitando transação ao Transaction Coordinator...\n")
        time.sleep(3)
        UDPClientSocket.sendto(bytes_to_send, server_address_port)

        # Recebe a resposta do servidor
        resposta_servidor, __ = UDPClientSocket.recvfrom(BUFFER_SIZE)
        msg = resposta_servidor.decode().split(",")
        # Verifica se a transação obteve sucesso
        if msg[0] == "OK" and len(msg) >= 3:
            saldo_atual = msg[1]
            data_operacao = msg[2]
            print("\nTransação efetuada com sucesso!\nSaldo atual: {}\nData: {}\n".format(saldo_atual, data_operacao))
        else:
            print("Transação não aprovada pelo servidor.")

# Exemplo de uso
for i in range(10):
    if i % 2 == 0:
        cliente_exemplo = Cliente(saldo_cliente=1000, tipo_transacao="C", valor_operacao=500)
        cliente_exemplo.OpClient()
    cliente_exemplo = Cliente(saldo_cliente=1000, tipo_transacao="D", valor_operacao=500)
    cliente_exemplo.OpClient()