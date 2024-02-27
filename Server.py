import socket
from queue import Queue
from datetime import datetime

# Cria um socket UDP
UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

class Coordenador:
    def __init__(self):
        # Configurações do coordenador
        self.__LOCAL_IP = "IP"
        self.__LOCAL_PORT = 20001
        self.BUFFER_SIZE = 1024
        self.__FILA = Queue(10)
        self.shard_a = Shard_a()
        self.shard_b = Shard_b()
    
        # Resposta ao cliente
    def responde_cliente(self, client_address, dados):
        bytesToSend = str.encode(dados)
        UDPServerSocket.sendto(bytesToSend, client_address)

    def start(self):
        # Abre a porta do IP
        UDPServerSocket.bind((self.__LOCAL_IP, self.__LOCAL_PORT))
        print("\nCoordenador online e recebendo...\n")

        while True:
            bytes_recebidos = UDPServerSocket.recvfrom(self.BUFFER_SIZE)
            req_cliente = bytes_recebidos[0]
            client_address = bytes_recebidos[1]

            req_msg = req_cliente.decode()
            self.__FILA.put(req_msg)

            tipo_op = self.__FILA.get().split(',')[2]
            self.__FILA.put(req_msg)

            resposta = False

            if tipo_op == "C":
                resposta_shard_a = self.shard_a.credito(self.__FILA)
                if resposta_shard_a.split(',')[0] == "OK":    
                    resposta = True #Transação concluída com sucesso
                    saldo = resposta_shard_a.split(',')[1]
                    print("\nOperação em Shard_A concluída com sucesso!\nSaldo Atual: {}\n".format(saldo))
                    self.responde_cliente(client_address, resposta_shard_a)
            elif tipo_op == "D":
                resposta_shard_b = self.shard_b.debito(self.__FILA)
                if resposta_shard_b.split(',')[0] == "OK":
                    resposta = True #Transação concluída com sucesso
                    saldo = resposta_shard_b.split(',')[1]
                    print("\nOperação em Shard_B concluída com sucesso!\nSaldo Atual: {}\n".format(saldo))
                    self.responde_cliente(client_address, resposta_shard_b)

            # Resposta ao cliente caso ocorra alguma falha
            if resposta:
                msgFromServer = "Falha na transação"
                bytesToSend = str.encode(msgFromServer)
                UDPServerSocket.sendto(bytesToSend, client_address)
    
class Shard_a():

    def constroi_msg(self, *args):
        return ",".join([ str(arg) for arg in args])

    def credito(self, fila):
        if not fila.empty():
            msg = fila.get()
            saldo_fila = float(msg.split(',')[1])  # Convertendo para float
            valor_operacao = float(msg.split(',')[3])  # Convertendo para float
            data_operacao = msg.split(',')[0]
            
            saldo_atual = saldo_fila + valor_operacao
            data_operacao = datetime.now().strftime('%d/%m/%Y %H:%M')
            
            enviar_dados = self.constroi_msg("OK", saldo_atual, data_operacao)

            return enviar_dados
        return "Erro"

class Shard_b:

    def constroi_msg(self, *args):
        return ",".join([ str(arg) for arg in args])

    def debito(self, fila):
        if not fila.empty():
            msg = fila.get()
            saldo_cliente = float(msg.split(',')[1])  # Convertendo para float
            valor_operacao = float(msg.split(',')[3])  # Convertendo para float
            data_operacao = msg.split(',')[0]

            if saldo_cliente >= valor_operacao:
                saldo_atual = saldo_cliente - valor_operacao
                data_operacao = datetime.now().strftime('%d/%m/%Y %H:%M')

                enviar_dados = self.constroi_msg("OK", saldo_atual, data_operacao)

                return enviar_dados
            else:
                return "Erro"

coord = Coordenador()
coord.start()