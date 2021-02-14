# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time

import hashlib


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.send_seq = 0x00000000
        self.send_buffer = dict()

        self.recv_seq = 0x00000000
        self.recv_buffer = dict()

        self.closed = False

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

        self.acks = set()
        self.fin = False

        


    def send(self, data_bytes: bytes) -> None:
        for data in self.partition_data(data_bytes):
            self.send_buffer[self.send_seq] = self.build_packet(self.send_seq,False,False,data)
            self.socket.sendto(self.send_buffer[self.send_seq], (self.dst_ip, self.dst_port))
            time.sleep(0.25)
            if self.send_seq not in self.acks:
                self.send(data_bytes)
            else :
                self.send_seq += 1

    def send_ack(self,seq):
        #print("Sending Acknowledgement for Packet #" + str(seq))
        self.socket.sendto(self.build_packet(seq,True,False,bytes()), (self.dst_ip, self.dst_port))

    def send_fin(self):
        self.socket.sendto(self.build_packet(self.send_seq,False,True,bytes()), (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        while self.recv_seq not in self.recv_buffer:
            continue
        self.recv_seq += 1
        return self.recv_buffer.pop(self.recv_seq - 1)

    def close(self) -> None:
    

        while self.send_seq not in self.acks:
            self.send_fin()
            time.sleep(0.1)
        #print("FIN recieved")
        time.sleep(2)
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed: # a later hint will explain self.closed
            try:
                packet = self.socket.recvfrom()[0]
                if packet:
                    seq, ack, fin, data = self.deconstruct_packet(packet)

                    if ack:
                        self.acks.add(seq)
                    else:
                        self.recv_buffer[seq] = data
                        self.send_ack(seq)
                
            except Exception as e:
                print("listener died!")
                print(e)

    def build_packet(self, seq, ack, fin, data):
        f = 'I??' + str(len(data)) + 's'
        return struct.pack(f, seq, ack, fin, data)

    def deconstruct_packet(self, packet):
        f = 'I??' + str(len(packet)-6) + 's'
        return struct.unpack(f, packet)

    def partition_data(self,data):
        return (data[0 + i : 1466 + i] for i in range(0, len(data), 1466))

