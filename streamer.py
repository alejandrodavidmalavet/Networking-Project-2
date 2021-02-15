# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time

from threading import Timer

import hashlib
import zlib


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

        self.nagles_binary_string = b''

    def send(self, data_bytes: bytes) -> None:
        if not self.nagles_binary_string:
            Timer(0.01, self.nagles_algorithm, [len(data_bytes)]).start() 
        self.nagles_binary_string+=data_bytes

    def recv(self) -> bytes:

        while self.recv_seq not in self.recv_buffer:
            time.sleep(0.01)

        self.recv_seq += 1
        return self.recv_buffer.pop(self.recv_seq - 1)

    def close(self) -> None:
        time.sleep(5)
        while self.send_seq not in self.acks:
            self.send_fin()
            time.sleep(0.1)

        time.sleep(5)

        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed:
            try:
                packet = self.socket.recvfrom()[0]

                if not packet: return
                    
                packet, hash, seq, ack, fin, data = self.deconstruct_packet(packet)

                if self.corrupted(packet,hash):
                    print(packet,hash,zlib.adler32(packet))
                    return

                if ack:
                    self.acks.add(seq)

                elif fin:
                    self.send_ack(seq)

                else :
                    self.recv_buffer[seq] = data
                    self.send_ack(seq)
                
            except Exception as e:
                print("listener died!")
                print(e)

    def partition_data(self,data):
        return (data[0 + i : 1462 + i] for i in range(0, len(data), 1462))

    def build_packet(self, seq, ack, fin, data):
        f = 'I??' + str(len(data)) + 's'
        return self.packet_hasher(struct.pack(f, seq, ack, fin, data))

    def deconstruct_packet(self, packet):
        f = str(len(packet)-4) + 'sI'
        packet, hash = struct.unpack(f, packet)
        f = 'I??' + str(len(packet)-6) + 's'
        seq, ack, fin, data = struct.unpack(f,packet)
        return packet, hash, seq, ack, fin, data
    
    def packet_hasher(self,packet):
        f = str(len(packet)) + 'sI'
        return struct.pack(f,packet,zlib.adler32(packet))

    def corrupted(self,packet,hash):
        return zlib.adler32(packet) != hash

    def nagles_algorithm(self, currentData):
        if currentData<len(self.nagles_binary_string): # if there is more data to send
            Timer(0.01, self.nagles_algorithm, [len(self.nagles_binary_string)]).start() 
        else:
            for data in self.partition_data(self.nagles_binary_string):
                self.send_buffer[self.send_seq] = self.build_packet(self.send_seq,False,False,data)
                self.resend(self.send_seq)
                self.send_seq += 1
            self.nagles_binary_string = b''

    def repeat(self,seq):
        if seq not in self.acks:
            self.resend(seq)

    def resend(self, seq):
        self.socket.sendto(self.send_buffer[seq], (self.dst_ip,self.dst_port))
        Timer(1,self.repeat,[seq]).start()

    def send_ack(self,seq):
        self.socket.sendto(self.build_packet(seq,True,False, bytes(b'\x00\x00')), (self.dst_ip, self.dst_port))

    def send_fin(self):
        self.socket.sendto(self.build_packet(self.send_seq,False,True, bytes(b'\x00\x00')), (self.dst_ip, self.dst_port))

        



