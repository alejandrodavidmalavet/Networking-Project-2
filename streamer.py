# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time

from threading import Timer

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

        self.send_seq = 0x00000001
        self.send_buffer = dict()

        self.recv_seq = 0x00000001
        self.recv_buffer = dict()

        self.closed = False

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)

        self.acks = set()
        self.fin = False

    def resend(self, seq):
        self.socket.sendto(self.send_buffer[seq], (self.dst_ip,self.dst_port))
        Timer(2.5,self.repeat,[seq]).start()

    def send(self, data_bytes: bytes) -> None:
        
        for data in self.partition_data(data_bytes):
            print(data)
            self.send_buffer[self.send_seq] = self.build_packet(self.send_seq,False,False,data)
            self.resend(self.send_seq)
            self.send_seq += 1

    def repeat(self,seq):
        if seq not in self.acks:
            self.resend(seq)

    def send_ack(self,seq):
        self.socket.sendto(self.build_packet(seq,True,False, bytes(b'\x00\x00')), (self.dst_ip, self.dst_port))

    def send_fin(self):
        self.socket.sendto(self.build_packet(self.send_seq,False,True, bytes(b'\x00\x00')), (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        while self.recv_seq not in self.recv_buffer:
            time.sleep(0.01)
        self.recv_seq += 1
        return self.recv_buffer.pop(self.recv_seq - 1)

    def close(self) -> None:
    

        while self.send_seq not in self.acks:
            self.send_fin()
            time.sleep(0.1)
        print("FIN HANDSHAKE")
        time.sleep(10)
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed:
            try:
                packet = self.socket.recvfrom()[0]

                if not packet: return
                    
                packet, hash, seq, ack, fin, data = self.deconstruct_packet(packet)

                if self.corrupted(packet,hash):
                    print(packet)
                    print(ack,fin)
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
        print(packet)
        return struct.pack(f,packet,zlib.adler32(packet))

    def corrupted(self,packet,hash):
        return zlib.adler32(packet) != hash

