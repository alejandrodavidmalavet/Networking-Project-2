# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time

from threading import Timer

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

        self.nagles_binary_string = b''
        
    def resend(self, seq):
        self.socket.sendto(self.send_buffer[seq], (self.dst_ip,self.dst_port))
        return Timer(.5,self.repeat,[seq]).start()

    def send(self, data_bytes: bytes) -> None:
        if not len(self.nagles_binary_string):
            Timer(0.05, self.nagles_algorithm, [len(data_bytes)]).start()
            self.nagles_binary_string = data_bytes
        else:
            self.nagles_binary_string+=data_bytes
    
    def nagles_algorithm(self, currData):
        if currData < len(self.nagles_binary_string):
            return Timer(1, self.nagles_algorithm, [len(self.nagles_binary_string)]).start()
        for data in self.partition_data(self.nagles_binary_string):
            self.send_buffer[self.send_seq] = self.build_packet(data,0,self.send_seq, 0)
            self.resend(self.send_seq)
            self.send_seq += 1
        self.nagles_binary_string = b''

    def repeat(self,seq):
        if seq not in self.acks:
            self.resend(seq)

    def send_ack(self,seq):
        #print("Sending Acknowledgement for Packet #" + str(seq))
        self.socket.sendto(self.build_packet(bytes(),1,seq, 0), (self.dst_ip, self.dst_port))

    def send_fin(self):
        self.socket.sendto(self.build_packet(bytes(),0,self.send_seq, 1), (self.dst_ip, self.dst_port))

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
        time.sleep(15)
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        self.closed = True
        self.socket.stoprecv()

    def listener(self):
        while not self.closed:
            try:
                packet = self.socket.recvfrom()[0]
                if packet:
                    seq, ack, fin, stored_hash, data = self.deconstruct_packet(packet)

                    comparison_packet = struct.pack('iii' + str(len(data)) + 's', seq, ack, fin, data)
                    # print('comparison packet seq: ', seq)
                    # print(comparison_packet)
                    hashed_packet_code = self.hash_helper(comparison_packet)
                    
                    hash_check = self.hash_validation(stored_hash, hashed_packet_code)
                    if hash_check:
                        continue
                    if ack:
                        self.acks.add(seq)
                    elif fin:
                        self.send_ack(seq)
                    else:
                        self.recv_buffer[seq] = data
                        self.send_ack(seq)
                
            except Exception as e:
                print("listener died!")
                print(e)

    def partition_data(self,data):
        return (data[0 + i : 1444 + i] for i in range(0, len(data), 1444))

    # packs data and hashes it
    def build_packet(self, data, ack, seq, fin):
        first_packet = struct.pack('iii' + str(len(data)) + 's', seq, ack, fin, data)
        # print('first packet for seq', seq)
        # print(first_packet)
        hashed_packet_code = self.hash_helper(first_packet)
        return struct.pack('iii16s' + str(len(data))+ 's', seq, ack, fin, hashed_packet_code, data)

    def deconstruct_packet(self, data):
        return struct.unpack('iii16s' + str(len(data)-28) + 's', data)

    def hash_validation(self, stored_hash, hashed_packet_code):
        # hashed_code = self.hash_helper(data)
        if (stored_hash==hashed_packet_code): return False
        return True

    # returns a hash value
    def hash_helper(self, data):
        m = hashlib.md5()
        m.update(data)
        return m.digest()
