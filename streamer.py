# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY

import struct
import concurrent.futures
import time


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

        


    def send(self, data_bytes: bytes) -> None:
        for data in self.partition_data(data_bytes):
            self.send_buffer[self.send_seq] = self.build_packet(self.send_seq,0,data)
            self.socket.sendto(self.send_buffer[self.send_seq], (self.dst_ip, self.dst_port))
            #if self.send_seq not in self.acks: self.acks[self.send_seq] = False
            time.sleep(0.25)
            if self.send_seq in self.acks:
                self.send_seq += 1
            else :
                print("Resending Packet #" + str(self.send_seq))
                self.send(data_bytes)

    def send_ack(self,seq):
        self.socket.sendto(self.build_packet(seq,True,bytes()), (self.dst_ip, self.dst_port))
        self.acks.add(seq)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        while self.recv_seq not in self.recv_buffer:
            continue
        self.send_ack(self.recv_seq)
        self.recv_seq += 1
        return self.recv_buffer.pop(self.recv_seq - 1)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown()

    def listener(self):
        while not self.closed: # a later hint will explain self.closed
            try:

                seq, ack, data = self.deconstruct_packet(self.socket.recvfrom()[0])

                if not ack:
                    self.recv_buffer[seq] = data
                
            except Exception as e:
                print("listener died!")
                print(e)
                self.close()

    def build_packet(self, seq, ack, data):
        f = 'I?' + str(len(data)) + 's'
        return struct.pack(f, seq, ack, data)

    def deconstruct_packet(self, packet):
        f = 'I?' + str(len(packet)-5) + 's'
        return struct.unpack(f, packet)

    def partition_data(self,data):
        return (data[0 + i : 1467 + i] for i in range(0, len(data), 1467))

