import socket
from typing import Tuple
# from nagini_contracts.contracts import *
# from nagini_contracts.io_contracts import *
# from nagini_contracts.obligations import MustTerminate

# from leader.types import *


### Declaration of I/O operations ###

# @IOOperation
# def UDP_send_int(
#         t_pre: Place,
#         v: AddrMsg,
#         w: Unit = Result(),
#         t_post: Place = Result()) -> bool:
#     Terminates(True)

# @IOOperation
# def UDP_receive_int(
#         t_pre: Place,
#         v: Unit,
#         w: Msg = Result(),
#         t_post: Place = Result()) -> bool:
#     Terminates(False)


### Socket operation implementation (trusted)

MAX_INT = 4294967295


class CustomConnectionRefusedException(Exception):
    def __init__(self, err: ConnectionRefusedError) -> None:  # def __init__(self, t: Place, err: ConnectionRefusedError) -> None:
        # self.newplace = t
        self.error = err
        # Ensures(Acc(self.newplace) and self.newplace is t)


# @ContractOnly
def send_int(send_socket: socket.socket, to_send: int) -> None:  # def send_int(t1: Place, send_socket: socket.socket, to_send: int) -> Place:
    # IOExists2(Place, Unit)(lambda t_post, w: (
    #     Requires(MustTerminate(2)),
    #     Requires(to_send >= 0 and to_send < MAX_INT),
    #     Requires(token(t1, 2)),
    #     Requires(Acc(send_socket.type, 1 / 4) and Acc(send_socket.family, 1 / 4)),
    #     Requires(Acc(send_socket.peer(), 1/4)),
    #     Requires(send_socket.family is socket.AF_INET),
    #     Requires(send_socket.type is socket.SOCK_DGRAM),
    #     Requires(UDP_send_int(t1, AddrMsg(to_send, send_socket.getpeername()), w, t_post)),
    #
    #     Ensures(Result() is t_post and token(t_post)),
    #     Ensures(Acc(send_socket.type, 1 / 4) and Acc(send_socket.family, 1 / 4)),
    #     Ensures(Acc(send_socket.peer(), 1 / 4)),
    #
    #     Exsures(CustomConnectionRefusedException, Acc(RaisedException().newplace) and RaisedException().newplace is t_post and token(t_post)),
    #     Exsures(CustomConnectionRefusedException, Acc(send_socket.type, 1 / 4) and Acc(send_socket.family, 1 / 4)),
    #     Exsures(CustomConnectionRefusedException, Acc(send_socket.peer(), 1 / 4)),
    #     Exsures(CustomConnectionRefusedException, UDP_send_int(t1, AddrMsg(to_send, send_socket.getpeername()), w, t_post)),
    # ))
    try:
        send_socket.send(to_send.to_bytes(4, "big"))
    except ConnectionRefusedError as e:
        raise CustomConnectionRefusedException(e)  # raise CustomConnectionRefusedException(t1, e)
    return  # return Place()


# @ContractOnly
def receive_int(rec_socket: socket.socket) -> int:  # def receive_int(t1: Place, rec_socket: socket.socket) -> Tuple[Place, int]:
    # IOExists2(Msg, Place)(lambda w, t_post: (
    #     Requires(Acc(rec_socket.timeout(), 1/4)),
    #     Requires(Acc(rec_socket.type, 1 / 4) and Acc(rec_socket.family, 1 / 4)),
    #     Requires(Acc(rec_socket.sock(), 1 / 4)),
    #     Requires(Implies(not rec_socket.getblocking(), MustTerminate(2))),
    #     Requires(token(t1, 2)),
    #     Requires(rec_socket.family is socket.AF_INET),
    #     Requires(rec_socket.type is socket.SOCK_DGRAM),
    #     Requires(not rec_socket.getblocking()),
    #     Requires(UDP_receive_int(t1, Unit(), w, t_post)),
    #
    #     Ensures(Acc(rec_socket.timeout(), 1 / 4)),
    #     Ensures(Acc(rec_socket.type, 1 / 4) and Acc(rec_socket.family, 1 / 4)),
    #     Ensures(Acc(rec_socket.sock(), 1 / 4)),
    #     Ensures(Result()[0] is t_post and Result()[1] is w.msg and Result()[1] >= 0 and Result()[1] < MAX_INT and type(Result()[1]) == int and token(t_post)),
    #
    #     Exsures(socket.timeout, Acc(rec_socket.timeout(), 1 / 2)),
    #     Exsures(socket.timeout, Acc(rec_socket.type, 1 / 4) and Acc(rec_socket.family, 1 / 4)),
    #     Exsures(socket.timeout, Acc(rec_socket.sock(), 1 / 4)),
    #     Exsures(socket.timeout, token(t1, 2)),
    #     Exsures(socket.timeout, UDP_receive_int(t1, Unit(), w, t_post)),
    # ))
    msg = rec_socket.recv(4)
    received_id = int.from_bytes(msg, "big")
    return received_id  # return Place(), received_id
