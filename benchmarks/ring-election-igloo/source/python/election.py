import socket
import sys

from typing import cast, NamedTuple, Optional, Tuple
# from nagini_contracts.contracts import *
# from nagini_contracts.io_contracts import *
# from nagini_contracts.obligations import MustTerminate
from leader.int_socket import *
# from leader.spec import *


def mainLoop(out_addr: Tuple[str, int], my_id: int, in_port: int) -> None:  # def mainLoop(t: Place, out_addr: Tuple[str, int], my_id: int, in_port: int) -> Place:
    # Requires(my_id >= 0 and my_id < MAX_INT and type(my_id) == int)
    # Requires(token(t) and P(t, my_id, out_addr, tr3s_init_state))
    # Ensures(token(Result(), 5))
    # s = tr3s_init_state
    # Open(P(t, my_id, out_addr, tr3s_init_state))
    # Open(P_set_up_init(t, my_id, out_addr, tr3s_init_state))
    rec_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rec_socket.settimeout(0.5)
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    rec_socket.bind(("127.0.0.1", in_port))
    send_socket.connect(out_addr)

    to_send = my_id

    # t = set_up_init(t)

    # s = tr3s_state_local(s.winner2, s.ibuf2, s.obuf2 + PSet(my_id))
    # Open(P(t, my_id, out_addr, s))
    # Open(P_send(t, my_id, out_addr, s))
    # Assert(cast(AddrMsg, AddrMsg(to_send, out_addr)).msg in s.obuf2)
    succ = try_send_int(send_socket, to_send)  # t, succ = try_send_int(t, send_socket, to_send)

    while True:
        # Invariant(token(t) and P(t, my_id, out_addr, s))
        # Invariant(to_send >= 0 and to_send < MAX_INT)
        # Invariant(Acc(rec_socket.timeout(), 1 / 2) and Acc(rec_socket.sock(), 1 / 2))
        # Invariant(Acc(rec_socket.family, 1/2) and Acc(rec_socket.type, 1/2))
        # Invariant(Acc(send_socket.timeout(), 1 / 2) and Acc(send_socket.peer(), 1 / 2))
        # Invariant(Acc(send_socket.family, 1 / 2) and Acc(send_socket.type, 1 / 2))
        # Invariant(rec_socket.family is socket.AF_INET and rec_socket.type is socket.SOCK_DGRAM)
        # Invariant(send_socket.family is socket.AF_INET and send_socket.type is socket.SOCK_DGRAM)
        # Invariant(not rec_socket.getblocking() and rec_socket.getsockname()[1] is in_port)
        # Invariant(send_socket.getpeername() is out_addr)
        # Invariant(type(to_send) == int and to_send in s.obuf2 and to_send >= my_id)

        # Open(P(t, my_id, out_addr, s))
        # Open(P_receive(t, my_id, out_addr, s))
        msg = try_receive_int(rec_socket)  # t, msg = try_receive_int(t, rec_socket)
        if msg is not None:
            print(str(my_id) + " received identifier: " + str(msg))
            # s = tr3s_state_local(s.winner2, s.ibuf2 + PSet(msg), s.obuf2)
            # Assert(msg in s.ibuf2)
            # Open(P(t, my_id, out_addr, s))
            if msg == my_id:
                # Open(P_elect(t, my_id, out_addr, s))
                # t = elect(t)
                print("I am the leader: " + str(my_id))
                break
            elif msg > to_send:
                # Assert(Msg(msg).msg in s.ibuf2)
                # Open(P_accept(t, my_id, out_addr, s))
                # t = accept(t, msg)
                to_send = msg
                # s = tr3s_state_local(s.winner2, s.ibuf2, s.obuf2 + PSet(msg))
                # Assert(to_send in s.obuf2)
                # Open(P(t, my_id, out_addr, s))
        # Open(P_send(t, my_id, out_addr, s))
        # Assert(cast(AddrMsg, AddrMsg(to_send, out_addr)).msg in s.obuf2)
        print(str(my_id) + " sending out identifier: " + str(to_send))
        success = try_send_int(send_socket, to_send)  # t, success = try_send_int(t, send_socket, to_send)
    return  # return t


def try_receive_int(rec_socket: socket.socket) -> Optional[int]:  # def try_receive_int(t: Place, rec_socket: socket.socket) -> Tuple[Place, Optional[int]]:
    # IOExists2(Msg, Place)(lambda res, t_post: (
    #     Requires(token(t, 3)),
    #     Requires(Acc(rec_socket.timeout(), 1 / 3)),
    #     Requires(Acc(rec_socket.type, 1 / 4) and Acc(rec_socket.family, 1 / 4)),
    #     Requires(Acc(rec_socket.sock(), 1 / 4)),
    #     Requires(Implies(not rec_socket.getblocking(), MustTerminate(3))),
    #     Requires(rec_socket.family is socket.AF_INET),
    #     Requires(rec_socket.type is socket.SOCK_DGRAM),
    #     Requires(not rec_socket.getblocking()),
    #     Requires(UDP_receive_int(t, Unit(), res, t_post)),
    #     Ensures(Acc(rec_socket.timeout(), 1 / 3)),
    #     Ensures(Acc(rec_socket.type, 1 / 4) and Acc(rec_socket.family, 1 / 4)),
    #     Ensures(Acc(rec_socket.sock(), 1 / 4)),
    #     Ensures((Result()[0] is t_post and Result()[1] is res.msg and token(t_post)) if Result()[1] is not None else (token(t, 2) and Result()[0] is t)),
    #     Ensures(Implies(Result()[1] is not None, res.msg >= 0 and res.msg < MAX_INT and type(res.msg) == int))
    # ))
    try:
        r = receive_int(rec_socket)  # p, r = receive_int(t, rec_socket)
        res = r  # res = (p, r)
        # Assert(Implies(res[1] is not None, res[1] >= 0 and res[1] < MAX_INT and type(res[1]) == int))
        return res
    except socket.timeout as e:
        rese = None  # rese = t, None
        # Assert(Implies(rese[1] is not None, rese[1] >= 0 and rese[1] < MAX_INT and type(rese[1]) == int))
        return rese



def try_send_int(send_socket: socket.socket, to_send: int) -> bool:  # def try_send_int(t: Place, send_socket: socket.socket, to_send: int) -> Tuple[Place, bool]:
    # IOExists2(Place, Unit)(lambda t_post, w: (
    #     Requires(to_send >= 0 and to_send < MAX_INT),
    #     Requires(MustTerminate(3)),
    #     Requires(token(t, 3)),
    #     Requires(Acc(send_socket.type, 1 / 4) and Acc(send_socket.family, 1 / 4)),
    #     Requires(Acc(send_socket.peer(), 1 / 4)),
    #     Requires(send_socket.family is socket.AF_INET),
    #     Requires(send_socket.type is socket.SOCK_DGRAM),
    #     Requires(UDP_send_int(t, AddrMsg(to_send, send_socket.getpeername()), w, t_post)),
    #     Ensures(Result()[0] is t_post and token(t_post)),
    #     Ensures(Acc(send_socket.type, 1 / 4) and Acc(send_socket.family, 1 / 4)),
    #     Ensures(Acc(send_socket.peer(), 1 / 4)),
    # ))
    try:
        send_int(send_socket, to_send)  # t_res = send_int(t, send_socket, to_send)
        return True  # return t_res, True
    except CustomConnectionRefusedException as e:
        return False  # return e.newplace, False




