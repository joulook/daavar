package impl;

import java.io.*;
import java.net.*;
//@ import verifast.*;
//@ import UDPSocketHelper.*;

/*@
// State definition

inductive tr3s_state_local = tr3s_state_local(boolean, fixpoint(int, boolean), fixpoint(int, boolean));

fixpoint boolean winner2(tr3s_state_local s) {
    switch (s) {
        case tr3s_state_local(winner, ibuf, obuf): return winner;
    }
}

fixpoint fixpoint(int, boolean) ibuf2(tr3s_state_local s) {
    switch (s) {
        case tr3s_state_local(winner, ibuf, obuf): return ibuf;
    }
}

fixpoint fixpoint(int, boolean) obuf2(tr3s_state_local s) {
    switch (s) {
        case tr3s_state_local(winner, ibuf, obuf): return obuf;
    }
}

// Declaration of internal I/O operations

predicate B_set_up3s(place tPre, val out, val in, place tPost);
predicate B_accept3s(place tPre, val out, val in, place tPost);
predicate B_elect3s(place tPre, val out, val in, place tPost);
predicate B_skip3s(place tPre, val out, val in, place tPost);


lemma void set_up()
    requires token(?t) &*& B_set_up3s(t, Unit, ?w, ?tPost);
    ensures token(tPost);
{ assume(false); }

lemma void skip()
    requires token(?t) &*& B_skip3s(t, Unit, ?w, ?tPost);
    ensures token(tPost);
{ assume(false); }

lemma void accept(int msg)
    requires token(?t) &*& B_accept3s(t, Msg(msg), ?w, ?tPost);
    ensures token(tPost);
{ assume(false); }


lemma void elect()
    requires token(?t) &*& B_elect3s(t, Unit, ?w, ?tPost);
    ensures token(tPost);
{ assume(false); }

// I/O contract definition

predicate_ctor p_B_accept3s(place t, int id, address addr, tr3s_state_local s)(val v) =
    switch (v) {
        case Unit: return true;
        case Msg(msg): return (apply(ibuf2(s), msg) && id < msg) ?  B_accept3s(t, v, Unit, ?t2) &*& P(t2, id, addr, tr3s_state_local(winner2(s), ibuf2(s), fupdate(obuf2(s), msg, true))) : true;
        case AddrMsg(msg, addr2): return true;
    };

predicate_ctor p_UDP_send_int(place t, int id, address addr, tr3s_state_local s)(val v) =
    switch (v) {
        case AddrMsg(msg, addr2) : return apply(obuf2(s), msg) && addr2 == addr ? (UDP_send_int(t, v, ?res, ?t6) &*& P(t6, id, addr, s)) : true;
        case Unit: return true;
        case Msg(msg): return true;
    };

predicate_ctor p_B_set_up3s(place t, int id, address addr, tr3s_state_local s)(val v) =
    v == Unit ? B_set_up3s(t, v, ?w, ?t2) &*& P(t2, id, addr, tr3s_state_local(winner2(s), ibuf2(s), fupdate(obuf2(s), id, true)))  : true;

predicate_ctor p_B_elect3s(place t, int id, address addr, tr3s_state_local s)(val v) =
    v == Unit ? (apply(ibuf2(s), id) ? (B_elect3s(t, Unit, Unit, ?t4) &*& P(t4, id, addr, tr3s_state_local(true, ibuf2(s), obuf2(s)))) : true)  : true;

predicate_ctor p_B_skip3s(place t, int id, address addr, tr3s_state_local s)(val v) =
    v == Unit ? B_skip3s(t, v, ?w, ?t2) &*& P(t2, id, addr, s)  : true;

predicate_ctor p_UDP_receive_int(place t, int id, address addr, tr3s_state_local s)(val v) =
    v == Unit ? UDP_receive_int(t, v, ?res, ?t5) &*&
    switch (res) { case Unit: return true; case Msg(msg): return P(t5, id, addr, tr3s_state_local(winner2(s), fupdate(ibuf2(s), msg, true), obuf2(s))); case AddrMsg(m, a): return true; } : true;


predicate P(place t, int id, address addr, tr3s_state_local s) =
    bigstar(p_B_set_up3s(t, id, addr, s), nil)
    &*&
    bigstar(p_UDP_receive_int(t, id, addr, s), nil)
    &*&
    bigstar(p_B_accept3s(t, id, addr, s), nil)
    &*&
    bigstar(p_UDP_send_int(t, id, addr, s), nil)
    &*&
    bigstar(p_B_elect3s(t, id, addr, s), nil)
    &*&
    bigstar(p_B_skip3s(t, id, addr, s), nil)
    ;


// Initial state definition
fixpoint tr3s_state_local tr3s_init_state() {
  return tr3s_state_local(false, empty_set, empty_set);
}
@*/


public class Leader {

    public static void main(String[] args)
        /*@ requires 
              System_out(?o) &*& o != null &*&
              array_slice(args, 0, args.length, ?elems) &*& 
              (args.length == 4) ? (
                nth(0, elems) != null &*& nth(1, elems) != null &*& nth(2, elems) != null &*& nth(3, elems) != null &*&
                token(?tInit) &*& P(tInit, ?myId, ?addr, tr3s_init_state()) &*& addrHost(addr) == nth(0,elems)
              ) : true;
        @*/
        //@ ensures true;
    {
        if (args.length == 4) {
            String OUT_HOST = args[0];
            int MY_ID = Integer.parseInt(args[1]);
            int IN_PORT = Integer.parseInt(args[2]);
            int OUT_PORT = Integer.parseInt(args[3]);
            //@ assert token(?tInit) &*& P(tInit, ?myId, ?addr, tr3s_init_state());
            //@ assume(myId == MY_ID);
            //@ assume(addrPort(addr) == OUT_PORT);

	    //@ switch(addr) { case Address(h,p): assert h == OUT_HOST &*& p == OUT_PORT; }
            mainLoop(OUT_HOST, MY_ID, IN_PORT, OUT_PORT);
        } else {
            System.out.println("Wrong number of arguments, please provide host address, id, in-port, and out-port as arguments.");
        } 
    }

    public static void mainLoop(String outHost, int myId, int inPort, int outPort)
        //@ requires token(?tInit) &*& P(tInit, myId, Address(outHost, outPort), tr3s_init_state()) &*& System_out(?o) &*& o != null;
        //@ ensures true;
    {
        try {
            //@ tr3s_state_local s = tr3s_init_state();
            //@ address outAddr = Address(outHost, outPort);
            DatagramSocket recSocket = new DatagramSocket(inPort);
            recSocket.setSoTimeout(500);
            DatagramSocket sendSocket = new DatagramSocket();
            sendSocket.connect(InetAddress.getByName(outHost), outPort);

            int toSend = myId;
            //@ open P(tInit, myId, outAddr, s);
            //@ bigstar_extract(p_B_set_up3s(tInit, myId, outAddr, s), Unit);
            //@ open p_B_set_up3s(tInit, myId, outAddr, s)(Unit);
            //@ set_up();

            //@ s = tr3s_state_local(winner2(s), ibuf2(s), fupdate(obuf2(s), myId, true));
            //@ open P(?t1, myId, outAddr,  s);

            //@ bigstar_extract(p_UDP_send_int(t1, myId, outAddr, s), AddrMsg(toSend, outAddr));
            //@ open p_UDP_send_int(t1, myId, outAddr, s)(AddrMsg(toSend, outAddr));

            UDPSocketHelper.trySendInt(sendSocket, toSend);

            while (true)
                /*@
                invariant token(?t) &*& P(t, myId, outAddr, s) &*& sock(recSocket, inPort)
                        &*& sock(sendSocket, 0) &*& connected(sendSocket, ?addr, outPort) &*& address(addr, outHost)
                        &*& toSend >= myId &*& true == apply(obuf2(s), toSend)
                        &*& System_out(o) &*& o != null;
                @*/
            {
                //@ open P(t, myId, outAddr, s);
                //@ place tEnd = t;
                //@ bigstar_extract(p_UDP_receive_int(t, myId, outAddr, s), Unit);
                //@ open p_UDP_receive_int(t, myId, outAddr, s)(Unit);
                Integer msg = UDPSocketHelper.tryReceiveInt(recSocket);
                if (msg != null) {
                    int msgUnpacked = msg.intValue();
                    LoggerHelper.logReceive(myId, msgUnpacked);
                    //@ s = tr3s_state_local(winner2(s), fupdate(ibuf2(s), msgUnpacked, true), obuf2(s));
                    //@ open P(?t2, myId, outAddr, s);
                    //@ tEnd = t2;

                    if (msgUnpacked == myId) {
                        //@ bigstar_extract(p_B_elect3s(t2, myId, outAddr, s), Unit);
                        //@ open p_B_elect3s(t2, myId, outAddr, s)(Unit);
                        //@ elect();
                        //@ s = tr3s_state_local(true, ibuf2(s), obuf2(s));

                        LoggerHelper.logFoundLeader(myId);
                        break;
                    }else if (msgUnpacked > toSend) {
                        //@ bigstar_extract(p_B_accept3s(t2, myId, outAddr, s), Msg(msgUnpacked));
                        //@ open p_B_accept3s(t2, myId, outAddr, s)(Msg(msgUnpacked));
                        //@ accept(msgUnpacked);
                        toSend = msgUnpacked;
                        //@ s = tr3s_state_local(winner2(s), ibuf2(s), fupdate(obuf2(s), toSend, true));
                        //@ open P(?t3, myId, outAddr, s);
                        //@ tEnd = t3;
                    }
                }
                //@ bigstar_extract(p_UDP_send_int(tEnd, myId, outAddr, s), AddrMsg(toSend, outAddr));
                //@ open p_UDP_send_int(tEnd, myId, outAddr, s)(AddrMsg(toSend, outAddr));
                
                UDPSocketHelper.trySendInt(sendSocket, toSend);
                LoggerHelper.logSend(myId, toSend);
            }
        }catch(IOException e) {
            e.printStackTrace();
        }
    }


}
