From aneris Require Export lang network notation tactics.
From aneris.examples.library Require Export lib list.
From aneris.examples.two_phase_commit Require Export tpc.

Definition req_handler_log : ground_lang.val :=
  λ: "waiting", λ: "msg" "client",
  "waiting" <- value_of_message "msg";;
  #"COMMIT".

Definition fin_handler_log : ground_lang.val :=
  λ: "waiting" "log", λ: "msg" "client",
  if: "msg" = #"COMMIT"
  then "log" <- !"log" ^^ !"waiting"
  else #().

Definition db : ground_lang.val :=
  λ: "addr",
  let: "socket" := NewSocket #Network.PF_INET
                             #Network.SOCK_DGRAM
                             #Network.IPPROTO_UDP in
  let: "waiting" := ref #"" in
  let: "log" := ref #"" in
  let: "req" := req_handler_log "waiting" in
  let: "fin" := fin_handler_log "waiting" "log" in
  SocketBind "socket" "addr";;
  tpc_participant_setup "socket" "req" "fin".

Definition dec_handler_log : ground_lang.val :=
  λ: "msgs",
  let: "result" := list_fold (λ: "acc" "msg",
                              "acc" && "msg" = #"COMMIT") #true "msgs" in
  if: "result" then #"COMMIT" else #"ABORT".

Definition logger : ground_lang.val :=
  λ: "ip" "dbs",
  let: "socket" := NewSocket #Network.PF_INET
                             #Network.SOCK_DGRAM
                             #Network.IPPROTO_UDP in
  let: "socket_tpc" := NewSocket #Network.PF_INET
                                 #Network.SOCK_DGRAM
                                 #Network.IPPROTO_UDP in
  let: "addr" := MakeAddress "ip" #80 in
  let: "tpca" := MakeAddress "ip" #1200 in
  SocketBind "socket" "addr";;
  SocketBind "socket_tpc" "tpca";;
  let: "handler" :=
     rec: "handler" "msg" "sender" :=
       let: "req" := #"REQUEST"  ^^ #"_" ^^ "msg" in
       let: "result" := tpc_coordinate "req" "socket_tpc" "dbs" dec_handler_log in
       SendTo "socket" "result" "sender";;
       listen "socket" "handler" in
  listen "socket" "handler".

Definition client : ground_lang.val :=
  λ: "ip" "logger" "event",
     let: "socket" := NewSocket #Network.PF_INET
                                #Network.SOCK_DGRAM
                                #Network.IPPROTO_UDP in
     let: "addr" := MakeAddress "ip" #80 in
     SocketBind "socket" "addr";;
     SendTo "socket" "event" "logger";;
     Fst (listen_wait "socket").

Definition logger_runner :=
  ⟨"system";
   let: "db1" := MakeAddress #"127.0.0.1" #3306 in
   let: "db2" := MakeAddress #"localhost" #3306 in
   let: "dbs":= list_make #() in
   let: "dbs" := list_cons "db2" "dbs" in
   let: "dbs" := list_cons "db1" "dbs" in
   let: "logger" := MakeAddress #"0.0.0.0" #80 in
   Start "coordinator" "0.0.0.0" (logger  #"0.0.0.0" "dbs");;
   Start "db1" "127.0.0.1" (db "db1");;
   Start "db2" "localhost" (db "db2");;
   Start "client1" "127.0.0.2" (client #"127.0.0.2" "logger" #"EVENT X");;
   Start "client2" "127.0.0.3" (client #"127.0.0.3" "logger" #"EVENT Y")
  ⟩.
