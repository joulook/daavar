(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
section \<open>Interface Model\<close>

text \<open>We add local input buffers and output buffers. Here, they are modelled as sets and messages
       are never removed from the buffers.\<close>
theory Leader_Election_2
  imports 
    Leader_Election_1
begin

subsection \<open>Local Buffers Model tr2\<close>
record 'a tr2_state_local =
  leader2 :: bool (* has this node declared itself a leader? *)
  ibuf2 :: "'a set"     \<comment> \<open>incoming internal buffer of a node\<close> 
  obuf2 :: "'a set"     \<comment> \<open>outgoing internal buffer of a node\<close>

text \<open>The global state is a pair of the nodes' local states, and the channels.\<close>

type_synonym 'a tr2s_state = "'a \<Rightarrow> 'a tr2_state_local"
type_synonym ('a, 'ADDR) tr2e_state = "'ADDR \<Rightarrow> 'a set"   (* CHSP: a record in the paper *)

type_synonym ('a,'ADDR) tr2_state = "'a tr2s_state \<times> ('a, 'ADDR) tr2e_state"     

datatype ('a,'ADDR) evt2 = 
  B_set_up2 'a | B_receive2 'a 'a | B_accept2 'a 'a | 
  B_elect2 'a | B_send2 'a 'a  'ADDR | B_skip2

declare evt2.split evt2.split_asm [split]

text \<open>
\begin{verbatim}
       receive
       /  |  \
      /   |   \
 elect reject accept
                |
              send
\end{verbatim}
send refines both @{term "set_up"} and @{term "accept"}.
all other event refine skip
we need to prove: all messages msg that send at node @{term x} wants to put into 
@{term "chan (nxt x)"} are either:
\begin{itemize}
\item @{term "msg = x"}       (i.e., we refine the @{term "set_up"} event)
\item @{term "msg \<in> chan x"}   (i.e., we refine the accept event)
\end{itemize}

Can we come up with some more generic solution?

The problem in this instance is that the message msg is no longer in ibuf when it is in obuf. Thus
we can't use the invariant @{term "inv_buffers2"}, which requires that msg is in ibuf in the current 
state (as opposed to some past state).

We could prove about our event system that the set of traces from some state s is monotonic in the
messages contained in its various buffers. Thus, if we remove messages from buffers, we get a strict
subset of traces (trace inclusion, i.e., property preservation).
\<close>

definition add_msg_to_ibuf where 
  "add_msg_to_ibuf s x msg \<equiv> s(x := s x \<lparr>ibuf2 := insert msg (ibuf2 (s x))\<rparr>)"

definition add_msg_to_obuf where 
  "add_msg_to_obuf s x msg \<equiv> s(x := s x \<lparr>obuf2 := insert msg (obuf2 (s x))\<rparr>)"

definition add_msg_to_chan2 where
  "add_msg_to_chan2 s x msg \<equiv> s(x := insert msg (s x))"
 
abbreviation add_leader2 where
  "add_leader2 s x s' \<equiv> s' = s(x := s x \<lparr>leader2 := True\<rparr>)"

lemmas tr2_state_mod_defs = add_msg_to_ibuf_def add_msg_to_obuf_def add_msg_to_chan2_def

context addressedRingnetwork begin

definition
  tr2_init :: "('a, 'ADDR) tr2_state"
  where
  "tr2_init \<equiv> ((\<lambda>l . \<lparr>leader2 = False, ibuf2 = {}, obuf2 = {}\<rparr>), (\<lambda>_.{}))"

(* Add your own ID to the next node's buffer*)
definition
  tr2_set_up_init
where
  "tr2_set_up_init s x \<equiv> (=) (add_msg_to_obuf s x x)"

definition
  tr2_receive
where
  "tr2_receive s t x msg s' \<equiv> 
          msg \<in> t (addr x)
        \<and> s' = add_msg_to_ibuf s x msg"

definition
  tr2_accept
where
  "tr2_accept s x msg s' \<equiv> 
          msg \<in> ibuf2 (s x) \<comment> \<open>Node x received a name msg higher than his own x.\<close>
        \<and> x \<^bold>< msg
        \<and> s' = add_msg_to_obuf s x msg"

definition
  tr2_send
where
  "tr2_send s t x msg a t' \<equiv> 
          msg \<in> obuf2 (s x)
        \<and> a = addr (nxt x)
        \<and> t' = add_msg_to_chan2 t a msg"

definition
  tr2_elect
where
  "tr2_elect s x s' \<equiv> 
          x \<in> ibuf2 (s x) \<comment> \<open>Node x received his own name.\<close>
        \<and> add_leader2 s x s'"

definition tr2_skip where "tr2_skip s s' \<equiv> s = s'"

fun tr2_trans where
  "tr2_trans (s,t) (B_set_up2 x) (s',t') \<longleftrightarrow> tr2_set_up_init s x s' \<and> t' = t" |
  "tr2_trans (s,t) (B_receive2 x msg ) (s',t') \<longleftrightarrow> tr2_receive s t x msg s' \<and> t' = t" |
  "tr2_trans (s,t) (B_accept2 x msg) (s',t') \<longleftrightarrow> tr2_accept s x msg s' \<and> t' = t" |
  "tr2_trans (s,t) (B_send2 x msg a) (s',t') \<longleftrightarrow> tr2_send s t x msg a t' \<and> s' = s" |
  "tr2_trans (s,t) (B_elect2 x) (s',t') \<longleftrightarrow> tr2_elect s x s' \<and> t' = t" |
  "tr2_trans (s,t) (B_skip2) (s',t') \<longleftrightarrow> tr2_skip s s' \<and> t' = t" 

lemmas tr2_trans_induct = 
  tr2_trans.induct [case_names B_set_up2 B_receive2 B_accept2 B_send2 B_elect2 B_skip2]


definition tr2  :: "(('a,'ADDR) evt2, ('a,'ADDR) tr2_state) ES" where
  "tr2 \<equiv> \<lparr>
    init = (=) tr2_init,
    trans = tr2_trans
  \<rparr>"

lemmas tr2_trans_defs = 
  tr2_set_up_init_def tr2_receive_def tr2_accept_def tr2_send_def tr2_elect_def tr2_skip_def
  tr2_state_mod_defs

lemmas tr2_defs = tr2_def tr2_init_def tr2_trans_defs

(******************************************************************************)
subsection \<open>tr2 invariants\<close>
(******************************************************************************)

find_theorems name: "tr2_state_local" name: "tr"

text \<open>Mediator function. Project out the internal buffers.\<close>

fun med21xlocal where
  "med21xlocal l = \<lparr>leader = leader l, chan = chan l\<rparr>"

abbreviation med21x :: "('a,'ADDR) tr2_state \<Rightarrow> 'a tr1_state" where
  "med21x st \<equiv> (case st of (s,t) \<Rightarrow> (\<lambda>x . \<lparr>leader = leader2 (s x), chan = t (addr x)\<rparr>))"

text \<open>All messages in obuf originate from chan or are due to the setup event in which msg = x.\<close> 

definition inv_buffers2 :: "('a,'ADDR) tr2_state \<Rightarrow> bool" where 
  "inv_buffers2 st \<equiv> (case st of (s,t) \<Rightarrow> 
      \<forall>x msg. (msg \<in> obuf2 (s x) \<longrightarrow> msg \<in> t (addr x) \<and> x \<^bold>< msg \<or> msg = x) \<and>
              (msg \<in> ibuf2 (s x) \<longrightarrow> msg \<in> t (addr x)))"

lemma inv_buffers2I:
  "\<lbrakk>\<And>x msg. \<lbrakk>msg \<in> obuf2 (s x); msg \<noteq> x\<rbrakk> \<Longrightarrow> msg \<in> t (addr x); 
    \<And>x msg. \<lbrakk>msg \<in> obuf2 (s x); msg \<noteq> x\<rbrakk> \<Longrightarrow> x \<^bold>< msg; 
    \<And>x msg. msg \<in> ibuf2 (s x) \<Longrightarrow> msg \<in> t (addr x)\<rbrakk>
\<Longrightarrow> inv_buffers2 (s, t)"
  by (auto simp add: inv_buffers2_def)

lemma inv_buffers2E [elim]:
  "\<lbrakk>inv_buffers2 (s,t); 
          \<lbrakk>\<And>x msg. \<lbrakk>msg \<in> obuf2 (s x); msg \<noteq> x\<rbrakk> \<Longrightarrow> msg \<in> t (addr x); 
           \<And>x msg. \<lbrakk>msg \<in> obuf2 (s x); msg \<noteq> x\<rbrakk> \<Longrightarrow> x \<^bold>< msg; 
           \<And>x msg.  msg \<in> ibuf2 (s x) \<Longrightarrow> msg \<in> t (addr x)\<rbrakk> \<Longrightarrow> R\<rbrakk> 
  \<Longrightarrow> R"
  by(auto simp add: inv_buffers2_def)

lemma reach_inv_buffers2 [simp, intro]: "reach tr2 s \<Longrightarrow> inv_buffers2 s"
  by (induction s rule: reach.induct)
     (fastforce simp add: tr2_defs tr2_state_mod_defs intro!: inv_buffers2I elim!: tr2_trans.elims)+

(*********************)
subsection \<open>Refinement from tr1 to tr2\<close>
(*********************)

fun \<pi>2 :: "('a,'ADDR) evt2 \<Rightarrow> 'a evt1" where
  "\<pi>2 (B_send2 x y a) = (if x = y then B_set_up1 x else B_accept1 x y)" |
  "\<pi>2 (B_elect2 x) = B_elect1 x" |
  "\<pi>2 _ = B_skip1"

lemma tr2_refines_tr1: "tr2 \<sqsubseteq>\<^sub>\<pi>2 tr1"
proof (intro simulate_ES_fun_with_invariant[where I=inv_buffers2]) 
  fix s0
  assume "init tr2 s0" 
  then show "init tr1 (med21x s0)" by (auto simp add: tr1_defs tr2_defs)
next
  fix s a s'
  assume "tr2: s\<midarrow>a\<rightarrow> s'" and "inv_buffers2 s"
  then show "tr1: med21x s\<midarrow>\<pi>2 a\<rightarrow> med21x s'" 
    by (auto 4 3 simp add: tr1_defs tr2_defs tr2_state_mod_defs elim!: tr2_trans.elims)
qed simp

end
end