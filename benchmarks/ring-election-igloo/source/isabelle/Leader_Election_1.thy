(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
section \<open>Protocol Model\<close>

theory Leader_Election_1
  imports 
    Ring_network Leader_Election_0
begin

(******************************************************************************)
subsection \<open>Protocol Model tr1\<close> 
(******************************************************************************)

text \<open>We have just one non-trivial event: the one-shot election of a leader.\<close>
datatype 'b evt1 = B_set_up1 'b | B_accept1 'b 'b | B_elect1 'b | B_skip1
declare evt1.split evt1.split_asm if_split_asm [split]

text \<open>We now introduce a channel. For each i and global state s, chan (s i) contains all messages
      that are to be delivered from i's predecessor to i .\<close>
record 'a tr1_state_local = "'a tr0_state_local" + chan :: "'a set"
type_synonym 'a tr1_state = "'a \<Rightarrow> 'a tr1_state_local"

text \<open>We use the assumptions in the ringnetwork locale.\<close>
context ringnetwork begin

abbreviation add_msg_to_chan where 
  "add_msg_to_chan s x msg \<equiv> s(x := s x \<lparr>chan := insert msg (chan (s x))\<rparr>)"


text \<open>Buffers are initialized as follows: every node ID is contained exactly in the buffer of
      the successor of the ID.\<close> 

definition
  tr1_init :: "'a tr1_state"
where
  "tr1_init \<equiv> \<lambda>l. \<lparr>leader = False, chan = {}\<rparr>"

(* Add your own ID to the next node's buffer*)
definition tr1_set_up where
  "tr1_set_up s x \<equiv> (=) (add_msg_to_chan s (nxt x) x)"

definition tr1_accept where
  "tr1_accept s x msg s' \<equiv>
          msg \<in> chan (s x)      \<comment> \<open>Node x received a name msg higher than his own x.\<close>
        \<and> x \<^bold>< msg
        \<and> s' = add_msg_to_chan s (nxt x) msg"   \<comment> \<open>Forward to next node in ring.\<close>

definition tr1_elect where
  "tr1_elect s x s' \<equiv>  
          x \<in> chan (s x)       \<comment> \<open>Node x received its own name.\<close>
        \<and> s' = add_leader s x"

fun tr1_trans where
  "tr1_trans s (B_set_up1 x) s' \<longleftrightarrow> tr1_set_up s x s'" | 
  "tr1_trans s (B_accept1 x msg) s' \<longleftrightarrow> tr1_accept s x msg s'" | 
  "tr1_trans s (B_elect1 x) s' \<longleftrightarrow> tr1_elect s x s'" | 
  "tr1_trans s B_skip1 s' \<longleftrightarrow> s = s'"

lemmas tr1_trans_cases [elim!] = tr1_trans.elims(2)    \<comment> \<open>always decompose\<close>

definition 
  tr1 :: "('a evt1, 'a tr1_state) ES" where
  "tr1 \<equiv> \<lparr>
    init = (=) tr1_init,
    trans = tr1_trans
  \<rparr>"

lemmas tr1_trans_defs = tr1_set_up_def tr1_accept_def tr1_elect_def
lemmas tr1_defs = tr1_def tr1_init_def tr1_trans_defs

(******************************************************************************)
subsection \<open>tr1 invariants\<close>
(******************************************************************************)

(*********************)
subsubsection \<open>tr1 invariant: @{text "inv_valid_interval"} \<close>
(*********************)

text \<open>If msg is in buffer of x, then all z in the interval between (msg,x) are
      strictly smaller than msg.\<close>

definition inv_valid_interval :: "'a tr1_state \<Rightarrow> bool" where 
  "inv_valid_interval s \<equiv> \<forall>x msg. msg \<in> chan (s x) \<longrightarrow> (\<forall>z \<in> collect msg x. z \<^bold>< msg)"

lemma inv_valid_intervalI [intro]: 
  "(\<And>x msg z. \<lbrakk>msg \<in> chan (s x); z \<in> collect msg x\<rbrakk> \<Longrightarrow> z \<^bold>< msg) \<Longrightarrow> inv_valid_interval s"
  by(auto simp add: inv_valid_interval_def)

lemma inv_valid_intervalE [elim]: 
  "\<lbrakk>inv_valid_interval s; (\<And>x msg z. \<lbrakk>msg \<in> chan (s x); z \<in> collect msg x\<rbrakk> \<Longrightarrow> z \<^bold>< msg) \<Longrightarrow> Q\<rbrakk> \<Longrightarrow> Q"
  by(simp add: inv_valid_interval_def)

lemma inv_valid_intervalD [dest]: 
  "inv_valid_interval s \<Longrightarrow> (\<And>z. \<lbrakk>msg \<in> chan (s x); z \<in> collect msg x\<rbrakk> \<Longrightarrow> z \<^bold>< msg)"
  by(simp add: inv_valid_interval_def)

lemma reach_inv_valid_interval [simp, intro]:
  assumes "reach tr1 s" shows "inv_valid_interval s"
  using assms
proof(induction s rule: reach.induct)
  case (reach_init s)
  then show ?case 
    by(auto simp add: inv_valid_interval_def tr1_defs)
next
  case (reach_trans s e s')
  then show ?case using \<open>tr1: s\<midarrow>e\<rightarrow> s'\<close> 
    by (fastforce simp add: tr1_defs dest: collect_nxt_r)
qed


(*********************)
subsubsection \<open>tr1 invariant: @{text "inv_leader_top"}\<close> 
(*********************)

text \<open>Only top can be elected leader.\<close> 

definition inv_leader_top :: "'a tr1_state \<Rightarrow> bool" where 
  "inv_leader_top s \<equiv> \<forall>i . leader (s i) \<longrightarrow> i = \<^bold>\<top>"

(* this lemma should already exist somewhere... but find_theorems "_ \<Longrightarrow> ?x = \<^bold>\<top>" doesn't find it. *)
lemma topEqI: "\<lbrakk>\<And>z. z \<noteq> x \<Longrightarrow> z \<^bold>< x\<rbrakk> \<Longrightarrow> x = \<^bold>\<top>" by fastforce

lemma reach_inv_leader_top [simp, intro]:
  assumes "reach tr1 s" shows "inv_leader_top s"
  using assms
proof(induction s rule: reach.induct)
  case (reach_init s)
  then show ?case 
    by(auto simp add: inv_leader_top_def tr1_defs)
next
  case (reach_trans s e s')
  then have "inv_valid_interval s" by simp
  then show ?case using \<open>tr1: s\<midarrow>e\<rightarrow> s'\<close> reach_trans.IH 
    by (auto simp add: tr1_defs inv_leader_top_def)
       (auto intro: topEqI collect_self)
qed


(******************************************************************************)
subsection \<open>Refinement from tr0 to tr1 \<close> 
(******************************************************************************)

fun \<pi>1 where
  "\<pi>1 (B_elect1 x) = B_elect0 x" | 
  "\<pi>1 _ = B_skip0"

text \<open>Mediator function. Project to only the leader function.\<close> 

fun med10x :: "'a tr1_state \<Rightarrow> 'a tr0_state" where
  "med10x s = (\<lambda>l . \<lparr>leader = leader (s l)\<rparr>)"

text \<open>When a node x receives his own number x, then x must be the top
      element. This lemma is used to refine the elect event.\<close>

lemma rcv_own_is_top:
  assumes "inv_valid_interval s" 
  and "x \<in> chan (s x)"
  shows "x = \<^bold>\<top>"
proof(rule ccontr, drule not_sym)
  assume "\<^bold>\<top> \<noteq> x"
  then have "\<^bold>\<top> \<in> collect x x"
    by (rule collect_self)
  then show False using assms(1) assms(2) inv_valid_interval_def not_eq_extremum asym by blast
qed


text \<open>All together now.\<close> 

lemma tr1_refines_tr0: "tr1 \<sqsubseteq>\<^sub>\<pi>1 tr0"
proof (intro simulate_ES_fun_with_invariant) 
  fix s0
  assume "init tr1 s0" 
  then show "init tr0 (med10x s0)" by (auto simp add: tr0_defs tr1_defs)
next
  fix s a s' 
  assume "tr1: s\<midarrow>a\<rightarrow> s'" and "inv_valid_interval s \<and> inv_leader_top s"
  then show "tr0: med10x s\<midarrow>\<pi>1 a\<rightarrow> med10x s'"
    by (cases a) (auto simp add: tr0_defs tr1_defs inv_leader_top_def dest: rcv_own_is_top)
qed simp


end (* ringnetwork *)
end