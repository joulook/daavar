(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
chapter \<open>Igloo Case Study: Leader Election\<close>
text\<open>In this initial case study, which is used as a running example for our methodology in the Igloo 
paper, we model and implement a simple protocol which elects a leader of a number of nodes distributed
in a ring network.

First, we model an system that elects a leader in a single event, thus satisfying the protocol's 
central property: namely, that it elects a single leader. We then refine this model into one that 
implements a protocol that is based on 'Modeling in Event-B' book, Chapter 10. It works on a ring 
network topology which we also formalize. Then, we build the interface model, which has additional
input and output buffers. In a subsequent step, this monolithic model is decomposed into a family of 
event system, one for each node, and an environment over which the nodes communicate. Finally, the 
event system for a node is translated into a trace-equivalent IO specification which then is used
to verify the implementations. See the separate files for the code verification parts.
\<close>
(******************************************************************************)
section \<open>Abstract Model\<close>
(******************************************************************************)

theory Leader_Election_0
  imports 
    "../../Event_Systems"
begin

text \<open>Abstract Model that only specifies the problem and solves it in an artificial way by selecting
the leader in a single step.\<close> 

(*********************)
subsection \<open>Abstract Model tr0\<close>  
(*********************)

text \<open>We have just one non-trivial event: the one-shot election of a leader.\<close>

datatype 'b evt0 = B_elect0 'b | B_skip0
declare evt0.split evt0.split_asm if_split_asm [split]

text \<open>The abstract state simply states for each node whether it considers itself to be elected.\<close>

record 'a tr0_state_local = leader :: bool
type_synonym 'a tr0_state = "'a \<Rightarrow> 'a tr0_state_local"

text \<open>The initial value is False for all nodes.\<close>

definition tr0_init :: "'a tr0_state" where
  "tr0_init \<equiv> (\<lambda>x. \<lparr>leader = False\<rparr>)"

abbreviation add_leader where "add_leader s i \<equiv> s(i := s i \<lparr>leader := True\<rparr>)"

text \<open>We allow the same node to be elected multiple times as leader, but never two different nodes.\<close>

definition tr0_elect :: "'a tr0_state \<Rightarrow> 'a \<Rightarrow> 'a tr0_state \<Rightarrow> bool" where
  "tr0_elect s i s' \<equiv> (\<forall> j . leader (s j) \<longrightarrow> i = j) \<and> s' = add_leader s i"

fun tr0_trans :: "'a tr0_state \<Rightarrow> 'a evt0 \<Rightarrow> 'a tr0_state \<Rightarrow> bool" where
  "tr0_trans s (B_elect0 i) s' \<longleftrightarrow> tr0_elect s i s'" |
  "tr0_trans s B_skip0 s' \<longleftrightarrow> s = s'"

lemmas tr0_trans_induct = tr0_trans.induct [case_names tr0_elect tr0_skip]

definition tr0 :: "('a evt0, 'a tr0_state) ES" where
  "tr0 \<equiv> \<lparr>
    init = (=) tr0_init,
    trans = tr0_trans
  \<rparr>"

lemmas tr0_trans_defs = tr0_elect_def
lemmas tr0_defs = tr0_def tr0_init_def tr0_trans_defs

lemma tr0_trans [simp]: "trans tr0 = tr0_trans" by (simp add: tr0_def)


(******************************************************************************)
subsection \<open>tr0 trace property: No Multiple Leaders (@{term "U_0"})\<close> 
(******************************************************************************)

text \<open>We first prove an invariant: for all reachable states, no two different nodes exist that are 
      both leaders in that state.\<close>

definition inv_no_multiple_leaders :: "'a tr0_state \<Rightarrow> bool" where 
  "inv_no_multiple_leaders s \<longleftrightarrow> (\<forall>i j . leader (s i) \<longrightarrow> leader (s j) \<longrightarrow> i = j)"

lemmas inv_no_multiple_leadersI = inv_no_multiple_leaders_def [THEN iffD2, rule_format]
lemmas inv_no_multiple_leadersE [elim] = inv_no_multiple_leaders_def [THEN iffD1, elim_format, rule_format]

lemma reach_inv_no_multiple_leaders: "reach tr0 s \<Longrightarrow> inv_no_multiple_leaders s"
  by (induction s rule: reach.induct)
     (auto simp add: tr0_defs intro!: inv_no_multiple_leadersI elim!: tr0_trans.elims)


text \<open>We now show the desired trace property:\<close>

definition U_0 :: "'a evt0 trace \<Rightarrow> bool" where 
  "U_0 \<tau> \<longleftrightarrow> (\<forall> i j . B_elect0 i \<in> set \<tau> \<and> B_elect0 j \<in> set \<tau>  \<longrightarrow> i = j)"

lemmas U_0_I = U_0_def [THEN iffD2, rule_format]
lemmas U_0_E [elim] = U_0_def [THEN iffD1, elim_format, rule_format]

abbreviation elect_leader where     \<comment> \<open>auxiliary trace-state invariant needed in proof below\<close>
  "elect_leader \<tau> s \<equiv> \<forall>i. B_elect0 i \<in> set \<tau> \<longrightarrow> leader (s i)"   

lemma tr0_satisfies_property': "tr0 \<Turnstile>\<^sub>E\<^sub>S Collect U_0"
proof (rule trace_property_rule[where I="elect_leader"])
  fix s :: "'a tr0_state" and s' \<tau> e s''
  assume "init tr0 s" "tr0: s \<midarrow>\<langle>\<tau>\<rangle>\<rightarrow> s'" "tr0: s'\<midarrow>e\<rightarrow> s''" "elect_leader \<tau> s'" "reach tr0 s'"
  then show "elect_leader (\<tau> @ [e]) s''"
    by (induction s' e s'' rule: tr0_trans_induct) (auto simp add: tr0_trans_defs)
next
  fix \<tau> and s :: "'a tr0_state"
  assume "elect_leader \<tau> s" "reach tr0 s"
  then show "\<tau> \<in> Collect U_0"
    by (auto intro!: U_0_I dest!: reach_inv_no_multiple_leaders)
qed auto


end
