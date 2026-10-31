(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
section \<open>IO Specification and Trace Equivalence\<close> 

theory Leader_Election_4
  imports 
    Leader_Election_3
    "../../Event_Systems_into_IO_Processes"
    "../../IO_Processes_into_IO_Separation_Logic"
    "../../IO_Behavior"
begin

declare if_split_asm [split]

text \<open>Define some types (outside locale).\<close>

type_synonym ('a,'ADDR) tr3s_proc = "(bio3s, ('a, 'ADDR) val) process"
type_synonym ('a,'ADDR) tr3s_assn = "(bio3s, ('a, 'ADDR) val) cassn"

(**************************************************************************************************)
subsection \<open>Translation of Guarded Event System into IO-specification.\<close>
(**************************************************************************************************)

context bio3sTypeLoc 
begin

text \<open>Embedding of tr3s IOGES into a process.\<close>

definition pr_tr3s :: "'a \<Rightarrow> 'a tr2_state_local \<Rightarrow> ('a,'ADDR) tr3s_proc" where
  "pr_tr3s x s \<equiv> pr (tr3s_IOGES' x) s" (* initially, s = tr3s_init_state *)

text \<open>Embedding of the process into I/O spec.\<close>

definition iospec_tr3s :: "place \<Rightarrow> 'a \<Rightarrow> 'a tr2_state_local \<Rightarrow> ('a, 'ADDR) tr3s_assn"
  where "iospec_tr3s t x s \<equiv> embedp (pr_tr3s x s) t"

text \<open>Correctness of the translation to the I/O spec.\<close>

lemma tr3s_IOGES_trace_equiv_iospec_tr3s:
    "traces_assn (ExT (\<lambda>t. CTok t \<star> iospec_tr3s t i s))
   = traces (IOGES_to_ES ((tr3s_IOGES' i)) ((=) s))" (is "?L = ?R")
proof-
  have "?L = traces_opsem (pr_tr3s i s)" by(simp add: iospec_tr3s_def trace_equivalences)
  also have "... = ?R" by(simp add: pr_tr3s_def emb_opsem_equiv)
  finally show ?thesis by blast
qed


subsubsection \<open>Unfolding of the IO-spec definition\<close>
(**************************************************)

text \<open>We have already defined the IO-specification of the guarded event system, and proven the two
      trace equivalent. In order to use the IO-spec in a code verifier, we have to unfold it.
      This presents a technical challenge: so far, the Choice over all BIOs has been defined via
      the operator @{text "\<Oplus>\<^sub>b\<^sub>i\<^sub>o"}, which picks BIOs in an arbitrary order. Because this order is
      not defined (but rather uses the Hilbert Choice operator), we cannot formulate a syntactically
      equivalent IO-spec.
      Thus, we generate an IO-spec using the @{text "\<Oplus>\<^sup>l\<^sub>b\<^sub>i\<^sub>o"} operator, which works as @{text "\<Oplus>\<^sub>b\<^sub>i\<^sub>o"},
      but takes an (ordered) list instead of an (unordered) set of BIOs. We then prove trace equivalence
      to the original specification.\<close>

text \<open>Let this be the order in which events appear in the IO-spec.\<close>

definition bio3s_ordered :: "bio3s list" where
  "bio3s_ordered = [B_set_up3s, B_receive3s, B_accept3s, B_send3s, B_elect3s, B_skip3s]"

lemma bio3s_ordered_UNIV: "set bio3s_ordered = (UNIV::bio3s set)" 
  by (auto simp add: bio3s_ordered_def intro: bio3s.exhaust)

definition pr_tr3s_ordered :: "'a \<Rightarrow> 'ADDR \<Rightarrow> 'a tr2_state_local \<Rightarrow> ('a,'ADDR) tr3s_proc" where
  "pr_tr3s_ordered i ad s \<equiv> lPr bio3s_ordered (tr3s_IOGES i ad) s"

lemmas pr_tr3s_ordered_defs = pr_tr3s_ordered_def bio3s_ordered_def

lemma pr_tr3s_ordered_opsem_equiv: "ad = addr (nxt i) \<Longrightarrow> 
  traces_opsem (pr_tr3s i s) = traces_opsem (pr_tr3s_ordered i ad s)"
  by(auto simp add: pr_tr3s_def pr_tr3s_ordered_def bio3s_ordered_UNIV 
          intro: bisim_lPr_pr del: equalityI)

definition IOspec :: "place \<Rightarrow> 'a \<Rightarrow> 'ADDR \<Rightarrow> 'a tr2_state_local \<Rightarrow> ('a, 'ADDR) tr3s_assn"
  where "IOspec t i ad s \<equiv> embedp (pr_tr3s_ordered i ad s) t"

abbreviation UDP_send where
  "UDP_send \<equiv> CBio B_send3s"


lemma leader_election_node_iospec: 
  "IOspec t i ad s = 
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      if v = Unit 
      then ExT (\<lambda>t'. ExV (\<lambda>w. 
        CBio B_set_up3s t v w t' \<star> IOspec t' i ad (s\<lparr>obuf2 := insert i (obuf2 s)\<rparr>))) 
      else Bool True) \<star>
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      if v = Unit 
      then ExT (\<lambda>t'. ExV (\<lambda>w. 
        CBio B_receive3s t v w t' \<star>  IOspec t' i ad (s\<lparr>ibuf2 := insert (MsgD w) (ibuf2 s)\<rparr>))) 
      else Bool True) \<star>
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      if case v of Msg msg \<Rightarrow> msg \<in> ibuf2 s \<and> i \<^bold>< msg | _ \<Rightarrow> False 
      then ExT (\<lambda>t'. ExV (\<lambda>w. 
        CBio B_accept3s t v w t' \<star> IOspec t' i ad (s\<lparr>obuf2 := insert (MsgD v) (obuf2 s)\<rparr>))) 
      else Bool True) \<star>
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      if case v of AddrMsg msg ad' \<Rightarrow> msg \<in> obuf2 s \<and> ad' = ad | _ \<Rightarrow> False 
      then ExT (\<lambda>t'. ExV (\<lambda>w. 
        UDP_send t v w t' \<star> IOspec t' i ad s)) 
      else Bool True) \<star>
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      if i \<in> ibuf2 s \<and> v = Unit 
      then ExT (\<lambda>t'. ExV (\<lambda>w. CBio B_elect3s t v w t' \<star> IOspec t' i ad (s\<lparr>leader2 := True\<rparr>))) 
      else Bool True) \<star>
    (\<forall>\<^sup>\<star> (\<lambda>v. 
      ExT (\<lambda>t'. ExV (\<lambda>w. CBio B_skip3s t v w t' \<star> IOspec t' i ad s))) \<star> 
    Bool True))))))"

  \<comment> \<open>Note first part of proof only work on LHS (in particular, lPr.code).\<close>
  by (simp add: IOspec_def[where t=t] pr_tr3s_ordered_defs lPr.code[where s="s"])
     (auto simp add: IOspec_def pr_tr3s_ordered_defs tr3s_trans_defs lBChoice.simps 
                     embedp_VChoice_is_AllStar  
           intro!: arg_cong[where f="\<forall>\<^sup>\<star>"] 
           split: val.split)

text \<open>This lemma proves the equivalence between the original, unordered IO-spec, and the unfolded,
      ordered IO-spec.\<close>

lemma translation_iospec_tr3:
  "traces_assn (ExT (\<lambda>t. CTok t \<star> iospec_tr3s t i s)) = 
   traces_assn (ExT (\<lambda>t. CTok t \<star> IOspec t i (addr (nxt i)) s))"
  by(auto del: equalityI simp add: pr_tr3s_ordered_opsem_equiv IOspec_def iospec_tr3s_def 
          intro: traces_assn_embedI)

abbreviation iospec_traces where
  "iospec_traces i \<equiv> traces_assn (ExT (\<lambda>t. CTok t \<star> iospec_tr3s t i tr3s_init_state))"

definition traces_tr4s where
  "traces_tr4s \<equiv> \<parallel>\<parallel> (\<lambda>i. iospec_traces i)"
definition traces_tr4 where
  "traces_tr4 \<equiv> (traces_tr4s \<parallel>\<chi>3 traces tr3e)"

lemma traces_tr3s_tr4: "traces_tr4s = traces tr3s"
  apply(auto simp add: tr3s_def tr3s_fES_def traces_tr4s_def translation_iospec_tr3[symmetric] 
                       interleaving_composition tr3s_IOGES_trace_equiv_iospec_tr3s del: equalityI)
  apply(rule arg_cong[where ?f="interleave_tsf"])
  by (auto simp add: tr3s_fES_def)

lemma traces_tr3_tr4: "traces_tr4 = traces tr3"
  by(auto simp add: traces_tr3s_tr4 traces_tr4_def tr3_def trace_composition del: equalityI)

(******************************************************************************)
subsection \<open>Property preservation\<close>
(******************************************************************************)

lemma tr4_trace_inclusion_tr0: "(map \<pi>1)`(map \<pi>2)`traces_tr4 \<subseteq> traces tr0"
  by(auto simp add: traces_tr3_tr4 tr3_trace_inclusion_tr0)

lemma tr4_satisfies_property: "traces_tr4 \<subseteq> Collect U_2"
  using tr3_satisfies_property' by(auto intro: trace_propertyI simp add: traces_tr3_tr4)

text \<open>Quod erat demonstrandum. This concludes our Leader Election Case study.\<close> 

end \<comment> \<open>locale bio3sTypeLoc\<close>

end
