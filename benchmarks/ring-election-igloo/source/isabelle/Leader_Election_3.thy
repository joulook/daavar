(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
section \<open>Decomposition\<close> 

theory Leader_Election_3
  imports 
    Leader_Election_2 
    "../../Interleaving"
    "../../Composition"
    "../../Decomposition"
    "../../Event_Systems_into_IO_Processes"
begin

declare if_split_asm [split]

(******************************************************************************)
subsection \<open>Decomposed Nodes and Environment Model tr3\<close> 
(******************************************************************************)

text \<open>We decompose the model tr3 intro an environment model and an id-indexed family of models,
one for each node component. \<close>


subsubsection \<open>Node model\<close> 
(*********************)

text \<open>For the system, we use (I/O-)guarded event systems, using Action labels that take a
bio name, an output and an input parameter. The guard can only depend on the output parameter.
Note that a typing guard constraining the input parameter is added in the embedding to a
general event system.\<close> 

datatype 'b evt3e = B_receive3e 'b 'b | B_send3e 'b 'b | B_skip3e
datatype bio3s = B_set_up3s | B_receive3s | B_accept3s | B_send3s | B_elect3s | B_skip3s

text \<open>Our values are defined as Unit (stands for not applicable, no value applies), Msg which 
encodes a single member of the ring network, and AddrMsg, which encodes a member of the ring 
network and an address.\<close> 

datatype ('x,'y) val = 
  Unit | 
  Msg (MsgD: 'x) | 
  AddrMsg (AddrMsgD1: 'x) (AddrMsgD2: 'y)

text \<open>We do not need a @{term "\<pi>3"} translation, since this is embedded in @{term "\<chi>3"}, which 
composes system and environment, which are separate in @{term "tr3"}.\<close> 

text \<open>In order to be able to use bio names, we have to show that the set of bio names is finite.\<close> 

lemma bio3s_UNIV: "UNIV = {B_set_up3s, B_receive3s, B_accept3s, B_send3s, B_elect3s, B_skip3s}"
  by (blast intro: bio3s.exhaust)

instance bio3s :: finite
proof
  show "finite (UNIV::bio3s set)"
    by (auto simp add: bio3s_UNIV)
qed

text \<open>Enforce the receive event to get an input value that uses the Msg constructor. All other
      inputs are set to Unit.\<close>  

fun bio3sType :: "bio3s \<Rightarrow> (('a::countable,'ADDR::countable) val) \<Rightarrow> (('a,'ADDR) val) set" where 
  "bio3sType B_receive3s _ = range Msg" |
  "bio3sType _           _ = {Unit}"

instance val :: (countable, countable) countable 
  by countable_datatype

text \<open>Define some types (outside locale).\<close>
type_synonym 'a tr3s_state_local = "'a tr2_state_local"  
type_synonym 'a tr3s_state = "'a \<Rightarrow> 'a tr3s_state_local"  
type_synonym ('a, 'ADDR) tr3e_state = "('a, 'ADDR) tr2e_state"   (* = 'ADDR \<Rightarrow> 'a set *)

locale bio3sTypeLoc = addressedRingnetwork less top ordr nxt addr for
  less :: "'a::countable \<Rightarrow> 'a \<Rightarrow> bool" (infix "\<^bold><" 50) and
  top :: "'a"  ("\<^bold>\<top>") and
  ordr :: "'a \<Rightarrow> 'a \<Rightarrow> bool" and
  nxt :: "'a \<Rightarrow> 'a" and
  addr :: "'a \<Rightarrow> 'ADDR::countable"
begin

sublocale Typing bio3sType
  by unfold_locales (auto elim: bio3sType.elims)

abbreviation add_msg_to_obuf3_local where 
  "add_msg_to_obuf3_local s msg \<equiv> s\<lparr>obuf2 := insert msg (obuf2 s)\<rparr>"

abbreviation add_msg_to_ibuf3_local where 
  "add_msg_to_ibuf3_local s msg \<equiv> s\<lparr>ibuf2 := insert msg (ibuf2 s)\<rparr>"

lemmas tr3_state_mod_defs =
  add_msg_to_chan2_def

text \<open>For the system component we formulate the initial state as a singleton state, instead of a
predicate on states, since the embedding demands a single initial state.\<close> 

abbreviation tr3s_init_state :: "'a tr3s_state_local" where 
  "tr3s_init_state \<equiv> \<lparr>leader2 = False, ibuf2 = {}, obuf2 = {}\<rparr>"

definition tr3e_init :: "('a,'ADDR) tr3e_state" where
  "tr3e_init \<equiv> (\<lambda>_ .{})"


subsubsection \<open>Node and environment events\<close>
(*******************************************)

text \<open>We specify node events as guarded updates.\<close>

definition tr3s_set_up_init :: "'a \<Rightarrow> ('a tr2_state_local, ('a, 'ADDR) val) guarded_update" where
  "tr3s_set_up_init i s outp \<equiv> \<lparr>
     guard = (outp = Unit), 
     update = (\<lambda>_ . s\<lparr>obuf2 := insert i (obuf2 s)\<rparr>)
   \<rparr>"

text \<open>If we receive something unexpected (i.e., not something of type 'a), then
   we ignore it (but still perform the bio).\<close>

definition tr3s_receive where
  "tr3s_receive i s v \<equiv> \<lparr>
     guard = (v = Unit), 
     update = (\<lambda>w. s\<lparr>ibuf2 := insert (MsgD w) (ibuf2 s)\<rparr>)  \<comment> \<open>typing ensures that w is a message\<close>
  \<rparr>"

definition tr3e_receive :: "('a,'ADDR) tr3e_state \<Rightarrow> 'a \<Rightarrow> 'a \<Rightarrow> ('a,'ADDR) tr3e_state \<Rightarrow> bool" where
  "tr3e_receive s x msg s' \<equiv> msg \<in> s (addr x) \<and> s = s'"

definition tr3s_accept where
  "tr3s_accept x s v \<equiv> case v of 
     Msg msg \<Rightarrow> \<lparr>
       guard = msg \<in> ibuf2 s \<and> x \<^bold>< msg, 
       update = (\<lambda>_. s\<lparr>obuf2 := insert msg (obuf2 s)\<rparr>) \<rparr> | 
     _ \<Rightarrow> gNull s v"

definition tr3s_send where
  "tr3s_send i ad s v \<equiv> case v of 
     AddrMsg msg ad' \<Rightarrow> \<lparr>
       guard = msg \<in> obuf2 s \<and> ad = ad', 
       update = \<lambda>_. s \<rparr> |
     _ \<Rightarrow> gNull s v"

definition tr3e_send where
  "tr3e_send s x msg \<equiv> (=) (s(addr (nxt x) := insert msg (s (addr (nxt x)))))"

definition tr3s_elect where
  "tr3s_elect i s v \<equiv> \<lparr>
     guard = i \<in> ibuf2 s \<and> v = Unit, 
     update = \<lambda>_. s\<lparr>leader2 := True\<rparr>
  \<rparr>"

definition tr3s_skip where
  "tr3s_skip \<equiv> gSkip"

definition tr3e_skip where
  "tr3e_skip s s' \<equiv> s = s'"


subsubsection \<open>Individual node model, system model, and environment model\<close>
(********************************************************************************)

fun tr3s_IOGES :: "'a \<Rightarrow> 'ADDR \<Rightarrow> ('a tr3s_state_local, bio3s, ('a,'ADDR) val) IOGES" where
  "tr3s_IOGES i ad B_set_up3s = tr3s_set_up_init i"
| "tr3s_IOGES i ad B_receive3s = tr3s_receive i"
| "tr3s_IOGES i ad B_accept3s = tr3s_accept i"
| "tr3s_IOGES i ad B_send3s = tr3s_send i ad"
| "tr3s_IOGES i ad B_elect3s = tr3s_elect i"
| "tr3s_IOGES i ad B_skip3s = tr3s_skip"

text \<open>The primed version @{term "tr3s_IOGES'"} requires that @{term "ad = addr (nxt i)"}.\<close> 

abbreviation tr3s_IOGES' where "tr3s_IOGES' i \<equiv> tr3s_IOGES i (addr (nxt i))"


text \<open>Construct event system corresponding to the composition of the individual family members.
      Note that despite system events having a label of type 'a, tr3s is event system that has 
      labels of type @{typ "'a option \<times> 'a"}. The second component is from the event, the first encodes
      which family member (Some x) performed the event, and is put in the output part of the action.
      In the input, we simply put None at the same position.\<close> 

definition tr3s_fES where
  "tr3s_fES i \<equiv> IOGES_to_ES (tr3s_IOGES' i) ((=) tr3s_init_state)"

definition tr3s where
  "tr3s = \<parallel>\<parallel> tr3s_fES"

lemma init_tr3s[simp]: "init (\<parallel>\<parallel> tr3s_fES) = (=) (\<lambda>_. tr3s_init_state)"
  by (auto simp add: interleave_fES_def tr3s_fES_def IOGES_to_ES_def)

lemmas tr3s_trans_defs = 
  tr3s_set_up_init_def tr3s_receive_def tr3s_accept_def tr3s_send_def tr3s_elect_def tr3s_skip_def

lemmas tr3s_defs = tr3s_def tr3s_fES_def tr3s_trans_defs

fun tr3e_trans where
  "tr3e_trans s (B_receive3e x msg) s' \<longleftrightarrow> tr3e_receive s x msg s'" |
  "tr3e_trans s (B_send3e x msg) s' \<longleftrightarrow> tr3e_send s x msg s'" |
  "tr3e_trans s (B_skip3e) s' \<longleftrightarrow> tr3e_skip s s'"

definition tr3e :: "('a evt3e, 'ADDR \<Rightarrow> 'a set) ES"
  where "tr3e \<equiv> \<lparr>init = (=) tr3e_init, trans = tr3e_trans\<rparr>"

lemmas tr3e_defs = tr3e_def tr3e_init_def tr3e_receive_def tr3e_send_def tr3e_skip_def


(********************************************************************************)
subsection \<open>Composition of system and environment models\<close>
(********************************************************************************)

text \<open>In the following we define how System and Environment events can be combined.
  The receive and send events of System and Environment have their parameters glued together
  by an if condition.\<close>  

fun \<chi>3 :: "('a \<times> (bio3s, ('a, 'ADDR) val) action) \<times> 'a evt3e \<rightharpoonup> ('a,'ADDR) evt2" where
  "\<chi>3 ((i, ActBio B_set_up3s Unit Unit), B_skip3e) = Some (B_set_up2 i)" |
  "\<chi>3 ((i, ActBio B_receive3s Unit (Msg msg)), B_receive3e i' msg') =
          (if i=i' \<and> msg=msg' then Some (B_receive2 i msg) else None)" |
  "\<chi>3 ((i, ActBio B_accept3s (Msg msg) Unit), B_skip3e) = Some (B_accept2 i msg)" |
  "\<chi>3 ((i, ActBio B_send3s (AddrMsg msg nxtaddr) Unit), B_send3e i' msg') = 
          (if i=i' \<and> msg=msg' \<and> nxtaddr = addr (nxt i) 
           then Some (B_send2 i msg nxtaddr) else None)" |
  "\<chi>3 ((i, ActBio B_elect3s Unit Unit), B_skip3e) = Some (B_elect2 i)" |
  "\<chi>3 ((i, ActBio B_skip3s Unit Unit), B_skip3e) = Some B_skip2" |
  "\<chi>3  _ = None"

lemma \<chi>3_invert [simp]: 
  "\<chi>3 x = Some (B_set_up2 i) \<longleftrightarrow> x = ((i, ActBio B_set_up3s Unit Unit), B_skip3e)"
  "\<chi>3 x = Some (B_receive2 i msg) \<longleftrightarrow> x = ((i, ActBio B_receive3s Unit (Msg msg)), B_receive3e i msg)"
  "\<chi>3 x = Some (B_accept2 i msg) \<longleftrightarrow> x = ((i, ActBio B_accept3s (Msg msg) Unit), B_skip3e)"
  "\<chi>3 x = Some (B_send2 i msg a) \<longleftrightarrow> a = addr (nxt i) \<and> x = ((i, ActBio B_send3s (AddrMsg msg a) Unit), B_send3e i msg)"
  "\<chi>3 x = Some (B_elect2 i) \<longleftrightarrow> x = ((i, ActBio B_elect3s Unit Unit), B_skip3e)"
  "\<chi>3 x = Some (B_skip2) \<longleftrightarrow> (\<exists> i . x = ((i, ActBio B_skip3s Unit Unit), B_skip3e))" (* CHECK! *)
  by (auto elim: \<chi>3.elims)

definition tr3 where
  "tr3 \<equiv> (tr3s \<parallel>\<chi>3 tr3e)"

lemmas tr3_defs = tr3_def tr3s_defs tr3e_defs


(********************************************************************************)
subsection \<open>Trace equivalence of composed and monolithic systems\<close>
(********************************************************************************)

lemma tr2_equiv_tr3: "traces tr2 = traces tr3"
  unfolding tr3_def tr3s_def
proof (intro decomposition_comp_family_env_equiv)
  fix u g u'
  assume "tr2: u\<midarrow>g\<rightarrow> u'" (* and "reach tr2 u" *)
  then show "\<exists>i e f. \<chi>3 ((i, e), f) = Some g"
    by (induction g u rule: tr2_trans_induct) (auto simp add: tr2_defs)
next
  fix s t 
  show "init tr2 (s, t) = ((\<forall>i. init (tr3s_fES i) (s i)) \<and> init tr3e t)"
    by (auto simp add: tr2_defs tr3_defs)
next
  fix i e f g s t s' t'
  assume "\<chi>3 ((i, e), f) = Some g"
  then show "(tr2: (s, t) \<midarrow>g\<rightarrow> (s', t')) \<longleftrightarrow>
             (tr3s_fES i: s i\<midarrow>e\<rightarrow> s' i) \<and> (tr3e: t\<midarrow>f\<rightarrow> t') \<and> (\<forall>j. j \<noteq> i \<longrightarrow> s' j = s j)"
    by (induction "(s, t)" g "(s', t')" rule: tr2_trans_induct)
       (auto simp add: tr2_defs tr3_defs IOGES_to_ES_def)
qed

(******************************************************************************)
subsection \<open>Property preservation\<close>
(******************************************************************************)

lemma tr3_trace_inclusion_tr0: "(map \<pi>1)`(map \<pi>2)`traces tr3 \<subseteq> traces tr0"
proof-
  have "(map \<pi>2)`traces tr3 \<subseteq> traces tr1"
    using tr2_refines_tr1 simulation_soundness tr2_equiv_tr3 by fastforce
  moreover have "(map \<pi>1)`traces tr1 \<subseteq> traces tr0"
    using tr1_refines_tr0 simulation_soundness by fastforce
  ultimately show ?thesis by blast
qed

abbreviation U_2 where 
  "U_2 \<tau> \<equiv> \<forall> i j. B_elect2 i \<in> set \<tau> \<and> B_elect2 j \<in> set \<tau>  \<longrightarrow> i = j"

lemma tr3_satisfies_property': "tr3 \<Turnstile>\<^sub>E\<^sub>S Collect U_2"
proof (intro trace_propertyI)
  fix \<tau>
  assume "\<tau> \<in> traces tr3"
  then have "(map \<pi>1) ((map \<pi>2) \<tau>) \<in> Collect U_0" 
    using tr3_trace_inclusion_tr0 tr0_satisfies_property' by blast
  then show "\<tau> \<in> Collect U_2" by force
qed

end \<comment> \<open>locale bio3sTypeLoc\<close>

end
