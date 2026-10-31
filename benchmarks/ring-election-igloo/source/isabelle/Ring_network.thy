(*
  Igloo Case Study: Leader Election

  Author: Tobias Klenze (tobias.klenze@inf.ethz.ch)
          Christoph Sprenger (sprenger@inf.ethz.ch)
  Date: 2019
*)
section \<open>Ring network structure\<close>

theory Ring_network
  imports Main "HOL-Library.Countable_Set"
begin

(******************************************************************************)
subsection \<open>Locale Ringnetwork\<close>  
(******************************************************************************)

text \<open>Our ringnetwork works on an abstract, finite type 'a for which some total
      order with a top element has been defined. We furthermore assume that there
      is some successor function nxt and that in the transitive closure of nxt
      every node is related to every other node (there is only one strongly connected
      component).\<close>  

locale ringnetwork =
  ordering_top ordr for ordr :: "('a::countable) \<Rightarrow> 'a \<Rightarrow> bool" +
  fixes nxt :: "'a \<Rightarrow> 'a"
  assumes singlescc: "\<And>x y . \<exists> k . (nxt ^^ k) x = y"
begin

abbreviation nxt_normal  (infix "\<leadsto>" 50) where
  "x \<leadsto> k \<equiv> (nxt ^^ k) x"

abbreviation nxt_positive  (infix "\<leadsto>\<^sup>+" 50) where
  "x \<leadsto>\<^sup>+ k \<equiv> nxt (x \<leadsto> k)"

text \<open>Whereas the assumption singlescc tells us that there is a k that goes from $l$ to $r$, 
this lemma guarantees the existance of a $k$ such that we get in $k+1$ steps from $l$ to $r$.\<close> 

lemma singlescc_positive_k: "\<exists>k. l \<leadsto>\<^sup>+k = r"
proof -
  obtain k' where "(nxt ^^ k') (nxt l) = r" using singlescc by blast
  thus ?thesis by (auto simp add: funpow_swap1)
qed

(*********************)
subsection \<open>Functions working on cut ring intervals\<close>
(*********************)

text \<open>collects all elements strictly in between l and r.
       collect l l is defined as containing all elements [nxt l,l).\<close> 

function collect where
  "collect l r = (if nxt l = r then {} else insert (nxt l) (collect (nxt l) r))"
by auto
termination
  proof (relation "measure (\<lambda>(l, r). LEAST k. l \<leadsto>\<^sup>+k = r)", simp_all)
    fix l r
    assume "nxt l \<noteq> r" 
    moreover
    from singlescc_positive_k obtain k where "(l \<leadsto>\<^sup>+ k) = r" by blast  
    ultimately
    have "(LEAST k. (l \<leadsto>\<^sup>+ k) = r) = Suc (LEAST k. (l \<leadsto>\<^sup>+ (Suc k)) = r)"
      by(auto simp add: Least_Suc[of _ k] funpow_swap1)
    then show "(LEAST k. ((nxt l) \<leadsto>\<^sup>+ k) = r) < (LEAST k. (l \<leadsto>\<^sup>+ k) = r)"
      by(simp add: funpow_swap1[THEN sym])
  qed

lemma collect_empty [simp]: "collect l (nxt l) = {}"
  by (simp)

lemma collect_insert [simp]: "nxt l \<noteq> r \<Longrightarrow> collect l r = insert (nxt l) (collect (nxt l) r)"
  by (simp)

declare collect.simps[simp del]      \<comment> \<open>avoid simplifier looping\<close>


text \<open>Lemma that shows we can shrink the interval from the right as well (collect itself
      shrinks from the left).\<close> 

lemma collect_nxt_r: "\<lbrakk>z \<in> collect l (nxt r); z \<noteq> r\<rbrakk> \<Longrightarrow> z \<in> collect l r"
  apply(induction l r rule: collect.induct, auto)
  by (metis collect_insert collect_empty empty_iff insert_iff)


text \<open>Widen an interval of collect. This allows us to prove in lemma @{text "collect_self"} 
      that any value is either equivalent to $x$, or contained in @{term "collect x x"}.\<close> 

lemma collect_interval_widen:
  "\<lbrakk>k \<le> (LEAST k. l \<leadsto>\<^sup>+k = r); z \<in> collect (l \<leadsto> k) r\<rbrakk> \<Longrightarrow> z \<in> collect l r"
    apply(induction k, auto)
    by (metis (mono_tags, lifting) Suc_le_lessD collect.elims insertCI not_less_Least)

(* not sure if useful, it's not needed right now. Remove? *)
lemma nxt_add: "(nxt ^^ (k+l)) x = (nxt ^^ l) ((nxt ^^ k) x)"
  using funpow_add comp_def add.commute by metis

lemma LeastI_nxt[simp]: "a \<leadsto> (LEAST k. a \<leadsto> k = b) = b"
proof -
  obtain k where "a \<leadsto> k = b" using singlescc by blast
  then show ?thesis by(rule LeastI)
qed

lemma LeastI_nxt_positive[simp]: "nxt (a \<leadsto> (LEAST k. (a \<leadsto>\<^sup>+ k) = b)) = b"
proof -
  obtain k where "(a \<leadsto>\<^sup>+ k) = b" using singlescc_positive_k by blast
  then show ?thesis by(rule LeastI)
qed

text \<open>Assume that there are elements a, b, and c such that the distance from a to c is higher
      than the distance from a to b. Then the distance from a to c is equal to the distance from
      a to b plus the distance from b to c. 
      We show correctness (distance a to c is equal to distance of b to c plus a to b), and
      leastness (there is no shorter distance).\<close>

lemma nxt_interpolation:
      assumes assm: "(LEAST k. a \<leadsto>\<^sup>+k = c) > (LEAST k. a \<leadsto>\<^sup>+k = b)"
      shows "(LEAST k. a \<leadsto>\<^sup>+k = c) = (LEAST k. b \<leadsto>k = c) + (LEAST k. a \<leadsto>\<^sup>+k = b)"
proof (rule Least_equality, auto)
(* show 'correctness' *)
  show "nxt (a \<leadsto> (LEAST k. (b \<leadsto> k) = c) + (LEAST k. (a \<leadsto>\<^sup>+ k) = b)) = c"
    by(simp add: funpow_add funpow_swap1)
next
(* show 'leastness' *)
  fix y
  show "c = (a \<leadsto>\<^sup>+ y) \<Longrightarrow> (LEAST k. (b \<leadsto> k) = (a \<leadsto>\<^sup>+ y)) + (LEAST k. a \<leadsto>\<^sup>+ k = b) \<le> y"
  proof (rule ccontr, drule not_le_imp_less)
    assume assm1: "c = (a \<leadsto>\<^sup>+ y)" 
       and assm2: "y < (LEAST k. (b \<leadsto> k) = (a \<leadsto>\<^sup>+ y)) + (LEAST k. a \<leadsto>\<^sup>+ k = b)"
    have ygeqk: "(LEAST k . a \<leadsto>\<^sup>+ k = c) \<le> y"
      by (rule Least_le, simp add:  `c = (a \<leadsto>\<^sup>+ y)`)
    then show False
      proof (cases "(LEAST k. a \<leadsto>\<^sup>+ k = b) \<ge> y")
        case True
        then show ?thesis using assm ygeqk by simp
      next
        case False
        then have "\<exists>y1. y = (LEAST k. a \<leadsto>\<^sup>+ k = b) + y1" by (auto intro: le_Suc_ex)
        then obtain y1 where y1def: "y = (LEAST k. a \<leadsto>\<^sup>+ k = b) + y1" by blast
        from assm2 have y1LEAST: "(LEAST k. (b \<leadsto> k) = c) > y1" by(simp add: y1def assm1) 
        have "c = (b \<leadsto> y1)"
          by(simp add: assm1 y1def funpow_add funpow_swap1 add.commute)
        then show ?thesis      
          using not_less_Least y1LEAST by blast
      qed
  qed
qed

text \<open>@{text "collect x x"} is $[nxt x,x)$, thus if we add $x$, we should get @{term "UNIV"}. 
This is implied by the following lemma.\<close> 

lemma collect_self: "z \<noteq> x \<Longrightarrow> z \<in> collect x x"
proof(rule collect_interval_widen[of "(LEAST k. x \<leadsto>\<^sup>+k = z)"], rule ccontr)
 assume "z \<noteq> x"
 assume "\<not> (LEAST kxz. x \<leadsto>\<^sup>+kxz = z) \<le> (LEAST kxx. x \<leadsto>\<^sup>+kxx = x)"
 then have int: "(LEAST kxz. (x \<leadsto>\<^sup>+ kxz) = z) = (LEAST kxz. (x \<leadsto> kxz) = z) + (LEAST kxx. x \<leadsto>\<^sup>+kxx = x)"
   apply - by(rule nxt_interpolation, simp)
 obtain kxz where "(x \<leadsto> kxz) = z" using singlescc by blast
 then have xzSuc: "(LEAST kxz. (x \<leadsto> kxz) = z) = Suc (LEAST kxz. (x \<leadsto> (Suc kxz)) = z)"
   apply - by(rule Least_Suc, auto simp add: `z \<noteq> x`)
 show "False" using int by(simp add: xzSuc)
next 
 assume "z \<noteq> x" 
 have nxt_l_elem_collect: "\<And> x z k . \<lbrakk>x \<leadsto>\<^sup>+ k = z; z \<noteq> x\<rbrakk> \<Longrightarrow> z \<in> collect (x \<leadsto> k) x"
  by (metis collect.elims insertCI)
 show "z \<in> collect ((nxt ^^ (LEAST k. x \<leadsto>\<^sup>+ k = z)) x) x"
   by(rule nxt_l_elem_collect, auto simp add: `z \<noteq> x`)
qed

end

locale addressedRingnetwork = ringnetwork +
  fixes addr :: "'a \<Rightarrow> 'ADDR::countable"
  assumes addr_inj: "inj addr"
begin

lemma addr_simp[simp]: "addr x = addr y \<longleftrightarrow> x = y" using addr_inj by (auto dest: injD)
end


subsection \<open>Example instantiation of ringnetwork\<close>
(******************************************************************************)

text\<open>To check that the assumptions of the locale are satisfiable, we give a very simple
instantiation.\<close>

datatype example012 = Zero | One | Two 
instance example012 :: countable
  apply standard 
  apply(rule exI[of _ "(\<lambda>x . (case x of Zero \<Rightarrow> 0::nat | One \<Rightarrow> 1 | Two \<Rightarrow> 2))"])
  apply(auto simp add: inj_def) subgoal for x y apply(cases x) by (cases y, auto)+ 
  done
context
begin

abbreviation nxt\<^sub>E :: "example012 \<Rightarrow> example012" where "nxt\<^sub>E \<equiv> 
  (\<lambda>x . case x of Zero \<Rightarrow> One | One \<Rightarrow> Two | Two \<Rightarrow> Zero)"

definition less\<^sub>E where
  "less\<^sub>E x y \<equiv> (x = Zero \<and> (y = One \<or> y = Two)) \<or>
                      x = One \<and> y = Two"
definition less_eq\<^sub>E where
  "less_eq\<^sub>E x y \<equiv> less\<^sub>E x y \<or> x = y"

definition top\<^sub>E where
  "top\<^sub>E \<equiv> Two"

lemmas exI123 = exI[of _ 0] exI[of _ 1] exI[of _ 2]

interpretation ord: ordering_top less_eq\<^sub>E less\<^sub>E top\<^sub>E
apply unfold_locales
by (auto simp add: less\<^sub>E_def less_eq\<^sub>E_def top\<^sub>E_def intro: example012.exhaust)

interpretation addressedRingnetwork less\<^sub>E top\<^sub>E less_eq\<^sub>E nxt\<^sub>E id
proof unfold_locales
  have "{Zero, One, Two} = (UNIV::example012 set)" 
    using example012.exhaust by blast
next
  fix x y
  show "\<exists>k. (nxt\<^sub>E ^^ k) x = y"
    by (cases x; cases y; auto simp add: numeral_eq_Suc intro: exI123)
qed auto

end 

end
