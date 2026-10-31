From iris Require Import invariants.
From iris.algebra Require Import gmap frac agree frac_auth.
From iris.base_logic Require Export gen_heap.
From iris.base_logic.lib Require Export own saved_prop.
From iris.program_logic Require Export weakestpre.
From iris.proofmode Require Import tactics.
From stdpp Require Import base.
From aneris Require Import lang lifting tactics proofmode notation adequacy network.
From iris.proofmode Require Import coq_tactics.
From iris.proofmode Require Export tactics.


Section code.

  Definition assert : ground_lang.val :=
  λ: "v", if: "v" #() then #() else #0 #0. (* #0 #0 is unsafe *)
(* just below ;; *)

Definition unSOME : ground_lang.val :=
  λ: "p",
     match: "p" with NONE => assert #false | SOME "p'" => "p'" end.

Definition listen : ground_lang.val :=
  (
    rec: "loop" "socket" "handle" :=
      match: ReceiveFrom "socket" with
        SOME "m" => let: "snd" := (Snd "m") in
                    "handle" (Fst "m") "snd"
      | NONE => "loop" "socket" "handle"
      end
  )%V.

Definition listen_wait : ground_lang.val :=
  (
    rec: "loop" "socket" :=
      match: ReceiveFrom "socket" with
        SOME "m" => "m"
      | NONE => "loop" "socket"
      end
  )%V.

Definition tag_of_message : ground_lang.val :=
  (
    λ: "msg",
    match: FindFrom "msg" #(0 : Z) #"_" with
      SOME "i" => Substring "msg" #0 "i"
    | NONE => #"UNDEFINED"
    end
  )%V.

Definition value_of_message : ground_lang.val :=
  (
    λ: "msg",
    match: FindFrom "msg" #(0 : Z) #"_" with
      SOME "i" => let: "length" := UnOp StringLength "msg" in
                  let: "start" := "i" + #1 in
                  Substring "msg" "start" ("length" - "start")
    | NONE => #"UNDEFINED"
    end
  )%V.

End code.

Notation "'assert:' e" := (assert (λ: <>, e))%E (at level 99) : expr_scope.

Set Default Proof Using "Type".

Import Network.
Import String.
Import uPred.

Section strings.

  Lemma append_nil_l s :
    "" +:+ s = s.
  Proof. done. Qed.

  Lemma append_cons s1 :
    ∀ s2 a, String a (s1 +:+ s2) = (String a s1) +:+ s2.
  Proof.
    induction s1; intros.
    - by rewrite append_nil_l.
    - rewrite -IHs1. done.
  Qed.

  Lemma append_assoc s1 s2 s3 :
    s1 +:+ s2 +:+ s3 = (s1 +:+ s2) +:+ s3.
  Proof.
    induction s1. Unset Printing Notations.
    - by rewrite !append_nil_l.
    - by rewrite -append_cons IHs1 append_cons.
  Qed.

  Lemma length_Sn a s :
    length (String a s) = S (length s).
  Proof. by cbn. Qed.

  Lemma length_app s1 :
    ∀ s2, length (s1 +:+ s2) = (length s1 + length s2)%nat.
  Proof.
   induction s1; intros.
    - by rewrite append_nil_l.
    - by rewrite -append_cons !length_Sn IHs1.
  Qed.

  Lemma prefix_empty_true s :
    prefix "" s = true.
  Proof. destruct s; cbn; auto. Qed.

  Lemma index_0_empty s :
    index 0 "" s = Some 0%nat.
  Proof. destruct s; by cbn. Qed.

  Lemma index_prefix_true s s' :
    index 0 s s' = Some 0%nat →
    prefix s s' = true.
  Proof.
    destruct s,s'; simpl; cbn; auto.
    - intro; inversion H.
    - intro; destruct ascii_dec.
      + destruct (prefix s s'); auto; destruct (index 0 _ s'); inversion H.
      + destruct (index 0 _ s'); inversion H.
  Qed.

  Lemma index_cons_0_eq a s s' :
    index 0 s s' = Some 0%nat → index 0 (String a s) (String a s') = Some 0%nat.
  Proof.
    intros Hindex.
    cbn. destruct ascii_dec.
    - assert (Hprefix: prefix s s' = true).
      { by apply index_prefix_true. }
        by rewrite Hprefix.
    - by destruct n.
  Qed.

  Lemma index_append_here s t :
    index 0 s (s +:+ t) = Some 0%nat.
  Proof.
    induction s.
    - apply index_0_empty.
    - apply index_cons_0_eq.
      apply IHs.
  Qed.

  Lemma index_0_append_char a t v s :
    s = String a "" →
    index 0 s t = None →
    index 0 s (t +:+ s +:+ v) = Some (length t).
  Proof.
    induction t; intros.
    - rewrite append_nil_l. apply index_append_here.
    - rewrite H. rewrite -append_cons. cbn.
      destruct ascii_dec; subst. cbn in H0. destruct ascii_dec.
      rewrite prefix_empty_true in H0. inversion H0.
      by destruct n.
      rewrite IHt; auto. cbn in H0. destruct ascii_dec. by destruct n.
      destruct index; auto. inversion H0.
  Qed.

  Lemma substring_0_length s :
    substring 0 (length s) s = s.
  Proof. induction s; simpl; auto. by rewrite IHs. Qed.

  Lemma substring_Sn a n m s :
    substring (S n) m (String a s) = substring n m s.
  Proof. induction s; destruct n,m; simpl; auto. Qed.

  Lemma substring_add_length_app n m s1 :
    ∀ s2, substring (length s1 + n) m (s1 +:+ s2) = substring n m s2.
  Proof. induction s1; destruct n,m; simpl; auto. Qed.

  Lemma substring_0_length_append s1 s2 :
    substring 0 (length s1) (s1 +:+ s2) = s1.
  Proof. apply prefix_correct, index_prefix_true, index_append_here. Qed.

  Lemma substring_length_append s1 :
    ∀ s2, substring (length s1) (length s2) (s1 +:+ s2) = s2.
  Proof.
    induction s1; intros s2.
    - rewrite append_nil_l. apply substring_0_length.
    - rewrite length_Sn substring_Sn. apply IHs1.
  Qed.

End strings.

Section library.
Context `{cG : ccounterG Σ}
        `{dG : distG Σ}
        `{siG : socketInterpG Σ}.

Ltac wp_value_head :=
  first [eapply tac_wp_value (* || eapply tac_twp_value *)].

Lemma wp_assert n E (Φ : val → iProp Σ) e :
  WP ⟨n;e⟩ @ E {{ v, ⌜v = 〈n;#true〉⌝ ∧ ▷ Φ 〈n;#()〉 }} -∗ WP ⟨n;assert: e⟩ @ E {{ Φ }}.
Proof.
  iIntros "HΦ". rewrite /assert /=. wp_pures.
  wp_apply (wp_wand with "HΦ").
  iIntros (v) "[% H]"; subst. by wp_if.
Qed.

Lemma unSOME_spec n v v' :
  {{{ ⌜v = SOMEV v'⌝ }}}
    ⟨n;unSOME (Val v)⟩
  {{{ RET 〈n;v'〉; True }}}.
Proof.
  iIntros (Φ ->) "HΦ".
  wp_lam. wp_match. by iApply "HΦ".
Qed.

Lemma listen_spec P Q n h s a rm handler φ :
  saddress s = Some a →
  (∀ m i,
   {{{ P ∗ h s↦[n] s
         ∗ a r↦{½} (<[i:=m]>rm)
         ∗ i m↦{¾} m
         ∗ ⌜received_message_info a m⌝
         ∗ φ (message_stable_from_message m) }}}
      ⟨n; (Val handler) #(m_body m) #(m_sender m)⟩
   {{{ v, RET 〈n;v〉; Q v }}}) -∗
  {{{ P ∗ h s↦[n] s ∗ a r↦{½} rm ∗ a ⤇ φ }}}
     ⟨ n; listen #(LitSocket h) (Val handler) ⟩
  {{{ v, RET 〈n;v〉; Q v }}}.
Proof.
  iIntros (Haddr) "#Hhandler". iLöb as "IH".
  iAlways. iIntros (Φ) "(HP & Hsocket & Hm & #Hsi) HΦ".
  wp_rec. wp_let. wp_bind (ReceiveFrom _).
  wp_apply (wp_receive_from_2 with "[$]"); first done.
  iIntros (r). iDestruct 1 as "(Hsocket & [H | [-> Ha]])"; simpl.
  - iDestruct "H" as (m j) "(% & Hrm' & Hj & Hφ) /=". simplify_eq.
    wp_pures. wp_apply ("Hhandler" with "[-HΦ] [HΦ]"); by iFrame.
  - wp_match. iApply ("IH" with "[-HΦ]"); by iFrame.
Qed.

Lemma listen_wait_spec n h s a rm  φ :
  saddress s = Some a →
  {{{ h s↦[n] s ∗ a r↦{½} rm ∗ a ⤇ φ}}}
     ⟨ n; listen_wait #(LitSocket h) ⟩
  {{{ m i, RET 〈n;(#(m_body m), #(m_sender m))〉;
      h s↦[n] s
        ∗ a r↦{½} (<[i:=m]>rm)
        ∗ i m↦{¾} m
        ∗ ⌜received_message_info a m⌝
        ∗ φ (message_stable_from_message m)
  }}}.
Proof.
  iIntros (Haddr Φ) "(Hs & Hrec & #Hφ) HΦ".
  iLöb as "IH". wp_rec.
  wp_apply (wp_receive_from_2 with "[$Hs $Hrec $Hφ]"); first done.
  iIntros (r). iDestruct 1 as "(Hsocket & [H | [-> Ha]])"; simpl.
  - iDestruct "H" as (m j) "(% & Hrm' & Hj & Hsipred) /=". simplify_eq.
    wp_match. iApply "HΦ". by iFrame.
  - wp_match. by iApply ("IH" with "Hsocket Ha HΦ").
Qed.

Definition valid_tag t := index 0 "_" t = None.

Lemma tag_of_message_spec n (s : string) t v:
  valid_tag t →
  {{{ ⌜s = t +:+ "_" +:+ v⌝ }}}
    ⟨n;tag_of_message #s⟩
  {{{ v, RET 〈n;#v〉; ⌜v = t⌝ }}}.
Proof.
  iIntros (Htag Φ HP) "HΦ". rewrite /tag_of_message.
  wp_pures. wp_find_from.
  { by instantiate (1:=0%nat). }
  rewrite HP (index_0_append_char "_") /=; auto.
  wp_match. wp_substring.
  { repeat split; eauto. by instantiate (1:=0%nat). }
  rewrite substring_0_length_append. by iApply "HΦ".
Qed.

Lemma value_of_message_spec n (s : string) t v :
  valid_tag t →
  {{{ ⌜s = t +:+ "_" +:+ v⌝ }}}
    ⟨n;value_of_message #s⟩
  {{{ r, RET 〈n;#r〉; ⌜r = v⌝ }}}.
Proof.
  iIntros (Htag Φ HP) "HΦ". rewrite /value_of_message.
  wp_pures. wp_find_from.
  { by instantiate (1:=0%nat). }
  rewrite HP (index_0_append_char "_") /=; auto.  wp_match.
  wp_pures.
  wp_substring.
  { repeat split; eauto.
    instantiate (1:=(length t + 1)%nat). apply Nat2Z.inj_add.
    instantiate (1:=(length v)%nat).
    rewrite !length_app plus_assoc length_Sn /= !Nat2Z.inj_add. ring. }
  rewrite substring_add_length_app substring_Sn substring_0_length.
  by iApply "HΦ".
Qed.

End library.
