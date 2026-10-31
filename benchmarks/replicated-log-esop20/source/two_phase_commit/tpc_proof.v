From iris Require Import invariants.
From iris.algebra Require Import gmap frac agree frac_auth.
From iris.base_logic Require Export gen_heap fancy_updates.
From iris.base_logic.lib Require Export own saved_prop viewshifts.
From iris.program_logic Require Export weakestpre.
From iris.proofmode Require Import coq_tactics.
From iris.proofmode Require Export tactics.
From stdpp Require Import base.
From aneris Require Import tactics proofmode notation adequacy.
From aneris.examples.library Require Import frac_auth.
From aneris.examples.two_phase_commit Require Import tpc.

Set Default Proof Using "Type".

Import Network.
Import uPred.

(* TODO, this could be unified? *)
Definition list_sa_val (l : list socket_address) :=
  map (λ (a : socket_address), #a) l.

Definition list_m_val (l : list message_body) :=
  map (λ (x : message_body), #x) l.

Lemma ms_sender_message m a :
  ms_sender (message_stable_from_message m) = a -> m_sender m = a.
Proof. eauto. Qed.

Lemma ms_body_message m s :
  ms_body (message_stable_from_message m) = s -> m_body m = s.
Proof. eauto. Qed.

Lemma submseteq_remove_l (x : socket_address) l l' :
  x ∉ l -> l ⊆+ x :: l' -> l ⊆+ l'.
Proof.
  intros H1.
  rewrite submseteq_cons_r. intros [H2 | [k [H2 _]]]; eauto.
  revert H1. rewrite H2.
  destruct 1. apply elem_of_list_here.
Qed.

Lemma submseteq_extend_l (x : socket_address) l l' :
  x ∈ l' -> x ∉ l ->
  l ⊆+ l' -> x :: l ⊆+ l'.
Proof.
  intros H1.
  apply elem_of_Permutation in H1.
  destruct H1 as [k H1]. rewrite H1. intros H2 H3.
  apply submseteq_skip. apply (submseteq_remove_l x); eauto.
Qed.

Inductive coordinator_state :=
| CS_INIT
| CS_WAIT
| CS_COMMIT
| CS_ABORT.

Inductive participant_state :=
| PS_INIT (prev : participant_state)
| PS_READY
| PS_COMMIT
| PS_ABORT.

Class tpcG Σ := MTpcG {
  tpc_nodesG :> inG Σ (agreeR (leibnizO (list socket_address)));
  tpc_coordinator_stateG :> gen_heapG socket_address (nat * coordinator_state) Σ;
  tpc_participant_stateG :> gen_heapG socket_address (nat * participant_state) Σ;
  tpc_nodes_name : gname;
                  }.

Class tpcPreG Σ := TpcPreG {
  tpc_nodes_PreG :> inG Σ (agreeR (leibnizO (list socket_address)));
  tpc_coord_preG :> gen_heapPreG socket_address (nat * coordinator_state) Σ;
  tpc_part_preG :> gen_heapPreG socket_address (nat * participant_state) Σ;
                   }.

Definition tpcΣ : gFunctors :=
  #[GFunctor (agreeR (leibnizO (list socket_address)));
    gen_heapΣ socket_address (nat * coordinator_state);
    gen_heapΣ socket_address (nat * participant_state)].

Global Instance subG_inG_tpcΣ {Σ} :
  subG tpcΣ Σ → tpcPreG Σ.
Proof. constructor; solve_inG. Qed.

Lemma make_tpcG `{tpcPreG} (parts : leibnizO (list socket_address)) :
  (|==> ∃ _ : tpcG Σ,
        own (A:=agreeR (leibnizO (list socket_address)))
            tpc_nodes_name (to_agree parts) ∗
        gen_heap_ctx (L:=socket_address) (V:=nat * coordinator_state) ∅ ∗
        gen_heap_ctx (L:=socket_address) (V:=nat * participant_state) ∅)%I.
Proof.
  iStartProof.
  iMod (own_alloc (to_agree parts)) as (γ) "H"; first done.
  iMod (gen_heap_init (L:=socket_address) (V:=nat * coordinator_state) ∅) as (γc) "Hc".
  iMod (gen_heap_init (L:=socket_address) (V:=nat * participant_state) ∅) as (γp) "Hp".
  iExists {|
      tpc_coordinator_stateG := γc;
      tpc_participant_stateG := γp;
      tpc_nodes_name := γ
    |}. by iFrame.
Qed.

(** Points-to locations on nodes *)
Notation "a ↦c{ q } s" := (mapsto (L:=socket_address)
                                  (V:=nat * coordinator_state) a q s)
    (at level 20, q at level 50, format "a  ↦c{ q }  s") : uPred_scope.
Notation "a ↦c s" := (mapsto (L:=socket_address)
                             (V:=nat * coordinator_state) a 1 s)
    (at level 20) : uPred_scope.
Notation "a ↦p{ q } s" := (mapsto (L:=socket_address)
                                  (V:=nat * participant_state) a q s)
    (at level 20, q at level 50, format "a  ↦p{ q }  s") : uPred_scope.
Notation "a ↦p s" := (mapsto (L:=socket_address)
                             (V:=nat * participant_state) a 1 s)
    (at level 20) : uPred_scope.

Class TpcProt Σ :=
  {
    is_req : message_body -> nat → Prop;
    is_vote : message_body -> nat → Prop;
    is_abort : message_body -> nat → Prop;
    is_abort_dec : forall m r, Decision (is_abort m r);
    is_global : message_body -> nat -> Prop;
    P : message_body -> socket_address → iProp Σ;
    Q : socket_address → nat → iProp Σ;
    tpc_inv_cs_name : namespace;
    tpc_inv_ps_name : namespace;
  }.

Class TpcPartProt Σ :={
                    R : socket_address -> iProp Σ;
                    R' : socket_address -> iProp Σ;
                      }.

Definition ownA `{tpcG Σ} (l : list socket_address) :=
  own (A:=agreeR (leibnizO (list socket_address)))
      tpc_nodes_name (to_agree l).

Section tpc_participant_protocol.
  Context `{tpc : TpcProt Σ}.
  Context `{tG : tpcG Σ}.
  Context `{dG : distG Σ}.
  Context `{siG : socketInterpG Σ}.

  Definition is_abort_m (x : message * nat) := is_abort (m_body (x.1)) (x.2).
  Global Instance is_abort_m_dec : ∀ m, Decision (is_abort_m m).
  Proof. intros []. rewrite /is_abort_m /=. apply is_abort_dec. Qed.


  Definition gen_heap_ctxC := gen_heap_ctx (L:=socket_address)
                                           (V:=nat * coordinator_state).
  Definition gen_heap_ctxP := gen_heap_ctx (L:=socket_address)
                                           (V:=nat * participant_state).

  Definition tpc_inv_cs a :=
    (∃ l σ, ⌜dom (gset socket_address) σ = list_to_set (a::l)⌝ ∗
            ⌜∀ (p : socket_address) v, σ !! p = Some v -> Some v = σ !! a⌝ ∗
            ownA l ∗ gen_heap_ctxC σ)%I.
  Definition tpc_inv_cs_I a := inv tpc_inv_cs_name (tpc_inv_cs a).

  Definition tpc_inv_ps := (∃ σ, gen_heap_ctxP σ)%I.
  Definition tpc_inv_ps_I := inv tpc_inv_ps_name (tpc_inv_ps).

  Lemma ownA_agree (l l' : list socket_address) :
    (ownA l ∗ ownA l' -∗ ⌜l = l'⌝)%I.
  Proof.
    rewrite -own_op own_valid. iPureIntro.
    apply (@agree_op_invL' (leibnizO (list socket_address))).
    typeclasses eauto.
  Qed.

  Lemma split_votes l r :
    ([∗ list] m ∈ l, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) -∗
    ([∗ list] m ∈ l, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
    ([∗ list] m ∈ l, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m).
  Proof.
    iIntros "H".
    rewrite -big_sepL_sep.
    iApply (big_sepL_mono with "H").
    iIntros (k y Hlookup) "H".
    iDestruct "H" as (mId π Hvote) "[H1 H2]".
    iSplitL "H1"; iExists mId,(π/2)%Qp; iFrame; eauto.
  Qed.

  Lemma p_not_in_list p l x :
    (p ↦c x -∗ ([∗ list] p0 ∈ l, p0 ↦c x) -∗ ⌜p ∉ l⌝)%I.
  Proof.
    destruct (decide (p ∈ l)); last by iIntros.
    iIntros "Hp Hp'".
    rewrite big_sepL_elem_of; last done.
    iCombine "Hp" "Hp'" as "Hp". rewrite mapsto_eq /mapsto_def.
    iDestruct (own_valid with "Hp") as %Hvalid.
      by apply singleton_valid in Hvalid as [[] ?].
  Qed.

  Lemma coordinator_state_update_all' σ d l v v' :
    dom (gset socket_address) σ = d ->
    gen_heap_ctxC σ -∗ ([∗ list] p ∈ l, p ↦c v) -∗
    |==> ∃ σ', ⌜dom (gset socket_address) σ' = d⌝ ∗
               ⌜∀ p, p ∈ l -> σ' !! p = Some v'⌝ ∗
               gen_heap_ctxC σ' ∗ [∗ list] p ∈ l, (p ↦c v').
  Proof.
    iIntros (Hdom) "Hctx Hlist".
    iInduction l as [|y l] "IH".
    - simpl. iExists σ. iSplitR; eauto. iFrame. iPureIntro. intros. inversion H.
    - iDestruct "Hlist" as "[H Hl]".
      iDestruct ("IH" with "Hctx Hl") as ">Hup".
      iDestruct "Hup" as (σ') "(% & % & Hctx & Hl)".
      iDestruct (gen_heap_valid (L:=socket_address) σ'
                   with "Hctx H") as %Hlookup.
      iDestruct (gen_heap_update (L:=socket_address) σ' y _ v' with "Hctx H")
        as ">[Hctx H]".
      iDestruct (gen_heap_valid (L:=socket_address) (<[y:=v']> σ')
                   with "Hctx H") as %Hlookup'.
      iFrame. iExists (<[y:=v']> σ'). iFrame. iSplitR.
      rewrite (dom_insert_Some (D:=gset socket_address) σ' _ v); eauto.
      iPureIntro. intros. destruct (decide (p = y)); subst; first done.
      rewrite lookup_insert_ne; eauto. apply H0.
      revert H1. rewrite elem_of_cons. destruct 1 as [Hy | Hp]; last done.
        by destruct n.
  Qed.

  Lemma coordinator_state_update_all a v v' l :
    (tpc_inv_cs_I a -∗ ownA l -∗ a ↦c v -∗ ([∗ list] p∈l, p ↦c v) -∗
    |={⊤}=> a ↦c v' ∗ [∗ list] p∈l, p ↦c v' )%I.
  Proof.
    iIntros "#Hinv #Hparts Hc Hc'".
    iInv tpc_inv_cs_name as ">Hi" "HClose".
    iDestruct "Hi" as (l' σ Hdom Hmap) "(#Hparts' & Hctx)".
    iDestruct (ownA_agree l l' with "[$Hparts $Hparts']") as %Heq; subst.
    iDestruct (coordinator_state_update_all' σ _ _ _ v'
                 with "Hctx Hc'")
      as ">HX"; eauto.
    iDestruct "HX" as (σ' Hall Hmap') "[Hctx Hpts]". iFrame.
    iDestruct (gen_heap_valid (L:=socket_address) σ'
                 with "Hctx Hc") as %Hlookup'.
    iDestruct (gen_heap_update (L:=socket_address) _ _ _ v' with "Hctx Hc")
      as ">[Hctx Hc]". iFrame.
    iMod ("HClose" with "[Hctx]") as "_".
    iNext. iExists l',(<[a:=v']> σ'); iFrame; iFrame "#". iSplitR.
    rewrite (dom_insert_Some (D:=gset socket_address) σ' _ v); eauto.
    iPureIntro. intros. rewrite lookup_insert. destruct (decide (a = p)); subst.
    - by rewrite lookup_insert in H.
    - rewrite lookup_insert_ne in H; last exact n. rewrite -H.
      apply Hmap'.
      apply (elem_of_dom_2 (D:=gset socket_address)) in H. revert H.
      rewrite Hall list_to_set_cons elem_of_union elem_of_singleton elem_of_list_to_set.
      intros [Hfalse | Hlist]; eauto. by destruct n.
    - iModIntro. eauto.
  Qed.

  Lemma coordinator_state_agree a p v v' :
    tpc_inv_cs_I a -∗ a ↦c v -∗ p ↦c v' -∗ |={⊤}=> ⌜v = v'⌝ ∗ a ↦c v ∗ p ↦c v'.
  Proof.
    iIntros "Hinv Hc Hc'".
    iInv tpc_inv_cs_name as ">Hi" "HClose".
    iDestruct "Hi" as (l' σ Hdom Hmap) "(#Hparts' & Hctx)".
    iDestruct (@gen_heap_valid with "Hctx Hc") as %Hcin1.
    iDestruct (@gen_heap_valid with "Hctx Hc'") as %Hcin2.
    assert (Heq: v = v').
    { specialize (Hmap p v' Hcin2); rewrite Hcin1 in Hmap. inversion Hmap. eauto. }
    iMod ("HClose" with "[Hctx]") as "_".
    { iExists _; by iFrame;iFrame "#";auto. }
    iModIntro. iFrame. eauto.
  Qed.

  Definition tpc_coordinator_vote_si :=
    (λ msg, ∃ r pl sp,
        ⌜ms_sender msg ∈ pl⌝ ∗ ⌜is_vote (ms_body msg) r⌝ ∗
        ownA pl ∗ ms_sender msg ↦c (r, CS_WAIT) ∗ ms_sender msg ↦p{¾} (r, sp) ∗
        (⌜is_abort (ms_body msg) r ∧ sp = PS_ABORT⌝ ∨
         ⌜¬is_abort (ms_body msg) r ∧ sp = PS_READY⌝)
    )%I.

  Definition tpc_coordinator_ack_si :=
    (λ msg, ∃ m' cs pl prev r,
        ⌜ms_sender msg ∈ pl⌝ ∗
        ownA pl ∗ ms_sender msg ↦c (r,cs) ∗ ms_sender msg ↦p{¾} (r, PS_INIT prev) ∗
        (⌜cs = CS_COMMIT ∧ prev = PS_COMMIT⌝ ∗ Q (ms_sender msg) r ∨
         ⌜cs = CS_ABORT ∧ prev = PS_ABORT⌝ ∗ P m' (ms_sender msg)))%I.

  Definition tpc_coordinator_si :=
    (λ msg, tpc_coordinator_vote_si msg ∨ tpc_coordinator_ack_si msg)%I.

  Definition tpc_participant_req_si p :=
    (λ msg, ∃ l r ps,
        ⌜p ∈ l⌝ ∗ ⌜is_req (ms_body msg) (S r)⌝ ∗
        ownA l ∗ ms_sender msg ⤇ tpc_coordinator_si ∗
        p ↦c (S r, CS_WAIT) ∗ p ↦p{¾} (r, PS_INIT ps) ∗ P (ms_body msg) p
    )%I.

  Definition participants_coh (l : list message) (pl : list socket_address) :=
    List.map (λ m, m_sender m) l ≡ₚ pl.

  Definition tpc_participant_global_si p :=
    (λ msg, ∃ ga l pl r sc sp,
        ⌜participants_coh l pl⌝ ∗ ⌜is_global (ms_body msg) r⌝ ∗
        ⌜sp = PS_READY ∨ sp = PS_ABORT⌝ ∗
        ⌜ga = filter (λ m, is_abort_m (m,r)) l⌝ ∗
        ms_sender msg ⤇ tpc_coordinator_si ∗ ownA pl ∗ p ↦c (r,sc) ∗
        p ↦p{¾} (r, sp) ∗
        ([∗ list] m ∈ l, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
        (⌜ga = [] ∧ ¬is_abort (ms_body msg) r ∧ sc = CS_COMMIT⌝ ∨
         ⌜ga ≠ [] ∧ is_abort (ms_body msg) r ∧ sc = CS_ABORT⌝)
    )%I.

  Definition tpc_participant_si participant :=
    (λ msg, tpc_participant_req_si participant msg ∨
            tpc_participant_global_si participant msg)%I.

  Definition req_handler_spec `{TpcPartProt Σ} n (handlerV : ground_lang.val) p :=
    (∀ (c : socket_address) m r  s,
    {{{ tpc_inv_ps_I ∗ ⌜is_req (m_body m) (S r)⌝ ∗ p ↦p (r, PS_INIT s) ∗
        P (m_body m) p ∗ R p }}}
      ⟨n;handlerV #(m_body m) #c⟩
    {{{v, RET 〈n;#v〉; ∃ ps, ⌜is_vote v (S r)⌝ ∗ p ↦p (S r, ps) ∗ R' p ∗
   (⌜is_abort v (S r) ∧ ps = PS_ABORT⌝ ∨ ⌜¬is_abort v (S r) ∧ ps = PS_READY⌝) }}})%I.

  Definition fin_handler_spec `{TpcPartProt Σ} n (handlerV : ground_lang.val) p :=
    (∀ (c : socket_address) m r s,
    {{{ tpc_inv_ps_I ∗ ⌜is_global (m_body m) r⌝ ∗
        ⌜(s = PS_READY ∨ s = PS_ABORT)⌝ ∗ p ↦p (r, s) ∗ R' p }}}
      ⟨n;handlerV #(m_body m) #c⟩
    {{{RET 〈n;#()〉; ∃ s' m', p ↦p (r, PS_INIT s') ∗ R p ∗
                   (⌜is_abort (m_body m) r ∧ s' = PS_ABORT⌝ ∗ P m' p ∨
                   ⌜¬is_abort (m_body m) r ∧ s' = PS_COMMIT⌝ ∗ Q p r)
    }}})%I.

  Definition dec_handler_spec n handler a :=
    (∀ v l l' r,
    {{{ ⌜list_coh (list_m_val (map (λ m, m_body m) l)) v⌝ ∗
         tpc_inv_cs_I a ∗ ownA l' ∗ IsNode n ∗ a ↦c (r, CS_WAIT) ∗
         ([∗ list] m∈l, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
         [∗ list] p ∈ l', p ↦c (r, CS_WAIT)
    }}}
      ⟨n;handler (ground_lang.of_val v)⟩
    {{{ v' ga st, RET 〈n;#(LitString v')〉; ⌜is_global v' r⌝ ∗
        ⌜ga = filter (λ m, is_abort_m (m,r)) l⌝ ∗
        (⌜ga = [] ∧ ¬is_abort v' r ∧ st = CS_COMMIT⌝ ∨
         ⌜ga ≠ [] ∧ is_abort v' r ∧ st = CS_ABORT⌝) ∗
        a ↦c (r, st) ∗ [∗ list] p ∈ l', p ↦c (r, st) }}})%I.

  Definition tpc_listen_vote_Q n l msgs h s a r :=
     (∃ l' x rm',
        ⌜map (λ m, m_sender m) l' ≡ₚ l⌝ ∗
        ⌜list_coh (list_m_val (map (λ m, m_body m) l')) x⌝ ∗
        msgs ↦[n] x ∗ h s↦[n] s ∗ a r↦{½} rm' ∗ a ↦c (r, CS_WAIT) ∗
        ([∗ list] p ∈ l, p ↦c (r, CS_WAIT)) ∗
        ([∗ list] m ∈ l', ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
        ([∗ list] p∈l, ∃ mId m sp π, ⌜m_sender m = p⌝ ∗
                       ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m ∗ p ↦p{¾} (r, sp) ∗
                       (⌜is_abort (m_body m) r ∧ sp = PS_ABORT⌝ ∨
                        ⌜¬is_abort (m_body m) r ∧ sp = PS_READY⌝)))%I.

  Lemma tpc_listen_vote n l lrec lrecV l'' (msgs : loc) (h : socket_handle) s a rm r :
    saddress s = Some a →
    {{{ ⌜list_coh (list_m_val (map (λ m, m_body m) lrec)) lrecV⌝ ∗ ⌜l'' ⊆+ l⌝ ∗
        ⌜map (λ m, m_sender m) lrec ≡ₚ l''⌝ ∗
        tpc_inv_cs_I a ∗ a ⤇ tpc_coordinator_si ∗ ownA l ∗ msgs ↦[n] lrecV ∗
        h s↦[n] s ∗ a r↦{½} rm ∗ a ↦c (r, CS_WAIT) ∗
        ([∗ list] m ∈ lrec, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
        ([∗ list] p∈l'', p ↦c (r, CS_WAIT)) ∗
        ([∗ list] p∈l'', ∃ mId m sp π, ⌜m_sender m = p⌝ ∗
                        ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m ∗ p ↦p{¾} (r, sp) ∗
        (⌜is_abort (m_body m) r ∧ sp = PS_ABORT⌝ ∨
         ⌜¬is_abort (m_body m) r ∧ sp = PS_READY⌝)) }}}
      ⟨n;listen #(LitSocket h) (
        rec: "handler" "msg" "sender" :=
          let: "msgs'" := !#msgs in
          #msgs <- list_cons "msg" "msgs'";;
          if: list_length !#msgs = #(length (list_sa_val l))
          then #() else listen #(LitSocket h) "handler")%V⟩
    {{{ RET 〈n;#()〉; tpc_listen_vote_Q n l msgs h s a r }}}.
  Proof.
    iIntros (Haddress Φ) "H HΦ".
    iDestruct "H" as (Hlistcoh Hsub Hsub')
        "(#Hinv & #Hsi & #Hparts & Hmsgs & Hs & Hrec & Hc & Hcert & Hcs & Hvotes)".
    wp_apply (listen_spec
                (▷(tpc_listen_vote_Q n l msgs h s a r -∗ Φ 〈 n; #() 〉) ∗
                 msgs ↦[ n] lrecV ∗ a ↦c (r, CS_WAIT) ∗
                 ([∗ list] m ∈ lrec, ∃ mId π, ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m) ∗
                 ([∗ list] p ∈ l'', p ↦c (r, CS_WAIT)) ∗
                 ([∗ list] p∈l'', ∃ mId m sp π, ⌜m_sender m = p⌝ ∗
                 ⌜is_vote (m_body m) r⌝ ∗  mId m↦{π} m ∗ p ↦p{¾} (r, sp) ∗
        (⌜is_abort (m_body m) r ∧ sp = PS_ABORT⌝ ∨
         ⌜¬is_abort (m_body m) r ∧ sp = PS_READY⌝)))%I
                with "[] [-]"); eauto; iFrame; iFrame "#".
    iLöb as "IH" forall (lrecV l'' lrec rm Hlistcoh Hsub Hsub').
    iIntros (m mId Φ') "!# H HΦ'".
    iDestruct "H" as
         "((HΦ & Hmsgs & Hc & Hcerts & Hlist & Hvotes) &
                  Hs & Hrm & HmId & _ & HP)".
    wp_rec. wp_let. wp_load. wp_let.
    wp_apply (list_cons_spec); eauto. iIntros (v' Hlistcoh'). simpl.
    wp_store. wp_seq. wp_load.
    wp_apply (list_length_spec _ _ v'); eauto. iIntros (vl Hvl). simpl.
    iDestruct "HP" as "[Hvote | Hack]"; last first.
    {
      iDestruct "Hack" as (m' cs pl prev r0 Hpin) "(_ & Hc' & _ & HP)".
      iApply fupd_wp.
      iDestruct (coordinator_state_agree with "Hinv Hc Hc'") as ">(Hagree & _)".
      iDestruct "Hagree" as %Hagree.
      iDestruct "HP" as "[([-> _] & _) | ([-> _] & _)]"; inversion Hagree.
    }
    iDestruct "Hvote" as (r0 pl sp Hpinl Hisvote)
                           "(#Hparts' & Hc' & Hp & Hvote)".
    iDestruct "Hvote" as %Hvote.
    iApply fupd_wp.
    iDestruct (coordinator_state_agree with "Hinv Hc Hc'")
      as ">(Hagree & Hc & Hc')". iDestruct "Hagree" as %Hagree.
    inversion Hagree; subst. iModIntro.
    iDestruct (ownA_agree l pl with "[$Hparts $Hparts']") as %->.
    iDestruct (p_not_in_list  with "Hc' Hlist") as %Hnotin.
    iDestruct "HmId" as "[HmId HmId']".
    iDestruct (big_sepL_cons
                 (λ x y, ∃ mId0 π, ⌜is_vote (m_body y) r0⌝ ∗
                                   mId0 m↦{π} y)%I
                 m lrec with "[HmId' $Hcerts]") as "Hcerts".
    { iExists mId,_. iFrame. eauto. }
    iDestruct (big_sepL_cons
                 (λ x y, y ↦c (r0, CS_WAIT))%I
                 _ l'' with "[$Hc' $Hlist]") as "Hlist".
    iDestruct (big_sepL_cons
                 (λ k p0, ∃ (mId1 : message_id) (m1 : message)
                                 (sp0 : participant_state)
                                 (π : Qp),
                                 ⌜m_sender m1 = p0⌝
                                 ∗ ⌜is_vote (m_body m1) r0⌝
                                   ∗ mId1 m↦{π} m1
                                     ∗ p0 ↦p{¾} (r0, sp0)
                                       ∗ (⌜is_abort (m_body m1) r0 ∧ sp0 = PS_ABORT⌝
                                          ∨ ⌜¬ is_abort (m_body m1) r0
                                             ∧ sp0 = PS_READY⌝))%I
                 _ l'' with "[Hp HmId $Hvotes]") as "Hvotes".
    { iExists mId,m,sp,_. iFrame; eauto. }
    wp_op. case_bool_decide; wp_if.
    - iApply "HΦ'". iApply "HΦ".
      assert (Hleq: (ms_sender (message_stable_from_message m)) :: l'' ≡ₚ pl).
      { apply submseteq_Permutation_length_eq.
        apply Permutation_length in Hsub'.
        rewrite !map_length in H.
        rewrite !map_length in Hsub'.
        rewrite cons_length. rewrite -Hsub'. lia.
        apply submseteq_extend_l; eauto.
      }
      rewrite Hleq.
      iExists (m::lrec),_,_. iFrame.
      rewrite !map_cons. iPureIntro; split; eauto.
      by rewrite -Hleq Hsub'
              (ms_sender_message _ (ms_sender (message_stable_from_message m))).
    - wp_apply (listen_spec with "[] [-HΦ' $Hsi]"); first eauto.
      iApply ("IH" $! v' (_::l'') (m::lrec)); eauto; iPureIntro.
      { apply submseteq_extend_l; eauto. }
      { rewrite !map_cons.
        rewrite (ms_sender_message _ (ms_sender (message_stable_from_message m)));
          auto. }
      + iFrame.
      + eauto.
  Qed.

  Definition tpc_listen_ackQ n h s a r st ps l :=
    (∃ rm', h s↦[n] s ∗ a r↦{½} rm' ∗ a ↦c (r,st) ∗
    ([∗ list] p∈l, p ↦c (r, st)) ∗
    ([∗ list] p∈l, p ↦p{¾} (r,PS_INIT ps)) ∗
    ((⌜st = CS_COMMIT ∧ ps = PS_COMMIT⌝ ∗ ([∗ list] p∈l, Q p r)) ∨
     (⌜st = CS_ABORT ∧ ps = PS_ABORT⌝ ∗ ([∗ list] p∈l, ∃ m', P m' p))))%I.

  Lemma tpc_listen_ack n l l' (acks : loc) (h : socket_handle) s a rm r st ps
    (result : string) :
    saddress s = Some a →
    {{{ ⌜l' ⊆+ l⌝ ∗ a ⤇ tpc_coordinator_si ∗ tpc_inv_cs_I a ∗ ownA l ∗
        acks ↦[n] #(length l') ∗ h s↦[n] s ∗ a r↦{½} rm ∗ a ↦c (r, st) ∗
        ([∗ list] p ∈ l', p ↦c (r, st)) ∗
        ([∗ list] p ∈ l', p ↦p{¾} (r,PS_INIT ps)) ∗
        ((⌜st = CS_COMMIT ∧ ps = PS_COMMIT⌝ ∗ ([∗ list] p∈l', Q p r)) ∨
         (⌜st = CS_ABORT ∧ ps = PS_ABORT⌝ ∗ ([∗ list] p∈l', ∃ m', P m' p)))
    }}}
      ⟨n;listen #(LitSocket h) (
        rec: "handler" "msg" "sender" :=
          #acks <- !#acks + #1;;
          if: !#acks = #(length (list_sa_val l))
          then #result else listen #(LitSocket h) "handler")%V⟩
    {{{ RET 〈n;#result〉; tpc_listen_ackQ n h s a r st ps l }}}.
  Proof.
    iIntros (Haddress Φ) "H HΦ".
    iDestruct "H" as (Hsub)
          "(#Hsi & #Hinv & #Hparts & Hacks & Hs & Hrec & Hc & Hcs & Hvotes)".
    wp_apply (listen_spec
                (▷ (tpc_listen_ackQ n h s a r st ps l -∗ Φ 〈 n; #result 〉) ∗
                 acks ↦[ n] #(length l') ∗ a ↦c (r, st) ∗
                 ([∗ list] p ∈ l', p ↦c (r, st)) ∗
                 ([∗ list] p ∈ l', p ↦p{¾} (r,PS_INIT ps)) ∗
                 ((⌜st = CS_COMMIT ∧ ps = PS_COMMIT⌝ ∗ ([∗ list] p∈l', Q p r)) ∨
                 (⌜st = CS_ABORT ∧ ps = PS_ABORT⌝ ∗ ([∗ list] p∈l', ∃ m', P m' p))))%I
                with "[] [-]"); eauto; iFrame; iFrame "#".
    iLöb as "IH" forall (l' rm Hsub).
    iIntros (m mId Φ') "!# H HΦ'".
    iDestruct "H" as
       "((HΦ & Hmsgs & Hc & Hlist & Hacks & HPs) & Hs & Hrec & HmId & _ & HP)".
    wp_rec. wp_let. wp_load. wp_op. wp_store. wp_seq. wp_load.
    iDestruct "HP" as "[Hvote | Hack]".
    { iDestruct "Hvote" as (r0 pl sp)
                             "(? & ? & ? & Hc' & ? & HP)".
      iApply fupd_wp.
      iDestruct (coordinator_state_agree with "Hinv Hc Hc'") as ">(Hagree & _)".
      iDestruct "Hagree" as %Hagree.
      iDestruct "HPs" as "[([% _] & _) | ([% _] & _)]"; subst; inversion Hagree. }
    iDestruct "Hack" as (m'' cs pl prev r0 Hpinpl)
                          "(#Hparts' & Hc' & Hp & HP)".
    iApply fupd_wp.
    iDestruct (coordinator_state_agree with "Hinv Hc Hc'")
      as ">(Hagree & Hc & Hc')".
    iDestruct "Hagree" as %Hagree. inversion Hagree; subst. iModIntro.
    iDestruct (ownA_agree l pl  with "[$Hparts $Hparts']") as %<-.
    iDestruct (p_not_in_list  with "Hc' Hlist") as %Hnotin.
    set (p := ms_sender (message_stable_from_message m)).
    iAssert (([∗ list] p0 ∈ p :: l', p0 ↦c (r0, cs)) ∗
             ([∗ list] p0 ∈ p :: l', p0 ↦p{¾} (r0, PS_INIT ps)) ∗
        (⌜cs = CS_COMMIT ∧ ps = PS_COMMIT⌝ ∗ ([∗ list] p0 ∈ p :: l', Q p0 r0)
       ∨ ⌜cs = CS_ABORT ∧ ps = PS_ABORT⌝ ∗ ([∗ list] p0 ∈ p :: l', ∃ m', P m' p0)))%I
               with "[Hlist Hacks HPs Hc' Hp HP]" as "(Hlist & Hacks & HPs)".
    { iDestruct (big_sepL_cons (λ x y, y ↦c (r0, cs))%I
                   with "[$Hlist $Hc']") as "Hlist".
      iDestruct "HPs" as "[([% %] & HQs) | ([% %] & HPs)]"; subst;
        iDestruct "HP" as "[([% %] & HQ) | ([% %] & HP)]"; subst;
          inversion Hagree; try inversion H.
      - iDestruct (big_sepL_cons (λ x p, p ↦p{¾} (r0, PS_INIT PS_COMMIT))%I
                     with "[$Hacks $Hp]") as "Hacks".
        iDestruct (big_sepL_cons (λ x p, Q p r0)%I
                     with "[$HQs $HQ]") as "HQs". iFrame.
        iLeft. iFrame. eauto.
      - iDestruct (big_sepL_cons (λ x p, p ↦p{¾} (r0, PS_INIT PS_ABORT))%I
                     with "[$Hacks $Hp]") as "Hacks".
        iDestruct (big_sepL_cons (λ x p, ∃ m', P m' p)%I
                     with "[$HPs HP]") as "HPs". eauto.  iFrame.
        iRight. iFrame. eauto.
    }
    wp_op. case_bool_decide; wp_if.
    - iApply "HΦ'". iApply "HΦ". iExists _.
      assert (Heq: p :: l' ≡ₚ l).
      { apply submseteq_Permutation_length_eq.
        rewrite !map_length in H.
        rewrite cons_length. lia.
        apply submseteq_extend_l; eauto.
      }
      rewrite Heq. iFrame.
    - wp_apply (listen_spec with "[] [-HΦ' $Hsi]"); first done.
      iApply ("IH" $! (p::l')); eauto; iPureIntro.
      { apply submseteq_extend_l; eauto. }
      rewrite cons_length Z.add_1_r /= -Zpos_P_of_succ_nat. iFrame.
      iApply "HΦ'".
  Qed.

  Lemma tpc_coordinator_setup_spec n
        (msg : string) (h : socket_handle)
        nodesV (s : socket) (a : socket_address)
        (dec_handlerV : ground_lang.val) l rm r:
    NoDup l →
    List.length l > 0 →
    saddress s = Some a →
    dec_handler_spec n dec_handlerV a -∗
    {{{ ⌜list_coh (list_sa_val l) nodesV⌝ ∗ ⌜is_req msg (S r)⌝ ∗
        tpc_inv_cs_I a ∗ ownA l ∗ a ⤇ tpc_coordinator_si ∗
        ([∗ list] p∈l, p ⤇ tpc_participant_si p) ∗
        IsNode n ∗ h s↦[n] s ∗ a r↦{½} rm ∗ a ↦c (r,CS_INIT) ∗
        ([∗ list] p∈l, P msg p) ∗
        ([∗ list] p∈l, p ↦c (r, CS_INIT)) ∗
        ([∗ list] p∈l, ∃ ps, p ↦p{¾} (r,PS_INIT ps)) }}}
    ⟨n;tpc_coordinate #(LitString msg)
                      #(LitSocket h)
                      nodesV
                      dec_handlerV⟩
    {{{v, RET 〈n;#(LitString v)〉; ∃ ps cs rm', ⌜is_global v (S r)⌝ ∗
         h s↦[n] s ∗ a r↦{½} rm' ∗ a ↦c (S r,cs) ∗
         ([∗ list] p∈l, p ↦c (S r, cs)) ∗
         ([∗ list] p∈l, p ↦p{¾} (S r,PS_INIT ps)) ∗
         ((⌜¬is_abort v (S r) ∧ cs = CS_COMMIT ∧ ps = PS_COMMIT⌝ ∗
             ([∗ list] p∈l, Q p (S r))) ∨
          (⌜is_abort v (S r) ∧ cs = CS_ABORT ∧ ps = PS_ABORT⌝ ∗
             ([∗ list] p∈l, ∃ m', P m' p)))
      }}}.
  Proof.
    iIntros (Hnodup Hllength Haddress).
    iIntros "#HdecH"; iIntros (Φ)"!# H HΦ".
    iDestruct "H" as (Hlist Hisreq)
       "(#Hinv & #Hparts & #Hcsi & #Hpsis & Hn & Hs & Hrec & Hc & Hcs & Hpcs & Hps)".
    wp_lam. do 3 wp_let.
    wp_apply (list_length_spec _ _ nodesV); eauto. iIntros (v Hlength); wp_let.
    wp_apply (list_make_spec); eauto. iIntros (v' Hmsgslist); simpl.
    wp_bind (ref _)%E.
    rewrite /list_coh in Hmsgslist; subst.
    wp_alloc msgs as "Hmsgs"; wp_let.
    wp_alloc ack as "Hack"; wp_let.
    iApply fupd_wp.
    iDestruct (coordinator_state_update_all _ _ (S r, CS_WAIT)
                 with "Hinv Hparts Hc Hpcs") as ">(Hc & Hpcs)". iModIntro.
    iDestruct (big_sepL_sep with "[Hps Hpcs]") as "HPs". iFrame.
    iDestruct (big_sepL_sep with "[HPs Hcs]") as "HPs". iFrame. simpl.
    wp_apply (list_iter_spec _ l _ _
                             (h s↦[n] s)%I _
                             (λ x, True)%I with "[] [$Hs $HPs]"); eauto;
      try (iPureIntro; exact Hlist).
    { iIntros (ca Φ1) "!# Ha HΦ1".
      iDestruct "Ha" as (Hinl) "(Hs & HP & Hc & Hpc)".
      iDestruct "Hpc" as (ps) "Hpc". wp_lam.
      iDestruct (big_sepL_elem_of with "Hpsis") as "Hpsi"; first eauto.
      wp_apply (wp_send_to_bound with "[$Hs Hc HP Hpc $Hpsi]"); eauto.
      {iLeft. iExists _,_,_.
        rewrite /message_stable_from_message /=.
        iFrame; iFrame "#"; eauto. }
      iIntros "[Hs _]". iApply "HΦ1". by iFrame.
    }
    iIntros "(Hs & _)". wp_seq. wp_pures.
    wp_apply (tpc_listen_vote _ _ [] with "[Hmsgs Hs Hrec Hc]"); eauto.
    { iFrame; iFrame "#". simpl. iSplitR; auto; iSplitR; last auto.
      iPureIntro. apply submseteq_nil_l. }
    rewrite /tpc_listen_vote_Q.
    iIntros "H"; simpl.
    iDestruct "H" as (l' x rm' Hall Hlcoh)
                       "(Hmsgs & Hs & Hrec & Hcs & Hpcs & Hcerts & Hvotes)".
    wp_seq. wp_load.
    rewrite /dec_handler_spec.
    iDestruct (split_votes with "Hcerts") as "[Hcerts Hcerts']".
    wp_apply ("HdecH" $! _ l' l (S r) with "[$Hcs $Hpcs Hcerts' Hn]"); eauto.
    iIntros (v' ga st) "Htemp".
    iDestruct "Htemp" as (Hisglobal Hga Hdec) "(Hc & Hpcs)". wp_let.
    iDestruct (big_sepL_sep with "[$Hvotes $Hpcs]") as "HPs".
    wp_apply (list_iter_spec
                _ l _ _
                (h s↦[ n] s ∗
                ([∗ list] m ∈ l', ∃ mId π, ⌜is_vote (m_body m) (S r)⌝ ∗
                                            mId m↦{π} m))%I _
                (λ x, True)%I with "[] [$Hs $HPs $Hcerts]");
      try (iPureIntro; exact Hlist).
    { iIntros (ca Φ1) "!# Ha HΦ1".
      iDestruct "Ha" as (Hinl) "((Hs & Hcerts) & HP & Hc')".
      iDestruct "HP" as (mId m sp π Hsender Hisvote) "(HmId & Hpc & %)". wp_lam.
      iDestruct (big_sepL_elem_of with "Hpsis") as "Hpsi"; first eauto.
      iDestruct (split_votes with "Hcerts") as "[Hcerts Hcerts']".
      wp_apply (wp_send_to_bound with "[$Hs Hc' Hpc Hcerts']"); eauto; iFrame; iFrame "#".
      { iRight. iExists _,_,_,_,_,_.
        iFrame; iFrame "#"; eauto; iPureIntro. rewrite -Hga.
          destruct Hdec as [Hcom | Hab]; intuition.
        }
      iIntros "[Hsh _]". iApply "HΦ1". iFrame. }
    iIntros "[[Hs _] _]". wp_seq.
    destruct Hdec as [[? [? Hst]] | [? [? Hst]]]; eauto;
      wp_apply (tpc_listen_ack _ _ [] with "[Hack Hs Hrec Hc]");
      simpl; eauto; iFrame; iFrame "#".
    - iPureIntro. repeat (split; eauto).
      apply submseteq_nil_l.
    - iIntros "H". iDestruct "H" as (rm'') "(Hsh & Hrec & Hc & Hcs & Hpcs & Hres)".
      iApply "HΦ". iExists _,_,_. iFrame. iSplitR; first done. iLeft.
      iDestruct "Hres" as "[(% & HQ) | (% & HP)]"; eauto. simplify_eq.
      inversion H1. inversion H2.
    - iPureIntro. repeat (split; eauto).
      apply submseteq_nil_l.
    - iIntros "H". iDestruct "H" as (rm'') "(Hsh & Hrec & Hc & Hcs & Hpcs & Hres)".
      iApply "HΦ". iExists _,_,_. iFrame. iSplitR. done. iRight.
      iDestruct "Hres" as "[[% HQs] | [% HPs]]"; eauto. inversion H1. inversion H3.
  Qed.

  Lemma tpc_participant_spec `{TpcPartProt Σ} n
        (h : socket_handle) (s : socket)
        (a : socket_address) rm st prevst l r
        (req_handlerV fin_handlerV : ground_lang.val):
    a ∈ l →
    st = PS_INIT prevst →
    saddress s = Some a →
    req_handler_spec n req_handlerV a -∗
    fin_handler_spec n fin_handlerV a -∗
    {{{ tpc_inv_ps_I ∗ a ⤇ tpc_participant_si a ∗ ownA l ∗ h s↦[n] s ∗ a r↦{½} rm ∗
        a ↦p{¼} (r, st) ∗ R a }}}
      ⟨n;tpc_participant_setup #(LitSocket h) req_handlerV fin_handlerV⟩
    {{{ RET 〈n;#()〉; True }}}.
  Proof.
    iIntros (Hpart Hstate Haddress) "#HrecH #HfinH".
    iIntros (Φ) "!# (#Hpsinv & #Hφ & #Hparts & Hs & Hrec & Hps & HR) HΦ".
    wp_lam. do 2 wp_let. wp_closure.
    iLöb as "IH" forall (rm r st prevst Hstate). wp_pures.
    wp_apply (listen_wait_spec with "[Hs Hrec]");
      first exact Haddress; iFrame; iFrame "#".
    iIntros (m mId) "(Hs & Hrec & HmId & _ & Hm)".
    iDestruct "Hm" as "[Hreq | Hready]"; last first.
    { rewrite /tpc_participant_global_si.
      iDestruct "Hready" as (g l' pl m0 r' sc sp)
                              "(% & Hfalse & _ & _ & _ & _ & Hps' & _)".
      iDestruct (mapsto_agree (L:=socket_address) with "Hps Hps'") as %Heq;
        simplify_eq. iDestruct "Hfalse" as %[Hfalse | Hfalse]; inversion Hfalse. }
    iDestruct "Hreq" as (l' r' p Hainl Hisreq)
                          "(#Hparts' & #Hcsi & Hcs & Hps' & HP)".
    iDestruct (mapsto_agree (L:=socket_address)
                            (V:=nat * participant_state)
                 with "Hps Hps'") as %Heq; simplify_eq.
    set (c:=ms_sender (message_stable_from_message m)).
    rewrite (ms_sender_message _ c) /=; last auto.
    wp_let. wp_proj. wp_let. wp_proj.
    wp_apply ("HrecH" $! _  m with "[Hps Hps' HP HR]"); eauto.
    { iCombine "Hps" "Hps'" as "Hps".
      rewrite Qp_quarter_three_quarter.
      iFrame. eauto. }
    iIntros (v) "H". iDestruct "H" as (ps Hisvote) "(Hps & HR' & Hmsg)".
    rewrite -{10}Qp_quarter_three_quarter. iDestruct "Hps" as "[Hps Hps']".
    iDestruct "Hmsg" as %Hstatus; simpl. simplify_eq.
    wp_apply (wp_send_to_bound with "[$Hs Hps' HmId Hcs Hcsi]");
      eauto; iFrame; iFrame "#".
    { iLeft. iExists _,l',ps. simpl. iFrame; iFrame "#".
      iPureIntro; eauto. }
    iIntros "[Hs _]". wp_let.
    wp_apply (listen_wait_spec with "[Hs Hrec]");
      first exact Haddress; iFrame; iFrame "#".
    iIntros (m_res mId_res) "(Hs & Hrec & HmId_res & _ & Hm)". wp_let.
    iDestruct "Hm" as "[Hreq | Hready]".
    { iDestruct "Hreq" as (? ? ? ? ? )
                          "(_ & _ & _ & Hps' & _)".
      iDestruct (mapsto_agree (L:=socket_address)
                              (V:=nat * participant_state)
                   with "Hps Hps'") as %Heq; simplify_eq.
      destruct Hstatus as [[_ Hfalse] | [_ Hfalse]]; inversion Hfalse. }
    iDestruct "Hready" as (gres msgs l'' m' r'' sc) "H".
    iDestruct "H" as (Hpartcoh Hglobal Hps Hgres)
                       "(#Hcsi' & #Hparts'' & Hc & Hps' & Hmsgs & Hgres)".
    iDestruct "Hgres" as %Hfinalres.
    iDestruct (mapsto_agree (L:=socket_address) with "Hps Hps'") as %Heq; simplify_eq.
    wp_proj.
    wp_let. wp_proj.
    wp_apply ("HfinH" with "[Hps Hps' HR']"); eauto.
    { iCombine "Hps" "Hps'" as "Hps".
      rewrite Qp_quarter_three_quarter.
      iFrame. eauto. }
    iDestruct 1 as (s' m') "(Hps & HR & Habort)".
    rewrite -{10}Qp_quarter_three_quarter.
    iDestruct "Hps" as "[Hps Hps']". wp_seq.
    iDestruct (ownA_agree l' l'' with "[$Hparts' $Hparts'']") as %->; eauto.
    wp_apply (wp_send_to_bound with "[Hc Hps' $Hs Habort]"); eauto; iFrame; iFrame "#".
    { iRight. iExists _,_,_,_,_. simpl. iFrame; iFrame "#". iSplitR; auto.
      iDestruct "Habort" as "[[[% %] HP] | [[% %] HP]]";
        destruct Hfinalres as [[_ [Hisnotab Hcommit]] | [_ [Hisab Habort]]]; eauto. }
    iIntros "[Hs _]". wp_seq.
    iApply ("IH" $! _ (S r') with "[] Hs [Hrec] [Hps] [HR]"); eauto.
  Qed.

End tpc_participant_protocol.
