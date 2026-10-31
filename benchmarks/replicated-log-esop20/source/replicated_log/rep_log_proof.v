From iris Require Import invariants.
From iris.algebra Require Import gmap frac agree frac_auth.
From iris.base_logic Require Export gen_heap fancy_updates.
From iris.base_logic.lib Require Export own saved_prop viewshifts.
From iris.program_logic Require Export weakestpre.
From iris.proofmode Require Import coq_tactics.
From iris.proofmode Require Export tactics.
From iris.program_logic Require Import ectx_lifting total_ectx_lifting.
From stdpp Require Import base numbers.
From aneris Require Import tactics proofmode notation adequacy network.
From aneris.examples.replicated_log Require Import rep_log.
From aneris.examples.library Require Import  frac_auth.
From aneris.examples.two_phase_commit Require Import tpc_proof.

Import Network.
Import tpc.

Class repLogG Σ := RepLogG {
                       repLog_inG :> gen_heapG socket_address string Σ;
                       repWait_inG :> gen_heapG socket_address (string * string) Σ;
                     }.

Class repLogPreG Σ := RepLogPreG {
  repPreLog_inG :> gen_heapPreG socket_address string Σ;
  repPreWait_inG :> gen_heapPreG socket_address (string * string) Σ;
                   }.

Definition repLogΣ : gFunctors :=
  #[gen_heapΣ socket_address string;
    gen_heapΣ socket_address (string * string)].

Instance subG_inG_repLogΣ {Σ} :
  subG repLogΣ Σ → repLogPreG Σ.
Proof. constructor; solve_inG. Qed.

Section rep_log.
  Context `{tG : tpcG Σ}
          `{rlG : repLogG Σ}
          `{dG : distG Σ}
          `{N : namespace}.

  Definition gen_heap_ctxDB σ := gen_heap_ctx (L:=socket_address) (V:=string) σ.
  Definition gen_heap_ctxW σ :=
    gen_heap_ctx (L:=socket_address) (V:=string * string) σ.

  Notation "p ↦L{ q } l" := (mapsto (L:=socket_address)
                                    (V:=string) p q l) (at level 20) : uPred_scope.
  Notation "p ↦L l" := (mapsto (L:=socket_address)
                               (V:=string) p 1 l) (at level 20) : uPred_scope.
  Notation "p ↦W{ q } m" := (mapsto (L:=socket_address)
                                    (V:=string * string) p q m)
                              (at level 20) : uPred_scope.
  Notation "p ↦W m" := (mapsto (L:=socket_address)
                               (V:=string * string) p 1 m)
                         (at level 20) : uPred_scope.

  Lemma wait_update_all σ l v v' :
    gen_heap_ctxW σ -∗ ([∗ list] p ∈ l, p ↦W v) -∗
    |==> ∃ σ', gen_heap_ctxW σ' ∗ [∗ list] p ∈ l, (p ↦W v').
  Proof.
    iIntros "Hctx Hlist".
    iInduction l as [|y l] "IH".
    - simpl. iExists σ. eauto.
    - iDestruct "Hlist" as "[H Hl]".
      iDestruct ("IH" with "Hctx Hl") as ">Hup".
      iDestruct "Hup" as (σ') "(Hctx & Hl)".
      iDestruct (gen_heap_valid (L:=socket_address) σ'
                   with "Hctx H") as %Hlookup.
      iDestruct (gen_heap_update (L:=socket_address) σ' y _ v' with "Hctx H")
        as ">[Hctx H]".
      iDestruct (gen_heap_valid (L:=socket_address) (<[y:=v']> σ')
                   with "Hctx H") as %Hlookup'.
      iFrame. iExists (<[y:=v']> σ'). iFrame. eauto.
  Qed.

  Definition request_msg := "REQUEST".
  Definition commit_msg := "COMMIT".
  Definition abort_msg := "ABORT".

  (* Global functions for TPC to get the type of message *)
  Definition is_req_log := λ m (n : nat), ∃ v, m = request_msg +:+ "_" +:+ v.
  (* Definition is_commit_log := λ m (n : nat), m = commit_msg. *)
  Definition is_abort_log := λ m (n : nat), m = abort_msg.
  Definition is_vote_log := λ m (n : nat), m = abort_msg ∨ m = commit_msg.
  Definition is_global_log :=  λ m (n : nat), m = abort_msg ∨ m = commit_msg.

  Definition tpc_inv_cs_n : namespace := N .@ "replog" .@ "tpc_cs".
  Definition tpc_inv_ps_n : namespace := N .@ "replog" .@ "tpc_ps".

  Lemma is_abort_log_dec m r : Decision (is_abort_log m r).
  Proof. rewrite /is_abort_log. solve_decision. Qed.

  Global Instance rep_log_tpc : TpcProt Σ := {|
                       is_req := is_req_log;
                       is_vote := is_vote_log;
                       is_abort := is_abort_log;
                       is_abort_dec := is_abort_log_dec;
                       is_global := is_global_log;
                       P := (λ m p, ∃ log s, ⌜m = request_msg +:+ "_" +:+ s⌝ ∗
                                             p ↦L{½} log ∗ p ↦W{½} (log, s))%I;
                       Q := (λ p n, ∃ log m,
                                p ↦L{½} (log +:+ m) ∗ p ↦W{½} (log, m) )%I;
                       tpc_inv_cs_name := tpc_inv_cs_n;
                       tpc_inv_ps_name := tpc_inv_ps_n;
                                     |}.

  Definition R_pa n llog lwait :=
    (λ p, ∃ (log : string) w', llog ↦[n] #log ∗ p↦L{½} log ∗ lwait ↦[n] w')%I.

  Definition R'_pa n llog lwait :=
    (λ p, ∃ (log : string) (m : string), llog ↦[n] #log ∗ p↦L log ∗
                                       lwait ↦[n] #m ∗ p ↦W{½} (log, m))%I.

  Definition rep_log_tpc_pa n llog lwait : TpcPartProt Σ :=
    {|
      R := R_pa n llog lwait;
      R' := R'_pa n llog lwait;
    |}.

  Definition rep_log_inv_n := N .@ "replog".
  Definition rep_log_inv := (∃ σ σ', gen_heap_ctxDB σ ∗ gen_heap_ctxW σ')%I.
  Definition rep_log_I := inv rep_log_inv_n rep_log_inv.

  Definition log_si : socket_interp Σ :=
    (λ msg, ∃ s φ,
        ⌜ms_body msg = s⌝ ∗ ms_sender msg ⤇ φ ∗
        (∀ m, (⌜ms_body m = commit_msg⌝ ∨ ⌜ms_body m = abort_msg⌝) -∗ φ m))%I.

  Lemma fin_handler_log_spec n p (lwait llog : loc)
        (Tpa:=rep_log_tpc_pa n llog lwait) :
    {{{ rep_log_I }}}
      ⟨n;fin_handler_log #lwait #llog⟩
    {{{ v, RET 〈n;v〉; fin_handler_spec n v p }}}.
  Proof.
    iIntros (Φ) "#Hinv HΦ". wp_lam. wp_pures. iApply "HΦ".
    iIntros (c m r s0 Φ'). iAlways.
    iIntros "(#HpsI & H) HΦ". iDestruct "H" as (Hisglobal Hdec) "[Hps HR]".
    wp_pures. case_bool_decide. inversion H as [Hmsg].
    rewrite /R' /= /R'_pa /R_pa /is_abort_log /abort_msg Hmsg.
    + wp_if.
      iDestruct "HR" as (log x) "(Hl & HL & Hw & HW)".
      wp_load. wp_load. wp_op.
      iApply fupd_wp.
      iInv tpc_inv_ps_n as (σ) ">H" "Hclose".
      iDestruct (gen_heap_update σ _ _ (r, PS_INIT PS_COMMIT)
                 with "H Hps") as ">[H Hps]".
      iMod ("Hclose" with "[H]") as "_". iExists _; iFrame.
      iInv rep_log_inv_n as (σ' ?) ">[HLctx HWctx]" "Hclose".
      iDestruct (gen_heap_update σ' _ _ (log +:+ x)
                 with "HLctx HL") as ">[HLctx [HL HL']]".
      iMod ("Hclose" with "[HLctx HWctx]") as "_". iExists _,_. iFrame.
      iModIntro.
      wp_store. iApply "HΦ". iExists PS_COMMIT, (m_body m). iFrame.
      iSplitR "HW HL". iExists _,#x; iFrame. iRight. iSplitR. eauto.
      iExists _,_. iFrame.
    + assert (Hisabort: m_body m = "ABORT").
      { destruct Hisglobal as [Habort | Hcommit]; eauto.
        destruct H. rewrite Hcommit. eauto. }
      iApply fupd_wp.
      iInv tpc_inv_ps_n as (σ) ">H" "Hclose".
      iDestruct (gen_heap_update σ _ _ (r, PS_INIT PS_ABORT)
                 with "H Hps") as ">[H Hps]".
      iMod ("Hclose" with "[H]") as "_". iExists _. iFrame. iModIntro.
      iDestruct "HR" as (log x) "(Hl & [HP HP'] & Hw & HW)".
      wp_if. iApply "HΦ". iExists PS_ABORT,("REQUEST" +:+ "_" +:+ x). iFrame.
      iSplitR "HW HP". iExists log,#x. iFrame. iLeft. iSplitR; auto.
      iExists _,_. iFrame; eauto.
  Qed.

  Lemma req_handler_log_spec n s (llog lwait : loc)
        (Tpa:=rep_log_tpc_pa n llog lwait) :
    ({{{ True }}}
      ⟨n;req_handler_log #lwait⟩
    {{{ v, RET 〈n;v〉; req_handler_spec n v s }}})%I.
  Proof.
    iIntros (Φ). iAlways.
    iIntros "H HΦ". wp_lam. wp_pures. iApply "HΦ".
    rewrite /req_handler_spec.
    iIntros (c m r s0 Φ'). iAlways.
    iIntros "(#HpsI & Hisreq & Hps & HP & HR) HΦ'". rewrite /= /R_pa.
    iDestruct "HR" as (log w') "(Hl & Hlog & Hlw)".
    iDestruct "Hisreq" as %[s' Hisreq]. wp_pures. rewrite Hisreq.
    wp_apply value_of_message_spec; eauto.
    { rewrite /request_msg /valid_tag. eauto. } iIntros (? Heq). simpl.
    iApply fupd_wp.
    iInv tpc_inv_ps_n as (σ) ">H" "Hclose".
    iDestruct (gen_heap_update σ _ _ (S r, PS_READY) with "H Hps") as ">[H Hps]".
    iMod ("Hclose" with "[H]") as "_". iExists _. iFrame. iModIntro.
    wp_store. wp_seq.
    iApply "HΦ'". iDestruct "HP" as (? v Hvm) "[Hlog' Hwait]".
    inversion Hvm; simplify_eq.
    iDestruct (mapsto_agree (L:=socket_address)
                 (V:=string) with "Hlog Hlog'") as %->. iExists _.
    rewrite /R'_pa /is_vote_log /commit_msg /is_abort_log /abort_msg. iFrame.
    iSplitR; eauto. iSplitL. iExists _,_. iFrame. iFrame. eauto.
  Qed.

  Lemma db_spec (n : node) A (a : socket_address) p l r pst :
    a ∈ l ->
    a ∈ A →
    port_of_address a = p ->
    {{{ rep_log_I ∗ tpc_inv_ps_I ∗ Fixed A ∗ a ⤇ tpc_participant_si a ∗ ownA l ∗
        FreePorts (ip_of_address a) {[p]} ∗ IsNode n ∗ a ↦L{½} "" ∗
        a ↦p{¼} (r, PS_INIT pst) }}}
      ⟨n;db #a⟩
    {{{ r, RET r; ⌜True⌝ }}}.
  Proof.
    iIntros (HinL HinA Hport Φ) "H HΦ".
    iDestruct "H" as "(#Hinv & #HtpcInv & #Hfixed & #Hsi & #HownL & H)".
    iDestruct "H" as "(Hip & Hn & HP & Hpst)".
    wp_lam. wp_socket h as "Hs". wp_let.
    wp_alloc lwait as "Hwaiting". wp_let.
    wp_alloc llog as "Hlog". wp_let.
    set (tpa := rep_log_tpc_pa n llog lwait).
    wp_bind (req_handler_log _)%E.
    wp_apply (req_handler_log_spec); eauto.
    iIntros (vreq) "#Hreq". wp_let.
    wp_apply (fin_handler_log_spec n _ lwait llog); eauto.
    iIntros (vfin) "#Hfin". wp_let.
    wp_apply (wp_socketbind_static
                with "[$Hfixed Hip $Hs]"); simpl; try done. by rewrite Hport.
    iDestruct 1 as (g) "(Hs & Hrecs & #Hsi')". wp_seq.
    (* Putting auto after times out *)
    wp_apply (tpc_participant_spec _ h _ a _ _ _ l with "[Hsi] [] [-HΦ]");
      last first; try iFrame; auto.
    iFrame "#". rewrite /R /= /R_pa. iExists _,_. iFrame.
  Qed.

  Definition dec_handler_fold_acc r : list message -> ground_lang.val -> iProp Σ :=
    (λ (l : list message) (v : ground_lang.val),
     ∃ ga, ⌜ga = filter (λ m, is_abort_m (m,r)) l⌝ ∗
           ⌜v = #true ∧ ga = [] ∨ v = #false ∧ ga ≠ []⌝)%I.

  Lemma list_filter_nil {A} P `{∀ x, Decision (P x)} :
    filter (A:=A) P [] = [].
  Proof. by rewrite /filter /list_filter. Qed.

  Lemma list_filter_cons {A} P `{∀ x, Decision (P x)} (a : A) (l : list A) :
    filter P (a::l) = filter P [a] ++ filter P l.
  Proof.
    destruct l.
    - by rewrite list_filter_nil app_nil_r.
    - rewrite {1}/filter {1}/filter /list_filter /=.
      case_decide; by rewrite list_filter_nil /=.
  Qed.

  Lemma list_filter_app {A} P `{∀ x, Decision (P x)} (l1 l2 : list A) :
    filter P (l1 ++ l2) = filter P l1 ++ filter P l2.
  Proof.
    induction l1.
    - by simpl.
    - rewrite -app_comm_cons
                 (list_filter_cons P a l1)
                 list_filter_cons
                 list_filter_cons list_filter_nil app_nil_r /=.
      rewrite -app_assoc. by rewrite IHl1.
  Qed.

  Lemma dec_handler_log_spec n s : dec_handler_spec n dec_handler_log s.
  Proof.
    iIntros (v l l' r Φ) "!# H HΦ".
    iDestruct "H" as (Hcoh) "(#Hinv & #Hparts & Hn & Hst & Hvotes & Hpst)".
    wp_lam.
    wp_apply (list_fold_spec n _  l #true v
                             (dec_handler_fold_acc r)
                             (λ m, (∃ (mId : message_id) (π : Qp),
                              ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m))%I
                             (λ m, (∃ (mId : message_id) (π : Qp),
                              ⌜is_vote (m_body m) r⌝ ∗ mId m↦{π} m))%I
                with "[] [Hvotes]"); last first.
    - iIntros (resV) "(Hacc & Hvotes)".
      iDestruct "Hacc" as (ga) "H";
        iDestruct "H" as %[Hga [[HresV Hgar] | [HresV Hgar]]];
        wp_let; iApply fupd_wp; rewrite HresV.
      +  iDestruct (coordinator_state_update_all _ _ (r, CS_COMMIT)
                      with "Hinv Hparts Hst Hpst") as ">(Hc & Hpcs)". iModIntro.
         wp_if. iApply ("HΦ" $! _ ga CS_COMMIT); eauto. iFrame.
         rewrite /is_abort_m /is_abort /= in Hga.
         rewrite /is_global /= /is_global_log /abort_msg /commit_msg.
         iSplitR. eauto. iSplitR;  eauto.
      +  iDestruct (coordinator_state_update_all _ _ (r, CS_ABORT)
                      with "Hinv Hparts Hst Hpst") as ">(Hc & Hpcs)". iModIntro.
         wp_if. iApply ("HΦ" $! _ ga CS_ABORT); eauto. iFrame.
         rewrite /is_global /= /is_global_log /abort_msg /commit_msg.
         iSplitR. eauto. iSplitR. eauto. eauto.
    - iFrame. rewrite /list_m_val map_map in Hcoh. iPureIntro. split.
      + rewrite /dec_handler_fold_acc. exact Hcoh.
      + exists []. eauto.
    - iIntros (m acc lacc lrem Φ') "!# H HΦ'".
      iDestruct "H" as (Hl) "[Hdec HP]". wp_lam. wp_let.
      rewrite /dec_handler_fold_acc. iDestruct "Hdec" as (ga Hfold) "Hacc".
      rewrite /is_vote /= /is_vote_log. iDestruct "HP" as (mId π Hvote) "H".
      iDestruct "Hacc" as %[[Hval Hga] | [Hval Hga]]; rewrite Hval; wp_if.
      + wp_op. case_bool_decide; iApply "HΦ'".
        * iSplitR; last eauto. iExists []. iSplitR; last eauto. iPureIntro.
          rewrite list_filter_app -Hfold Hga app_nil_l.
          rewrite /filter /list_filter list_filter_nil.
          case_decide; last done.
          rewrite /is_abort_log /abort_msg in H0.
          inversion H. rewrite H2 in H0. inversion H0.
        * assert (Habortmsg: (m_body m) = abort_msg).
          { destruct Hvote as [Hab | Hcm]; eauto. destruct H.
            rewrite Hcm /commit_msg. done. }
          iSplitR; last eauto. iExists [m].
          iSplitR; last eauto. iPureIntro.
          rewrite list_filter_app -Hfold Hga app_nil_l.
          rewrite /filter /list_filter list_filter_nil.
          case_decide; first done. destruct H0.
          by rewrite /is_abort_log.
      + wp_op.
        iApply "HΦ'". iSplitR; last eauto.
        iExists (ga ++ filter (λ m, is_abort_log (m_body m) r) [m]).
        iSplitR; last eauto. by rewrite list_filter_app Hfold. iPureIntro.
        right; split; eauto. intro. destruct Hga.
          by apply app_nil in H as [Hdone ?].
  Qed.

  Definition handlerR n tpca tpch tpcs dbs r : iProp Σ :=
    (∃ g ps log log2 s, tpca ↦c (r, CS_INIT) ∗ tpch s↦[n] tpcs ∗ tpca r↦{ ½} g ∗
         ([∗ list] p ∈ dbs, p ↦c (r, CS_INIT)) ∗
         ([∗ list] p ∈ dbs, p ↦p{¾} (r, PS_INIT ps)) ∗
         ([∗ list] p∈dbs, p ↦L{½} log) ∗
         ([∗ list] p∈dbs, p ↦W (log2,s))
    )%I.

  Lemma logger_spec n (ip : string) dbsV (dbs : list socket_address)
    addr tpca A r ps :
    NoDup dbs ->
    length dbs > 0 ->
    addr = SocketAddressInet ip 80 ->
    tpca = SocketAddressInet ip 1200 ->
    list_coh (list_sa_val dbs) dbsV ->
    addr ∈ A ->
    tpca ∉ A ->
    {{{ tpc_inv_cs_I tpca ∗ rep_log_I ∗ ownA dbs ∗ addr ⤇ log_si ∗ Fixed A ∗
        ([∗ list] p∈dbs, p ⤇ tpc_participant_si p) ∗
        IsNode n ∗ tpca ↦c (r,CS_INIT) ∗ FreePorts ip {[80%positive;1200%positive]} ∗
        ([∗ list] p∈dbs, p ↦c (r, CS_INIT)) ∗
        ([∗ list] p∈dbs, p ↦p{¾} (r,PS_INIT ps)) ∗
        ([∗ list] p∈dbs, p ↦L{½} "") ∗
        ([∗ list] p∈dbs, p ↦W ("",""))
    }}}
      ⟨n;logger #ip dbsV⟩
    {{{v, RET 〈n;v〉; True }}}.
  Proof.
    iIntros (Hnodubs Hlength Haddr Htpca Hcoh HainA HtnotinA).
    iIntros (Φ) "(#Hinv & #Hrepinv & #Hdbs & #Hlogsi & #Hfixed & #Hpsi & H) HΦ".
    iDestruct "H" as "(Hn & Hc & Hip & Hcs & Hps & Hlogs & Hupdates)".
    wp_lam. wp_let. wp_socket z1 as "Haddr". wp_let.
    wp_socket z2 as "Htpc". wp_pures. simplify_eq.
    iDestruct (FreePorts_distribute with "Hip") as "[Hip Hip']". set_solver.
    wp_apply (wp_socketbind_static with "[$Hfixed Hip $Haddr]"); eauto.
    iDestruct 1 as (g) "(Haddr & Harecs & _)". wp_seq.
    wp_apply (wp_socketbind_dynamic _ _ _ _ _ _ _ tpc_coordinator_si
                with "[Hip' $Htpc]"); eauto.
    iDestruct 1 as (g') "(Htpc & Htrecs & #Htsi)". wp_pures.
    iDestruct "Hn" as "#Hn".
    wp_apply (listen_spec (handlerR n (SocketAddressInet ip 1200) z2 _ dbs r)
                          (λ v, ⌜True⌝)%I
                          _ _ _ (SocketAddressInet ip 80)
                with "[] [-HΦ $Hlogsi]"); last auto; last iFrame; auto.
    iLöb as "IH" forall (g r).
    iIntros (m mId Φ') "!# H HΦ'".
    iDestruct "H" as "(Hhandler & Hs & Hrec & HmId & _ & HP)".
    wp_rec.  wp_let. wp_op. wp_op. wp_let.
    iDestruct "HP" as (s φa Hmbody) "(#Hasi & Hret)".
    iDestruct "Hhandler" as
        (? ? log log2 s') "(Hc & Htpca & Htpcrec & Hdbcs & Hdbps & Hlogs & Hupdates)".
    iApply fupd_wp.
    iInv rep_log_inv_n as (σ' ?) ">[HLctx HWctx]" "Hclose".
    iDestruct (wait_update_all _ _ _ (log, s) with "HWctx Hupdates") as ">H".
    iDestruct "H" as (?) "[HWctx Hupdates]".
    iMod ("Hclose" with "[HLctx HWctx]") as "_". iExists _,_. iFrame.
    iAssert ([∗ list] p∈dbs, p ↦W{½} (log, s) ∗ p ↦W{½} (log, s))%I
      with "[Hupdates Hrepinv]" as "Hupdates".
    { iApply (big_sepL_mono with "Hupdates").
      iIntros (k y Hlookup) "[H1 H2]". iFrame. }
    iDestruct (big_sepL_sep with "Hupdates") as "[Hupdates Hupdates']".
    iModIntro.
    wp_apply (tpc_coordinator_setup_spec n _ z2 _
                {|
                  sfamily := PF_INET;
                  stype := SOCK_DGRAM;
                  sprotocol := IPPROTO_UDP;
                  saddress := Some (SocketAddressInet ip 1200) |}
                (SocketAddressInet ip 1200) _ _ _ r
                with "[] [Hn Htpca Htpcrec Hc Hdbcs Hdbps Hlogs Hupdates]");
      eauto; last first.
    - iIntros (v) "H".
      iDestruct "H" as (ps' cs rm' Hisglob)
                         "(Htpcs & Htpcrec & Hc & Hcs & Hps & Hres)".
      wp_let.
      iDestruct "Hres" as "[[Hres Hcommit] | [Hres Habort]]";
        iDestruct "Hres" as %Hres.
      + iDestruct (big_sepL_sep with "[Hupdates' Hcommit]") as "Hcommit"; iFrame.
        iAssert ([∗ list] p ∈ dbs, p ↦L{½} (log +:+ s) ∗ p ↦W (log, s))%I
          with "[Hcommit]" as "Hcommit".
        { iApply (big_sepL_mono with "Hcommit").
          iIntros (k y Hin) "(Hlog & Hu)".
          iDestruct "Hlog" as (log0 m0) "[Hlog Hu']".
          iDestruct (mapsto_agree (L:=socket_address) with "Hu Hu'") as %Hseq;
          inversion Hseq; simplify_eq. iFrame.
        }
        iDestruct (big_sepL_sep with "Hcommit") as "[Hlogs Hwait]"; iFrame.
        wp_apply (wp_send_to_bound with "[$Hs Hret]"); eauto; iFrame; iFrame "#".
        iApply "Hret". simpl. iPureIntro.
        destruct Hisglob as [Hnotabort | Habort]; eauto.
        iIntros "[Hs _]". wp_seq.
        iApply fupd_wp.
        iMod (coordinator_state_update_all _ _ (S r, CS_INIT)
                with "Hinv Hdbs Hc Hcs") as "(Hc & Hcs)".
        iModIntro.
        wp_apply (listen_spec (handlerR n (SocketAddressInet ip 1200) z2 _ dbs (S r))
                              (λ v, ⌜True⌝)%I _ _
                              {|
                                sfamily := PF_INET;
                                stype := SOCK_DGRAM;
                                sprotocol := IPPROTO_UDP;
                                saddress := Some (SocketAddressInet ip 80) |}
                              (SocketAddressInet ip 80)
                    with "[] [-HΦ' $Hlogsi]"); eauto.
        iApply "IH".
        iFrame. iExists _,_,(log +:+ s),_,_. iFrame.
      + iDestruct (big_sepL_sep with "[Hupdates' Habort]") as "Habort"; iFrame.
        iAssert ([∗ list] p ∈ dbs, p ↦L{ ½} log ∗ p ↦W (log, s))%I
          with "[Habort]" as "Habort".
        { iApply (big_sepL_mono with "Habort").
          iIntros (k y Hin) "(Hlog & Hu)".
          iDestruct "Hlog" as (m' log0 m0) "(% & Hlog & Hu')"; simplify_eq.
          iDestruct (mapsto_agree (L:=socket_address) with "Hu Hu'") as %Hseq.
          inversion Hseq; simplify_eq. iFrame. }
        iDestruct (big_sepL_sep with "Habort") as "[Habort Hupdates]".
        wp_apply (wp_send_to_bound with "[$Hs Hret]"); eauto; iFrame; iFrame "#".
        iApply "Hret". simpl. iPureIntro.
        destruct Hres as [Habort Hcs].
        rewrite /is_abort_log in Habort. rewrite Habort. eauto.
        iIntros "[Hs _]". wp_seq.
        iApply fupd_wp.
        iMod (coordinator_state_update_all _ _ (S r, CS_INIT)
                with "Hinv Hdbs Hc Hcs") as "(Hc & Hcs)".
        iModIntro.
        wp_apply (listen_spec (handlerR n (SocketAddressInet ip 1200) z2 _ dbs (S r))
                              (λ v, ⌜True⌝)%I _ _
                              {|
                                sfamily := PF_INET;
                                stype := SOCK_DGRAM;
                                sprotocol := IPPROTO_UDP;
                                saddress := Some (SocketAddressInet ip 80) |}
                              (SocketAddressInet ip 80)
                    with "[] [-HΦ' $Hlogsi]"); eauto.
        iApply "IH".
        iFrame. iExists _,_,log,_,_. iFrame.
    - iFrame; iFrame "#". eauto.
      iSplitR. eauto. iSplitR. rewrite /is_req /= /is_req_log. eauto.
      iDestruct (big_sepL_sep with "[$Hlogs $Hupdates]") as "Hall".
      iDestruct (big_sepL_mono _
                               (λ k p, ∃ ps, p ↦p{¾} (r, PS_INIT ps))%I
                   with "[$Hdbps]") as "Hdbps".
      { iIntros (k y Hiny) "H". eauto. }
      iFrame. iApply (big_sepL_mono with "Hall").
      apply ms_body_message in Hmbody.
      rewrite /tpc_proof.P /= /request_msg Hmbody.
      iIntros. iExists _,_. iFrame. eauto.
    - iApply dec_handler_log_spec.
    - iExists g',ps,"","","". iFrame.
  Qed.

  Definition client_si : socket_interp Σ :=
    (λ msg, ⌜ms_body msg = commit_msg⌝ ∨ ⌜ms_body msg = abort_msg⌝)%I.

  Lemma client_spec n (ip : string) (logaddr : socket_address)
        (event : string) A :
    SocketAddressInet ip 80 ∉ A ->
    {{{ logaddr ⤇ log_si ∗ Fixed A ∗
        IsNode n ∗ FreePorts ip {[80%positive]} }}}
      ⟨n;client #ip #logaddr #event⟩
      {{{v, RET 〈n;#v〉; ⌜v = commit_msg ∨ v = abort_msg⌝ }}}.
  Proof.
    iIntros (HnotinA Φ) "(#Hlogsi & #Hfixed & Hn & Hports) HΦ".
    rewrite /client.
    wp_pures. wp_socket h as "Hs". wp_pures.
    wp_apply (wp_socketbind_dynamic _ _ A _ _ _ _ client_si
                with "[Hports $Hs]"); eauto.
    iDestruct 1 as (g') "(Hs & Hrecs & #Hsi)". wp_seq.
    wp_apply (wp_send_to_bound with "[$Hs]"); eauto; iFrame "#".
    rewrite /log_si /=.
    iExists event,client_si; eauto.
    iIntros "[Hs _]". wp_seq.
    wp_apply (listen_wait_spec with "[$Hs $Hrecs]"); eauto.
    iIntros (m mId) "(Hs & Hrecs & HmId & _ & [% | %])"; simpl;
      wp_proj; iApply "HΦ"; eauto.
  Qed.

End rep_log.

Section rep_log_runner.
  Context `{dG : distG Σ, tG : tpcG Σ, rlG : repLogG Σ, N : namespace}.

  Definition db1_ip : string := "127.0.0.1".
  Definition db2_ip : string := "localhost".
  Definition server_ip : string := "0.0.0.0".
  Definition client1_ip : string := "127.0.0.2".
  Definition client2_ip : string := "127.0.0.3".

  Definition db1_addr : socket_address := SocketAddressInet db1_ip 3306.
  Definition db2_addr : socket_address := SocketAddressInet db2_ip 3306.
  Definition server : socket_address := SocketAddressInet server_ip 80%positive.
  Definition coord_addr : socket_address := SocketAddressInet server_ip 1200.

  Definition ips : gset string := {[ server_ip ; db1_ip ; db2_ip ]}.
  Definition db_addresses : list socket_address := [db1_addr;db2_addr].

  Lemma mapsto_p_split_3_4 p x :
    p ↦p x -∗ p ↦p{¾} x ∗ p ↦p{¼} x.
  Proof.
      by rewrite mapsto_eq /mapsto_def -own_op -auth_frag_op
         op_singleton -pair_op agree_idemp frac_op' Qp_three_quarter_quarter.
  Qed.

  Lemma make_tpc_inv :
    ownA db_addresses -∗ gen_heap_ctxC ∅ -∗
    |==>
         tpc_inv_cs coord_addr ∗ coord_addr ↦c (0%nat, CS_INIT) ∗
         db1_addr ↦c (0%nat, CS_INIT) ∗ db2_addr ↦c (0%nat, CS_INIT).
  Proof.
    iIntros "HA Hc".
    iDestruct (gen_heap_alloc _ db2_addr (0%nat, CS_INIT)
                 with "Hc") as ">(Hc & Hdb2 & _)"; first set_solver.
    iDestruct (gen_heap_alloc _ db1_addr (0%nat, CS_INIT)
                 with "Hc") as ">(Hc & Hdb1 & _)".
    { rewrite lookup_insert_ne; set_solver. }
    iDestruct (gen_heap_alloc _ coord_addr (0%nat, CS_INIT)
                 with "Hc") as ">(Hc & Hcoord & _)".
    { repeat rewrite lookup_insert_ne; try set_solver. }
    iFrame.
    rewrite /tpc_inv_cs.
    iExists [db1_addr;db2_addr],_. iFrame. simpl. iSplitR.
      by rewrite !dom_insert_L dom_empty_L.
    iModIntro. iPureIntro.
    intros.
    rewrite lookup_insert.
    case (decide (p = coord_addr)); intro; simplify_eq.
    - rewrite lookup_insert in H; eauto.
    - rewrite lookup_insert_ne in H; last done.
      case (decide (p = db1_addr)); intro; simplify_eq.
      + rewrite lookup_insert in H; eauto.
      + rewrite lookup_insert_ne in H; last done.
        rewrite insert_empty in H.
        revert H.
        rewrite lookup_singleton_Some. by intros [_ <-].
  Qed.

  Lemma logger_runner_spec A :
    server ∈ A ->
    coord_addr ∉ A →
    db1_addr ∈ A →
    db2_addr ∈ A →
    SocketAddressInet client1_ip 80 ∉ A →
    SocketAddressInet client2_ip 80 ∉ A →
    {{{ server ⤇ log_si ∗
        db1_addr ⤇ tpc_participant_si (tpc:=rep_log_tpc (N:=N)) (db1_addr) ∗
        db2_addr ⤇ tpc_participant_si (tpc:=rep_log_tpc (N:=N)) (db2_addr) ∗
        Fixed A ∗
        ownA db_addresses ∗
        gen_heap_ctxC ∅ ∗
        gen_heap_ctxP ∅ ∗
        gen_heap_ctxDB ∅ ∗
        gen_heap_ctxW ∅ ∗
        FreeIP client1_ip ∗
        FreeIP client2_ip ∗
        [∗ set] ip ∈ ips, FreeIP ip }}}
        logger_runner
    {{{ v, RET v; True }}}.
  Proof.
    iIntros (HsinA HsnotinA Hdb1A Hdb2A Hc1A Hc2A Φ)
            "(#Hserver & #Hdb1si & #Hdb2si & #Hfix & #Hparts & H) HΦ".
    iDestruct "H" as "(Hcst & Hpst & Hlog & Hwait & Hc1ip & Hc2ip & Hips)".
    iApply fupd_wp.
    iDestruct (make_tpc_inv with "Hparts Hcst") as ">(Hinv & Hcs & Hcsp1 & Hcsp2)".
    iDestruct (gen_heap_alloc _ db1_addr (0%nat,(PS_INIT PS_COMMIT))
                 with "Hpst") as ">(Hpst & Hpdb1st & _)";
      first set_solver.
    iDestruct (gen_heap_alloc _ db2_addr (0%nat,(PS_INIT PS_COMMIT))
                 with "Hpst") as ">(Hpst & Hpdb2st & _)".
    { rewrite lookup_insert_ne; set_solver. }
    iDestruct (mapsto_p_split_3_4 with "Hpdb1st") as "[Hpdb1st Hpdb1st']".
    iDestruct (mapsto_p_split_3_4 with "Hpdb2st") as "[Hpdb2st Hpdb2st']".
    iDestruct (gen_heap_alloc _ db1_addr "" with "Hlog")
      as ">(Hlog & [Hpdb1log Hpdb1log'] & _)";
      first set_solver.
    iDestruct (gen_heap_alloc _ db2_addr "" with "Hlog")
      as ">(Hlog & [Hpdb2log Hpdb2log'] & _)".
    { rewrite lookup_insert_ne; set_solver. }
    iDestruct (gen_heap_alloc _ db1_addr ("","") with "Hwait")
      as ">(Hwait & Hpdb1wait & _)"; first set_solver.
    iDestruct (gen_heap_alloc _ db2_addr ("","") with "Hwait")
      as ">(Hwait & Hpdb2wait & _)".
    { rewrite lookup_insert_ne; set_solver. }
    iMod (inv_alloc tpc_inv_cs_n _ (tpc_inv_cs coord_addr) with "Hinv") as "#HcI".
    iMod (inv_alloc tpc_inv_ps_n _ tpc_inv_ps with "[Hpst]") as "#HpI".
    { iNext. iExists _. iFrame. }
    iMod (inv_alloc rep_log_inv_n _ rep_log_inv with "[Hlog Hwait]") as "#HrepI".
    { iNext. iExists _,_. iFrame. }
    iModIntro.
    iDestruct (big_sepS_delete _ _ "0.0.0.0" with "Hips") as "(Hc & Hips)";
      first set_solver.
    iDestruct (big_sepS_delete _ _ "127.0.0.1" with "Hips") as "(Hdb1 & Hips)";
      first set_solver.
    iDestruct (big_sepS_delete _ _ "localhost" with "Hips") as "(Hdb2 & _)";
      first set_solver.
    rewrite /logger_runner.
    wp_makeaddress. wp_let. wp_makeaddress. wp_let.
    wp_apply list_make_spec; auto. iIntros (? ?). simpl. wp_let.
    wp_apply list_cons_spec; auto. iIntros (? ?). simpl. wp_let.
    wp_apply list_cons_spec; auto. iIntros (dbs Hdbs). simpl. wp_let.
    wp_makeaddress. wp_let.
    wp_apply (wp_start with "[-]"); first auto. iFrame. simpl.
    iSplitL "Hpdb1log Hpdb2log Hpdb1st' Hpdb2st' Hdb1 Hdb2 Hc1ip Hc2ip HΦ";
      last first.
    { iNext. iIntros "Hn Hip".
      iApply (logger_spec _ server_ip _ db_addresses server coord_addr _
                with "[-] []"); try done.
      - apply NoDup_cons_2; last apply NoDup_singleton.
        inversion 1. inversion H2.
      - simpl. iFrame "#". iFrame.
      }
    iNext. wp_seq.
    wp_apply (wp_start with "[-]"); first auto. iFrame.
    iSplitL "Hpdb2log Hpdb2st' Hdb2 Hc1ip Hc2ip HΦ"; last first.
    { iNext. iIntros "Hn Hip".
      iApply (db_spec _ A _ _ db_addresses with "[Hn Hip $Hpdb1st' $Hpdb1log] []");
        eauto; try iFrame; try iFrame "#".
      rewrite /db_addresses /db1_addr /db1_ip. set_solver. }
    iNext. wp_seq.
    wp_apply (wp_start with "[-]"); first auto. iFrame.
    iSplitL "Hc1ip Hc2ip HΦ"; last first.
    { iNext. iIntros "Hn Hip".
      iApply (db_spec _ A _ _ db_addresses with "[Hn Hip $Hpdb2st' $Hpdb2log] []");
        eauto; try iFrame; try iFrame "#".
      rewrite /db_addresses /db1_addr /db1_ip. set_solver. }
    iNext. wp_seq.
    wp_apply (wp_start with "[-]"); first auto; iFrame.
    iSplitL "Hc2ip HΦ"; last first.
    { iNext. iIntros "Hn Hip".
      iApply (client_spec with "[Hn $Hip]"); eauto; iFrame "#". }
    iNext. wp_seq.
    wp_apply (wp_start with "[-]"); first auto; iFrame.
    iSplitL "HΦ"; last first.
    { iNext. iIntros "Hn Hip".
      iApply (client_spec with "[Hn $Hip]"); eauto; iFrame "#". }
    by iApply "HΦ".
  Qed.

End rep_log_runner.

Lemma make_repLogG `{repLogPreG} :
  (|==> ∃ _ : repLogG Σ, gen_heap_ctxDB ∅ ∗ gen_heap_ctxW ∅)%I.
Proof.
  iStartProof.
  iMod (gen_heap_init (L:=socket_address) (V:=string) ∅) as (γdb) "Hdb".
  iMod (gen_heap_init (L:=socket_address) (V:=string*string) ∅) as (γw) "HW".
  iModIntro.
  iExists {|
      repLog_inG := γdb;
      repWait_inG := γw;
    |}. iFrame.
Qed.

Definition rep_log_is :=
  {|
    state_heaps := ∅;
    state_sockets := ∅;
    state_ports_in_use :=
      <[server_ip := ∅ ]> $ <[db1_ip := ∅ ]> $ <[db2_ip := ∅ ]>
      $ <[client1_ip := ∅ ]> $ <[client2_ip := ∅ ]> $ ∅;
    state_ms := ∅;
  |}.

Definition fixed_dom : gset socket_address := {[ server; db1_addr; db2_addr ]}.
Definition client_ips : gset string := {[ client1_ip ; client2_ip ]}.
Definition all_ips : gset string := ips ∪ client_ips.

Lemma client_ips_disj :
  ips ## client_ips.
Proof. set_solver. Qed.

Definition socket_interp `{distG Σ, tpcG Σ, repLogG Σ, N : namespace} (sa : socket_address) : socket_interp Σ :=
  (match sa with
   | SocketAddressInet "0.0.0.0" 80 => log_si
   | SocketAddressInet "127.0.0.1" 3306 =>
     tpc_participant_si (tpc:=rep_log_tpc (N:=N)) (db1_addr)
   | SocketAddressInet "localhost" 3306 =>
     tpc_participant_si (tpc:=rep_log_tpc (N:=N)) (db2_addr)
   | _ => client_si
   end)%I.

Theorem rep_log_safe : adequate NotStuck logger_runner rep_log_is (λ v _, True).
Proof.
  set (Σ := #[distΣ; tpcΣ; repLogΣ]).
  apply (@dist_adequacy Σ _ all_ips fixed_dom); try done; last first.
  { set_solver. }
  { intros i.
    rewrite /all_ips !elem_of_union !elem_of_singleton.
    intros [[]|]; subst; set_solver. }
  { rewrite /all_ips /= !dom_insert_L dom_empty_L right_id_L !assoc_L //. }
  iIntros (dinvG).
  iMod (@make_repLogG Σ) as (?) "[? ?]".
  iMod (@own_alloc Σ (agreeR (leibnizO (list socket_address))) _ (to_agree db_addresses)) as (γ) "H"; first done.
  iMod (gen_heap_init (Σ:=Σ) (L:=socket_address) (V:=nat * coordinator_state) ∅) as (γc) "Hc".
  iMod (gen_heap_init (Σ:=Σ) (L:=socket_address) (V:=nat * participant_state) ∅) as (γp) "Hp".
  iModIntro. iExists socket_interp.
  iIntros "Hsi #Hsc Hips".
  iApply (@logger_runner_spec _ _ {|
      tpc_coordinator_stateG := γc;
      tpc_participant_stateG := γp;
      tpc_nodes_name := γ
    |} _ nroot with "[-] []"); eauto;
    rewrite /fixed_dom /server; try iFrame; try set_solver.
  rewrite (big_sepS_union _ {[SocketAddressInet server_ip 80;_]}); last set_solver.
  rewrite (big_sepS_union _ {[SocketAddressInet server_ip 80]}); last set_solver.
  rewrite big_sepS_singleton big_sepS_singleton big_sepS_singleton.
  iDestruct "Hsc" as "[[Hsi1 Hsi2] Hsi3]".
  unfold db1_addr,db2_addr. simpl. unfold db1_addr,db2_addr. iFrame "#".
  rewrite /all_ips. rewrite (big_sepS_union _ ips); last apply client_ips_disj.
  rewrite /client_ips (big_sepS_union _ {[client1_ip]}); last set_solver.
  rewrite big_sepS_singleton big_sepS_singleton.
  iDestruct "Hips" as "[Hips1 [Hips2 Hips3]]". iFrame.
Qed.
