# step-088 — Fenêtrage du `submit_sm` entrant (traitement concurrent borné par session)

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-025 · **Bloque :** —

## But
Découpler le traitement d'un `submit_sm` de la goroutine de lecture de la session. Aujourd'hui
`handleSubmit` appelle `ingestor.Accept` (dont le `Produce` Kafka **synchrone**) **inline sur l'unique
read-goroutine** : tant qu'un submit est en cours, aucun autre PDU n'est lu — `enquire_link` compris —
et les submits d'une session sont **sérialisés** (débit ≈ `1 / latence_produce` par bind, ce qui annule
le fenêtrage SMPP). Cette step introduit une **fenêtre entrante bornée par session** : la read-goroutine
reste non-bloquante, les submits sont traités en parallèle jusqu'à un plafond, et un dépassement répond
`ESME_RTHROTTLED`. Prérequis réel de la cible de perf (8 000 SMS/s soutenus) ; complète l'item
« fenêtre (window_size) » du serveur SMPP annoncé au §7 (M3) mais différé en step-024/025.

## Périmètre (ce que fait CETTE PR)
- `internal/smpp/session` : sémaphore entrant borné (`Config.InboundWindow`, défaut raisonnable, clampé
  ≥ 1). `handleSubmit` : la garde `canSubmit()` **et** la construction de la `SubmitRequest` restent sur
  la read-goroutine (l'état `st` en est propriétaire exclusif) ; puis acquisition **non-bloquante** d'un
  slot :
  - slot libre → un worker (`go`, suivi par `s.wg`) exécute `callOnSubmit` puis répond `submit_sm_resp`
    et libère le slot ;
  - fenêtre pleine → réponse immédiate `submit_sm_resp` avec `command_status = ESME_RTHROTTLED`, sans
    bloquer la read-goroutine.
- `internal/smppserver` : exposer la taille de fenêtre via `Options` → `session.Config.InboundWindow`.
- `cmd/smpp-server-svc` : valeur par défaut (constante ; configurable plus tard si besoin).

## Points d'implémentation clés
- **La read-goroutine ne bloque jamais** sur un submit : gate + build synchrones (rapides), dispatch ou
  throttle. `enquire_link`/`unbind`/binds restent répondus instantanément.
- **`st` reste propriété exclusive de la read-goroutine** : le worker ne lit ni ne mute jamais l'état ;
  il ne fait que `callOnSubmit` (déjà sous recovery de panic) + `reply`.
- **Écritures déjà sûres** : `s.write`/`s.reply` sérialisent via `writeMu` — un worker peut répondre en
  concurrence de la read-goroutine sans course. Réutiliser le pattern `wg`/sémaphore déjà présent pour
  la direction sortante (`window`, `pending`).
- **Réponses hors ordre assumées** : SMPP corrèle par `sequence_number`, donc N workers qui répondent
  dans le désordre sont conformes.
- **Drain à l'arrêt** : les workers sont ajoutés à `s.wg` ; `Serve` les attend après `readLoop`. Le
  `Produce` d'`ingest.Accept` est déjà `ctx`-borné → l'annulation à l'arrêt le débloque. Ajouter un
  **timeout de Produce borné** (sur le `ctx` du worker) pour qu'un Kafka qui pend sans annulation ne
  retienne pas un worker indéfiniment ; au dépassement, répondre `ESME_RSUBMITFAIL`.
- **Back-pressure = throttle, pas blocage** : quand la fenêtre est pleine (ESME qui dépasse sa propre
  fenêtre, ou Kafka lent), `ESME_RTHROTTLED` fait fail-fast et laisse l'ESME lever le pied — préférable
  à un blocage silencieux de la session.
- **Invariant (a)** inchangé : le corps reste dans `msg.Body` ; le worker ne le révèle jamais (le codec
  WIRE reste l'unique egress).
- **Ordre inter-messages** : à confirmer contre §1.6/§7.3 avant de coder — hypothèse : pas de contrat
  d'ordre entre deux `submit_sm` distincts d'un même compte (l'ordre de partition concerne les segments
  d'**un** message) ; le fenêtrage SMPP est de toute façon intrinsèquement désordonné.

## Découpage (commits atomiques, TDD strict)
1. `session` : `Config.InboundWindow` + sémaphore entrant + dispatch worker dans `handleSubmit` (gate/
   build sync, worker async, `wg`, drain). Test rouge : un `OnSubmit` bloquant en vol n'empêche pas la
   session de répondre à un `enquire_link` (l'`enquire_link_resp` arrive avant le `submit_sm_resp`).
2. `session` : fenêtre pleine → `ESME_RTHROTTLED` (acquisition non-bloquante). Test rouge dédié.
3. `session` : timeout de Produce borné côté worker → `ESME_RSUBMITFAIL` au dépassement. Test avec un
   `OnSubmit` qui excède le délai.
4. `smppserver`/`cmd` : câblage `InboundWindow` (Options → Config, défaut).

## Tests (écrits dans la même PR)
- **Keepalive** (`-race`) : `OnSubmit` bloquant + `enquire_link` concurrent → `enquire_link_resp` reçu
  pendant que le submit est en vol (critère central).
- **Concurrence** : N submits en vol simultanément (jusqu'à `InboundWindow`) avec un `OnSubmit` lent ;
  tous obtiennent `ESME_ROK`, réponses corrélées par `sequence_number` (ordre indifférent).
- **Fenêtre pleine** → `ESME_RTHROTTLED`.
- **Timeout Produce** → `ESME_RSUBMITFAIL`.
- **Drain** : `Serve` attend les workers en vol avant de rendre la main (pas de goroutine orpheline).
- La parité REST/SMPP reste verte (le mapping d'enveloppe est inchangé).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `enquire_link` répondu pendant un `submit_sm` en vol (keepalive prouvé sous Produce lent)
- [ ] submits concurrents jusqu'à la fenêtre ; dépassement → `ESME_RTHROTTLED`

## Hors périmètre
Coordination multi-pod de la fenêtre (chaque session borne la sienne) ; AIMD/adaptatif **entrant** (la
fenêtre fixe suffit ; l'AIMD **sortant** est step-086) ; disjoncteur/reconnexion SMSC (M8) ; le vrai
simulateur SMSC (M8) — le faux SMSC in-repo et un `OnSubmit` scriptable suffisent ici.
