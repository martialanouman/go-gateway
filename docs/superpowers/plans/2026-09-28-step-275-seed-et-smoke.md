# step-275 — seed du plan de contrôle et smoke bout-en-bout — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** chaque déploiement de test seede le plan de contrôle par l'Admin API puis prouve qu'un SMS
traverse (bind SMPP TLS → `submit_sm` → DLR reçu) ; un smoke rouge fait échouer le workflow.

**Architecture:** une commande `cmd/test-env` (`seed`, `smoke`) bâtie par `deploy-test.yml` hors
GoReleaser ; deux Jobs de l'overlay en phases `seed` et `smoke`, lancés par `gateway-deploy` après le
rollout de l'application ; `gateway-deploy` recopie `connector_id=<uuid>` des logs du seed dans la
ConfigMap `test-seed`, que `connector-pool-svc` lit en `CONNECTOR_ID`.

**Tech Stack:** Go (stdlib `net/http`, `crypto/tls`, `internal/smpp`), kustomize, bash.

**Spec:** `tasks-todo/step-275.md` § « Design arrêté » (commit `d2678a7`).

## Global Constraints

- `deploy/k8s/` et `scripts/render-manifests.sh` : **non modifiés**.
- `.goreleaser.yaml` : **non modifié** (garde `internal/deploy/images_test.go`).
- Seed **uniquement par l'Admin API** — jamais de SQL.
- Aucun secret en sortie standard ni dans git ; la seule ligne que `seed` imprime sur stdout est
  `connector_id=<uuid>`.
- Admin API : `https://admin-api-svc:8081/v1/admin`, mTLS (certificat `operator`, CA `tlsgen`),
  `Authorization: Bearer <jeton>` ; le jeton est la partie de `HTTP_ADMIN_TOKENS` avant le premier `:`.
- SMPP : `smpp-server-svc:2775`, TLS, `ServerName=smpp-server-svc`, même CA.
- Noms seedés, verbatim : client `test` ; sender ID `TEST` ; compte `smoke` ; credential `smpp_bind`
  de system_id `smoke` ; connecteur `smsc-simulator` (host `smsc-simulator`, port 2775, `bind_type`
  `trx`) ; route `test-catch-all` (`static`, priorité 100).
- Commentaires : zéro par défaut, seul le « pourquoi » non évident ; tout symbole exporté a une ligne
  de doc (`revive`). Messages et docs en français, identifiants en anglais.
- Un commit par tâche, message `type(scope): step-275 — …` + ligne `Co-Authored-By` de la session.

---

### Task 1: `cmd/test-env` — client Admin et `seed` idempotent

**Files:**
- Create: `cmd/test-env/admin.go`, `cmd/test-env/seed.go`
- Test: `cmd/test-env/seed_test.go`, `cmd/test-env/fakeadmin_test.go`

**Interfaces:**
- Produces:
  - `type admin struct{ base, token string; hc *http.Client }`
  - `func (a *admin) do(ctx context.Context, method, path string, in, out any) error`
  - `type connectorSpec struct{ Host string; Port int; SystemID, Password string }`
  - `func seed(ctx context.Context, a *admin, c connectorSpec) (connectorID string, err error)`
  - `func findSmokeAccount(ctx context.Context, a *admin) (accountID string, err error)` (réutilisée
    par Task 2)
  - constantes `customerName="test"`, `senderAddr="TEST"`, `accountName="smoke"`,
    `smokeSystemID="smoke"`, `connectorName="smsc-simulator"`, `routeName="test-catch-all"`
  - `fakeAdmin` (test) : `newFakeAdmin(t) *fakeAdmin`, champ `writes int` (POST+PATCH reçus),
    `srv *httptest.Server`, `admin() *admin`.

- [ ] **Step 1: écrire le faux Admin API** (`fakeadmin_test.go`) — un `httptest.Server` en mémoire
  qui sert exactement ces routes (préfixe `/v1/admin`) et incrémente `writes` sur chaque POST/PATCH :

  | Méthode + chemin | Comportement |
  |---|---|
  | `GET /customers?limit&cursor` | page `{"data":[…],"has_more":bool,"next_cursor":…}` ; **pagine par 1** pour exercer la boucle |
  | `POST /customers` | `{"name"}` → 201 `{"id","name"}` |
  | `GET/POST /customers/{id}/sender-ids` | tableau / 201 `{"id","address","status":"pending_carrier_approval"}` |
  | `PATCH /customers/{id}/sender-ids/{sid}` | `{"status"}` → 200 |
  | `GET /customers/{id}/smpp-accounts` | tableau |
  | `POST /smpp-accounts` | 201 `{"id","name","customer_id"}` |
  | `GET/POST /smpp-accounts/{id}/credentials` | tableau / 201 `{"id","type","system_id","status":"active","secret"}` |
  | `POST /smpp-accounts/{id}/credentials/{cid}/rotate` | 200 avec un `secret` neuf à chaque appel |
  | `GET/POST /connectors` | tableau / 201 `{"id","name"}` |
  | `GET/POST /routes` | tableau / 201 `{"id","name","target_connector_id"}` |

  Il refuse (401) toute requête sans `Authorization: Bearer tok`, et (422) tout POST dont le corps a un
  champ inconnu du contrat (`additionalProperties: false` côté serveur réel) — décoder avec
  `json.Decoder.DisallowUnknownFields` dans une struct par route calquée sur `api/openapi-admin.yaml`
  (`CustomerCreate`, `SenderIdCreate`, `SmppAccountCreate`, `ConnectorCreate`, `RouteCreate`, corps de
  `create-credential`). Pré-remplir un client **autre** (`"autre"`) en première page, pour que la
  recherche par nom doive paginer.

- [ ] **Step 2: écrire les tests rouges** (`seed_test.go`)

```go
func TestSeedCreatesTheControlPlaneThenReplaysWithoutWriting(t *testing.T) {
	f := newFakeAdmin(t)
	spec := connectorSpec{Host: "smsc-simulator", Port: 2775, SystemID: "gateway", Password: "pw"}

	id1, err := seed(context.Background(), f.admin(), spec)
	if err != nil {
		t.Fatalf("premier seed : %v", err)
	}
	if id1 == "" || f.connectorID(connectorName) != id1 {
		t.Fatalf("connector_id %q, want the id of connector %q", id1, connectorName)
	}
	if got := f.routeTarget(routeName); got != id1 {
		t.Errorf("route %q vise %q, want %q", routeName, got, id1)
	}
	if got := f.senderStatus(customerName, senderAddr); got != "active" {
		t.Errorf("sender ID %q : statut %q, want active", senderAddr, got)
	}
	if !f.hasActiveCredential(accountName, smokeSystemID) {
		t.Errorf("aucune credential smpp_bind %q active sur le compte %q", smokeSystemID, accountName)
	}

	before := f.writes
	id2, err := seed(context.Background(), f.admin(), spec)
	if err != nil {
		t.Fatalf("rejeu : %v", err)
	}
	if id2 != id1 {
		t.Errorf("rejeu : connector_id %q, want %q", id2, id1)
	}
	if f.writes != before {
		t.Errorf("rejeu : %d écritures, want 0", f.writes-before)
	}
}

func TestSeedActivatesAPendingSenderID(t *testing.T) {
	f := newFakeAdmin(t)
	f.preloadPendingSender(customerName, senderAddr)
	if _, err := seed(context.Background(), f.admin(), connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}); err != nil {
		t.Fatal(err)
	}
	if got := f.senderStatus(customerName, senderAddr); got != "active" {
		t.Errorf("statut %q, want active", got)
	}
}

func TestSeedSurfacesAnAdminRefusal(t *testing.T) {
	f := newFakeAdmin(t)
	a := f.admin()
	a.token = "wrong"
	if _, err := seed(context.Background(), a, connectorSpec{Host: "h", Port: 1, SystemID: "s", Password: "p"}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("err = %v, want the 401", err)
	}
}
```

  (`connectorID`, `routeTarget`, `senderStatus`, `hasActiveCredential`, `preloadPendingSender` sont des
  accesseurs de `fakeAdmin`.)

- [ ] **Step 3: lancer** `go test ./cmd/test-env/ -run TestSeed -v` — attendu : échec de compilation
  (`undefined: seed`).

- [ ] **Step 4: implémenter** `admin.go` :

```go
package main

type admin struct {
	base, token string
	hc          *http.Client
}

func (a *admin) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, msg)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
```

  puis `seed.go` : pour chaque objet, `GET` la liste, cherche par nom (client : pages de `limit=500`
  jusqu'à `has_more=false` ; sender ID par `address` ; compte par `name` ; credential par
  `system_id` **et** `status=="active"` ; connecteur et route par `name`), crée seulement s'il manque,
  dans l'ordre client → sender ID (puis `PATCH {"status":"active"}` s'il n'est pas `active`) → compte
  (`{"customer_id","name":"smoke","allowed_bind_types":"trx","max_sessions":2}` — 2 : le smoke du
  déploiement précédent peut encore tenir sa session le temps de l'unbind) → credential
  (`{"type":"smpp_bind","system_id":"smoke"}` ; le `secret` rendu est **ignoré**) → connecteur
  (`{"name","host","port","bind_type":"trx","system_id","password"}`) → route
  (`{"name":"test-catch-all","priority":100,"distribution_strategy":"static","target_connector_id"}`).
  `findSmokeAccount` = client `test` puis compte `smoke`, erreur explicite si l'un manque. Chaque
  erreur est enveloppée par l'objet visé (`fmt.Errorf("client %q : %w", …)`).

- [ ] **Step 5: lancer** `go test ./cmd/test-env/ -run TestSeed -v` — attendu : PASS.
- [ ] **Step 6: muter** (voir la mémoire `mutation-revert-hygiene` : `cp` avant) : (a) supprimer la
  recherche du connecteur → le rejeu doit échouer sur `writes` ; (b) retirer le `PATCH` d'activation
  → `TestSeedActivatesAPendingSenderID` doit échouer ; (c) retirer la pagination (une seule page) →
  le premier test doit échouer (le client `autre` occupe la page 1). Restaurer, reverdir.
- [ ] **Step 7: commit** `feat(test-env): step-275 — seed idempotent du plan de contrôle par l'Admin API`

---

### Task 2: `smoke` — bind TLS, submit, DLR

**Files:**
- Create: `cmd/test-env/smoke.go`
- Test: `cmd/test-env/smoke_test.go`

**Interfaces:**
- Consumes: `admin`, `findSmokeAccount`, `smokeSystemID`, `newFakeAdmin` (Task 1).
- Produces:
  - `type dialFunc func(ctx context.Context) (net.Conn, error)`
  - `func smoke(ctx context.Context, a *admin, dial dialFunc, retry time.Duration) error`
    (`ctx` porte l'échéance globale ; `retry` est l'intervalle entre deux binds refusés)

- [ ] **Step 1: tests rouges** — un pair SMPP en processus sur `net.Listen("tcp","127.0.0.1:0")`,
  écrit avec `smpp.ReadPDU`/`smpp.WritePDU` (modèle : `test/load/bindgen/bindgen.go:303`). Le pair lit
  le secret courant dans le `fakeAdmin` (dernier `rotate`) et refuse le bind (statut `0x0D`, ESME_RBINDFAIL) si le mot de passe diffère ; répond au `submit_sm` par `SubmitSMResp{MessageID:"gw-1"}`
  ; puis, selon le cas, envoie un `deliver_sm` `ESMClass: smpp.ESMClassMCDeliveryReceipt`,
  `ShortMessage: "id:<X> stat:UNDELIV err:001"` et exige un `deliver_sm_resp` de même séquence.

```go
func TestSmokeSucceedsOnTheReceiptOfItsOwnMessage(t *testing.T)          // X = gw-1, stat UNDELIV → nil : tout état final compte
func TestSmokeIgnoresAReceiptForAnotherMessage(t *testing.T)             // X = gw-0 seulement → erreur à l'échéance
func TestSmokeFailsWhenNoReceiptArrives(t *testing.T)                    // aucun deliver_sm → erreur contenant "DLR"
func TestSmokeRetriesARefusedBindUntilTheRotatedSecretIsKnown(t *testing.T) // 2 premiers binds refusés → nil
func TestSmokeUsesTheRotatedSecret(t *testing.T)                          // le pair n'accepte que le secret du dernier rotate
```

  Échéance de test : `context.WithTimeout(…, 2*time.Second)`, `retry` 10 ms. Chaque test seede d'abord
  le faux Admin via `seed(…)` de Task 1.

- [ ] **Step 2: lancer** `go test ./cmd/test-env/ -run TestSmoke -v` — attendu : `undefined: smoke`.
- [ ] **Step 3: implémenter** `smoke.go` :
  1. `findSmokeAccount` ; `GET /smpp-accounts/{id}/credentials` → id de la credential `smoke` active ;
     `POST …/rotate` sans corps → `secret`.
  2. boucle jusqu'à `ctx.Done()` : `dial(ctx)`, `bind_transceiver` (`SystemID: smokeSystemID`,
     `Password: secret`, `InterfaceVersion: smpp.InterfaceVersion34`) ; statut ≠ OK → fermer,
     `time.After(retry)`, recommencer (la rotation met un instant à atteindre session-manager).
  3. `submit_sm` : `SourceAddrTON: 5` (alphanumérique), `SourceAddr: senderAddr`,
     `DestAddrTON: 1`, `DestAddrNPI: 1`, `DestinationAddr: "33612345678"`,
     `RegisteredDelivery: smpp.RegisteredDeliveryReceipt`, `ShortMessage: []byte("step-275 smoke")` ;
     `submit_sm_resp` OK → `msgID`.
  4. lire jusqu'à l'échéance (`conn.SetDeadline` depuis `ctx.Deadline()`) : `enquire_link` →
     `enquire_link_resp` ; `deliver_sm` → toujours `deliver_sm_resp` ; si
     `ESMClass&smpp.ESMClassMCDeliveryReceipt != 0` et `ShortMessage` commence par `"id:"+msgID+" "`
     → `unbind`, return nil. Échéance → `fmt.Errorf("aucun DLR pour %s avant l'échéance", msgID)`.
  Le secret n'est jamais journalisé.
- [ ] **Step 4: lancer** — PASS.
- [ ] **Step 5: muter** : (a) accepter tout receipt sans comparer l'id → `IgnoresAReceiptForAnother`
  doit tomber ; (b) ne pas réessayer le bind → `Retries…` doit tomber ; (c) binder avec le secret de
  création plutôt que celui du rotate → `UsesTheRotatedSecret` doit tomber. Restaurer.
- [ ] **Step 6: commit** `feat(test-env): step-275 — smoke SMPP qui attend le DLR de son message`

---

### Task 3: `main.go` — sous-commandes et TLS

**Files:**
- Create: `cmd/test-env/main.go`
- Test: `cmd/test-env/main_test.go`

**Interfaces:**
- Consumes: `seed`, `smoke`, `admin`, `dialFunc`.
- Produces: binaire `test-env seed|smoke`, lu par les Jobs de Task 4 via ces variables
  d'environnement : `HTTP_ADMIN_TOKENS`, `CONNECTOR_SYSTEM_ID`, `CONNECTOR_PASSWORD`, `TLS_DIR`
  (contient `ca.crt`, `tls.crt`, `tls.key`), `ADMIN_URL` (défaut `https://admin-api-svc:8081/v1/admin`),
  `SMPP_ADDR` (défaut `smpp-server-svc:2775`).
  - `func adminToken(tokens string) (string, error)` — partie avant le premier `:` du premier jeton
    de la liste séparée par `,` ; erreur si vide.

- [ ] **Step 1: test rouge** `main_test.go` : `adminToken("tok:admin:read|admin:write,t2:admin:read")`
  → `"tok"` ; `adminToken("")` et `adminToken(":admin:read")` → erreur.
- [ ] **Step 2: lancer** — `undefined: adminToken`.
- [ ] **Step 3: implémenter** `main.go` : doc de package d'une ligne ; `os.Args[1]` ∈ {`seed`,
  `smoke`} sinon usage + exit 2 ; `tls.LoadX509KeyPair(TLS_DIR/tls.crt, tls.key)` et pool depuis
  `ca.crt` ; client HTTP `&http.Client{Timeout: 10*time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs, Certificates, ServerName: "admin-api-svc", MinVersion: tls.VersionTLS13}}}` ;
  `seed` : échéance 3 min, imprime `fmt.Printf("connector_id=%s\n", id)` ; `smoke` : échéance 3 min,
  `retry` 2 s, `dial` = `tls.Dialer{Config: &tls.Config{RootCAs, ServerName: "smpp-server-svc", MinVersion: tls.VersionTLS12}}.DialContext(ctx, "tcp", SMPP_ADDR)` ;
  `CONNECTOR_PORT` n'existe pas : le port du connecteur est `2775`, l'hôte `smsc-simulator`
  (Global Constraints). Erreur → `log.Fatal` (exit 1).
- [ ] **Step 4:** `go test ./cmd/test-env/ && go vet ./cmd/test-env/ && make lint` — PASS.
- [ ] **Step 5: commit** `feat(test-env): step-275 — sous-commandes seed et smoke`

---

### Task 4: overlay — Jobs `test-seed` / `smoke` et `CONNECTOR_ID`

**Files:**
- Create: `deploy/test/jobs/test-seed.yaml`, `deploy/test/jobs/smoke.yaml`
- Modify: `deploy/test/kustomization.yaml`, `deploy/test/patches/connector-pool-svc.yaml`,
  `deploy/test/check.sh`

**Interfaces:**
- Consumes: variables d'environnement de Task 3.
- Produces: Jobs nommés `test-seed` (étiquette `gateway.test/phase: seed`) et `smoke`
  (`gateway.test/phase: smoke`), image `ghcr.io/martialanouman/go-gateway/test-env:<VERSION>` ;
  ConfigMap attendue `test-seed`, clé `CONNECTOR_ID`.

- [ ] **Step 1: check.sh rouge** — ajouter avant `kubeconform` :

```bash
grep -q 'gateway.test/phase: seed$' "$all" || fail "pas de Job de seed"
grep -q 'gateway.test/phase: smoke$' "$all" || fail "pas de Job de smoke"
grep -q "go-gateway/test-env:$VERSION\$" "$all" || fail "l'image test-env ne porte pas la version déployée"
cid=$(grep -A4 -- '- name: CONNECTOR_ID$' "$all")
grep -q 'name: test-seed' <<<"$cid" || fail "CONNECTOR_ID ne vient pas de la ConfigMap test-seed"
! grep -q 'value:' <<<"$cid" || fail "CONNECTOR_ID garde sa valeur de production à côté de valueFrom"
```

  `make test-env` → échoue sur « pas de Job de seed ».
- [ ] **Step 2: Jobs** — `test-seed.yaml` (et `smoke.yaml` identique à `name`, phase et `args` près) :

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: test-seed
  labels:
    gateway.test/phase: seed
spec:
  backoffLimit: 2
  activeDeadlineSeconds: 240
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: test-env
          image: ghcr.io/martialanouman/go-gateway/test-env:v0.0.0
          args: [seed]
          env:
            - { name: TLS_DIR, value: /etc/test-env/tls }
            - name: HTTP_ADMIN_TOKENS
              valueFrom: { secretKeyRef: { name: gateway-secrets, key: HTTP_ADMIN_TOKENS } }
            - name: CONNECTOR_SYSTEM_ID
              valueFrom: { secretKeyRef: { name: gateway-secrets, key: CONNECTOR_SYSTEM_ID } }
            - name: CONNECTOR_PASSWORD
              valueFrom: { secretKeyRef: { name: gateway-secrets, key: CONNECTOR_PASSWORD } }
          resources:
            requests: { cpu: 10m, memory: 16Mi }
            limits: { memory: 64Mi }
          volumeMounts:
            - { name: tls, mountPath: /etc/test-env/tls, readOnly: true }
      volumes:
        - name: tls
          secret: { secretName: operator-tls }
```

  `smoke.yaml` : `name: smoke`, `gateway.test/phase: smoke`, `args: [smoke]`, `backoffLimit: 0`
  (le smoke réessaie lui-même ; un second pod doublerait le délai d'un vrai rouge), sans les deux
  variables `CONNECTOR_*`.
- [ ] **Step 3: kustomization** — ajouter `jobs/test-seed.yaml` et `jobs/smoke.yaml` à `resources`.
  Le patch `job-phase.yaml` ne les touche pas (son `labelSelector` est `part-of=go-gateway`, absent de
  ces Jobs) — le vérifier dans le rendu : chaque Job n'a qu'une étiquette de phase.
- [ ] **Step 4: patch connector-pool** — ajouter à l'`env` de `patches/connector-pool-svc.yaml` :

```yaml
            - name: CONNECTOR_ID
              value: null
              valueFrom:
                configMapKeyRef: { name: test-seed, key: CONNECTOR_ID, optional: true }
```

  (`value: null` efface la valeur de production à la fusion stratégique ; sans elle le conteneur
  porterait `value` et `valueFrom`, que l'API refuse.)
- [ ] **Step 5: tag** — dans `check.sh`, remplacer `kubectl kustomize deploy/test >"$out/all.yaml"` par
  `kubectl kustomize deploy/test | sed "s#go-gateway/test-env:v0.0.0\$#go-gateway/test-env:$VERSION#" >"$out/all.yaml"`
  (render-manifests.sh ne voit pas l'overlay ; la garde existante `:v0.0.0$` attrape un oubli).
- [ ] **Step 6:** `make test-env` — PASS (kubeconform compris). Muter : retirer `value: null` → la
  garde `value:` doit tomber ; retirer le `sed` → la garde `:v0.0.0$` ou `test-env:$VERSION` doit
  tomber. Restaurer.
- [ ] **Step 7: commit** `feat(deploy): step-275 — Jobs seed et smoke dans l'overlay de test`

---

### Task 5: `gateway-deploy` — phases seed et smoke, `CONNECTOR_ID`

**Files:**
- Modify: `deploy/test/host/gateway-deploy`, `deploy/test/host/gateway-deploy_test.sh`

**Interfaces:**
- Consumes: Jobs `test-seed` / `smoke` et leurs phases (Task 4) ; la ligne `connector_id=<uuid>` des
  logs de `test-seed` (Task 3).

- [ ] **Step 1: test rouge** — étendre le kubectl factice :

```bash
  *"get job -l gateway.test/phase=seed"*) echo test-seed ;;
  *"get job -l gateway.test/phase=smoke"*) echo smoke ;;
  *"get job test-seed -o jsonpath="*) echo "Complete=True," ;;
  *"get job smoke -o jsonpath="*)
    if [[ -n ${FAIL_SMOKE:-} ]]; then echo "Failed=True,"; else echo "Complete=True,"; fi ;;
  *"logs job/test-seed"*)
    [[ -n ${NO_SEED_LINE:-} ]] || echo "connector_id=${SEED_ID:-11111111-1111-1111-1111-111111111111}" ;;
  *"get configmap test-seed"*) echo "${CURRENT_ID:-}" ;;
```

  et `label_of` : `*"phase=seed"*|*"job test-seed"*) echo seed ;;`, `*"phase=smoke"*|*"job smoke"*) echo smoke ;;`,
  `*"rollout restart"*) echo restart ;;`, `*"configmap test-seed"*|*"apply -f -"*) echo seed ;;`.
  Assertions :
  1. premier déploiement (`CURRENT_ID` vide) : séquence
     `deps-apply deps-job job app-apply app-rollout seed restart smoke` ; le log contient
     `rollout restart deployment/connector-pool-svc`.
  2. id inchangé (`CURRENT_ID=$SEED_ID`) : séquence sans `restart`.
  3. `FAIL_SMOKE=1` : `gateway-deploy` sort non nul.
  4. logs de seed sans ligne `connector_id=` (`NO_SEED_LINE=1`) : sortie non nulle, et aucun `configmap` écrit.
  `bash deploy/test/host/gateway-deploy_test.sh` → échoue sur la séquence.
- [ ] **Step 2: implémenter** — après la boucle de rollout de l'application :

```bash
run_jobs seed
id=$(k logs job/test-seed | sed -n 's/^connector_id=//p' | tail -n1)
[[ $id =~ ^[0-9a-f-]{36}$ ]] || { echo "gateway-deploy: le seed n'a pas rendu de connector_id" >&2; exit 1; }
if [[ $(k get configmap test-seed --ignore-not-found -o jsonpath='{.data.CONNECTOR_ID}') != "$id" ]]; then
  k create configmap test-seed --from-literal=CONNECTOR_ID="$id" --dry-run=client -o yaml | k apply -f -
  k rollout restart deployment/connector-pool-svc
  k rollout status deployment/connector-pool-svc --timeout=600s
fi
run_jobs smoke
```

  (commentaire unique, le pourquoi : l'id est attribué par le serveur, l'overlay ne peut pas le porter.)
- [ ] **Step 3:** `make test-env` — PASS. Muter : retirer la comparaison (restart inconditionnel) →
  l'assertion 2 doit tomber ; déplacer `run_jobs smoke` avant le restart → l'assertion 1 doit tomber.
  Restaurer.
- [ ] **Step 4: commit** `feat(deploy): step-275 — gateway-deploy seede, recâble CONNECTOR_ID et lance le smoke`

---

### Task 6: identité des Jobs, image `test-env`, runbook

**Files:**
- Modify: `deploy/test/bootstrap-secrets.sh`, `deploy/test/bootstrap-secrets_test.sh`,
  `.github/workflows/deploy-test.yml`, `deploy/test/README.md`

- [ ] **Step 1: test rouge** — dans `bootstrap-secrets_test.sh` (mode `--print`), exiger un
  `kind: Secret` nommé `operator-tls` portant `ca.crt`, `tls.crt`, `tls.key`. Lancer → échec.
- [ ] **Step 2: bootstrap** — dans le bloc `{ … } >"$work/secrets.yaml"`, après la boucle des
  `<svc>-tls` :

```bash
  k secret generic operator-tls --from-file=tls.crt="$work/tls/operator.crt" \
    --from-file=tls.key="$work/tls/operator.key" --from-file=ca.crt="$work/tls/ca.crt"
```

  Test → PASS.
- [ ] **Step 3: workflow** — dans `deploy-test.yml`, après « Pousser les images » :

```yaml
      # Hors GoReleaser : internal/deploy/images_test.go refuse une image publiée que deploy/k8s ne
      # déploie pas. Même Dockerfile, même base distroless.
      - name: Image test-env
        run: |
          mkdir -p dist-test/linux/amd64
          CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o dist-test/linux/amd64/test-env ./cmd/test-env
          img=ghcr.io/martialanouman/go-gateway/test-env:$VERSION
          docker buildx build --platform linux/amd64 --build-arg BINARY=test-env -t "$img" --push dist-test -f Dockerfile
```

- [ ] **Step 4: README** — (a) prérequis : le paquet GHCR `test-env` naît privé, le passer public
  (même geste que les douze autres) ; (b) environnement déjà bootstrappé : créer `operator-tls` depuis
  le poste :

```bash
kubectl -n gateway create secret generic operator-tls --dry-run=client -o yaml \
  --from-file=tls.crt=$HOME/.config/go-gateway-test/operator.crt \
  --from-file=tls.key=$HOME/.config/go-gateway-test/operator.key \
  --from-file=ca.crt=$HOME/.config/go-gateway-test/ca.crt | ssh root@IP kubectl apply -f -
```

  (c) § 8 : `bind-credentials` sont les
  identifiants passerelle → simulateur ; un bind client se fait avec la credential `smoke` du compte
  `smoke`, secret obtenu par `POST /v1/admin/smpp-accounts/{id}/credentials/{credId}/rotate` (le smoke
  du déploiement suivant le re-rotate) ; (d) déroulé : `seed` puis `smoke` après l'application, un
  smoke rouge fait échouer le workflow.
- [ ] **Step 5:** `make test-env && actionlint .github/workflows/deploy-test.yml` (si `actionlint`
  est installé) — PASS.
- [ ] **Step 6: commit** `feat(deploy): step-275 — identité operator-tls, image test-env, runbook`

---

### Clôture (contrôleur, pas un sous-agent)

- `make check` vert ; revue (axes disjoints, dont un axe « code en trop ») → coupe → DoD cochée.
- Pose de `operator-tls` sur l'environnement en place (commande du README), merge, puis vérification
  du run Deploy test : `gateway-deploy: déployé` précédé du smoke `Complete`.
- Dernier commit de la PR : `git mv tasks-todo/step-275.md tasks-done/` + INDEX.
