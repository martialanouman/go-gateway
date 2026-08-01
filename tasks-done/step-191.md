# step-191 — PROXY protocol sur le listener SMPP (+ diagnostic de fin de session)

> **Jalon :** Audit pré-production (§1.11/M3) · **Statut :** À FAIRE
> **Dépend de :** step-026 · **Bloque :** step-205, step-207

## But
Rendre l'anti-brute-force de bind (step-026) utilisable en production derrière un répartiteur de charge.
Aujourd'hui `remoteIP()` (`internal/smppserver/listener.go` l. 302-310) lit l'adresse du **pair TCP** :
derrière un LB L4 sans PROXY protocol, tous les binds partagent une unique clé `bindfail:ip`, ce qui
transforme le throttle par IP en **throttle global** — un seul client fautif verrouille tous les autres
(auto-DoS). Le risque est déjà documenté en commentaire `OPERATIONAL` dans le code ; cette PR le corrige.

## Périmètre (ce que fait CETTE PR)
- `internal/smppserver/listener.go` : décodage optionnel de l'en-tête **PROXY protocol** (v1 et/ou v2) en
  tête de connexion, avant toute lecture SMPP ; l'IP client réelle alimente le throttle et les logs.
- Activation par configuration (désactivé par défaut — un déploiement sans LB ne doit rien changer).
- Remplacement du commentaire `OPERATIONAL` par la contrainte de déploiement réelle.
- **Nit inclus** (même fichier, même thème diagnostic) : `_ = sess.Serve(ctx)` l. 133 est le seul `_ =` non
  trivial du dépôt sans justification ; loguer/compter la fin de session anormale au lieu de la jeter.

## Points d'implémentation clés
- **Ne faire confiance à l'en-tête PROXY que si l'option est activée**, sinon un client direct pourrait
  usurper son IP et contourner le throttle. Activé = on écoute derrière un LB de confiance ; désactivé = on
  ignore totalement l'en-tête et on garde l'adresse du pair.
- **Borner le parsing** : en-tête malformé ou dépassant la taille attendue ⇒ connexion fermée, jamais de
  lecture non bornée en attendant un terminateur.
- Un **timeout de lecture** sur l'en-tête : une connexion qui se contente d'ouvrir sans rien envoyer ne doit
  pas immobiliser un slot.
- L'IP décodée doit alimenter **toutes** les consommations existantes de `clientIP` (`throttleBlocks`,
  `recordBindFailure`, logs) — pas seulement le throttle.
- **`ctx7`** avant d'ajouter une bibliothèque PROXY protocol (vérifier version et API à jour) ; le format v1
  est assez simple pour être décodé sans dépendance, à arbitrer sur cette base.

## Tests (écrits dans la même PR)
- Option activée : deux connexions portant des IP PROXY distinctes sont throttlées **indépendamment**.
- Option activée : deux connexions provenant du même pair TCP mais d'IP PROXY distinctes ne se pénalisent
  pas mutuellement (le cas d'auto-DoS aujourd'hui).
- Option désactivée : l'en-tête est ignoré, l'adresse du pair fait foi (non-régression).
- En-tête malformé / tronqué / absent alors que l'option est activée ⇒ connexion refusée proprement.
- Une fin de session en erreur est loguée (le nit).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] désactivé par défaut ; contrainte de déploiement documentée pour step-207

## Hors périmètre
TLS / SMPP-TLS / mTLS → step-205. Manifests k8s → step-207.
