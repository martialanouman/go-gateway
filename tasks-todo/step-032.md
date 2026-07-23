# step-032 — Déconnexion forcée des sessions SMPP (fin de grâce, révocation, suspension)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-022, step-024, step-027 · **Bloque :** —

## But
Fermer les binds SMPP dont l'autorisation a cessé d'être valide **après** leur établissement. Un bind est une
connexion TCP longue durée, jamais réauthentifiée : aujourd'hui, une session ouverte survit indéfiniment à
l'événement qui aurait dû la couper.

## Pourquoi (écart constaté en revue de step-027)
La spécification l'exige deux fois, et rien ne l'implémente :

- §769 `docs/specification-technique-passerelle-sms.md` : « Passé la fenêtre, l'ancien secret est invalidé
  **et les binds encore authentifiés avec lui sont fermés**. »
- §220 `docs/guide-ingenierie-passerelle-sms.md` : « Révocation, passage `status != active`, suspension du
  compte ou du client **force la déconnexion des sessions concernées**. »

step-027 n'a fermé que la porte d'entrée : passé `grace_expires_at`, l'ancien secret n'ouvre plus de
**nouveau** bind, mais une session ouverte pendant la fenêtre reste connectée et continue d'émettre. Même
trou pour une révocation ou une suspension : elles bloquent le prochain bind, pas le trafic en cours.

`SessionRegistry.Unbind` (`api/proto/session.proto:19`) ne convient pas tel quel — il retire l'entrée du
registre lors d'un unbind gracieux ou d'une perte de connexion, mais ne ferme aucune connexion côté pod
propriétaire. Il manque un ordre descendant : registre → pod → socket.

## Périmètre (ce que fait CETTE PR)
- Ajouter un RPC de déconnexion au `SessionRegistry` (p. ex. `Disconnect(account_id, reason)`), routé vers le
  pod propriétaire comme l'est déjà `Deliver` (step-046 en fournit le motif de routage).
- Côté `smpp-server-svc` : fermer les sessions visées proprement — `unbind` sortant si le lien le permet,
  puis fermeture du socket et libération du jeton de registre (le quota `max_sessions` doit se libérer).
- Déclencher sur les trois événements : expiration de `grace_expires_at`, `credential.status != active`
  (révocation/désactivation), suspension du compte ou du client.
- Le déclencheur d'expiration de grâce a besoin d'une horloge : balayage périodique borné côté
  `admin-api-svc` ou `session-manager-svc`. Choisir le mécanisme le plus simple qui tienne le multi-pod.

## Points d'implémentation clés
- **Idempotence** : déconnecter une session déjà partie ne doit pas être une erreur.
- **Borne** : une révocation sur un compte à `max_sessions` élevé ne doit pas produire une rafale non bornée.
- Motif de déconnexion loggé (identifiants uniquement, jamais de secret — §1.9).
- Ne jamais fermer une session dont l'autorisation est encore valide : le balayage lit `grace_expires_at`,
  il ne le recalcule pas.

## Tests (écrits dans la même PR)
- Bind établi avec l'ancien secret pendant la grâce → après expiration, la session est fermée (horloge
  injectée ; ne pas dépendre d'un `sleep`).
- Révocation d'un credential → les sessions vivantes de ce compte tombent, le jeton `max_sessions` est rendu.
- Suspension du client → idem pour tous les comptes du client.
- Idempotence : deux ordres de déconnexion successifs, pas d'erreur.
- Une session d'un compte voisin, toujours valide, n'est pas touchée.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] la coupure post-grâce est prouvée **sur une session vivante**, pas seulement au bind (step-027)

## Hors périmètre
La coupure au bind (faite en step-027) ; la rotation des clés API REST — sans session persistante, une clé
expirée est rejetée à la requête suivante, il n'y a rien à fermer.
