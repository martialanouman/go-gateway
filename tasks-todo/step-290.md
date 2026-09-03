# step-290 — Sécurité : gosec, govulncheck, secrets, piste d'audit

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Durcir la chaîne : scan d'injection `gosec`, gate `govulncheck` en CI, gestion des secrets, et une
piste d'audit consolidée des actions sensibles.

## Périmètre (ce que fait CETTE PR)
- `make lint`/CI : intégrer **gosec** (scan injection/mauvais usages) en plus de golangci-lint.
- `govulncheck` déjà présent (`make`) → en faire une **gate** bloquante documentée.
- Gestion des secrets : vérifier qu'aucun secret n'est en clair (hash pour mots de passe bind & clés API,
  §1.9), comparaison temps constant partout.
- Piste d'audit consolidée des actions opérateur/sensibles (réutilise les audits M10 : content, GDPR).

## Points d'implémentation clés
- **gosec** est un outil binaire (§16) — **`ctx7`** pour son intégration/exclusions justifiées ; ajouté à
  `make tools`.
- Vérifier les invariants de sécurité déjà posés : **SQL paramétré** (pas d'injection), secrets **hashés**
  (argon2id pour bind, SHA-256 pour clé API, §1.9), révélés une seule fois à la création/rotation.
- Aucune suppression d'alerte gosec sans justification en commentaire.
- La piste d'audit ne contient jamais de corps ni de secret en clair (invariant a).

## Tests (écrits dans la même PR)
- gosec passe (ou n'a que des exclusions justifiées) ; govulncheck vert.
- Un secret est bien hashé/comparé en temps constant (tests existants renforcés).
- Une action sensible produit une entrée d'audit.

## Hérité de la revue de step-250e (2026-09-02) — trois constats à trancher

Aucun n'est une régression de step-250e ; les deux premiers sont préexistants, le troisième est un
risque que cette step a rendu atteignable.

1. **Une cible `connector` lue depuis Redis n'est confrontée à rien.** `internal/routing/snapshot.go`
   vérifie l'appartenance d'une cible `route` au snapshot, mais renvoie une cible `connector` telle
   quelle. Qui écrit dans ce Redis peut donc détourner le trafic d'un MSISDN vers n'importe quel
   connecteur. Le modèle de menace le relativise — le même Redis porte les soldes de facturation et
   les token-buckets, il est dans la frontière de confiance — mais la posture mérite d'être écrite
   explicitement plutôt que subie.
2. **Aucun vecteur de hash `argon2` n'est épinglé.** Les tests de `internal/credential` hachent et
   vérifient avec la même version : un aller-retour circulaire. Si un futur bump de
   `golang.org/x/crypto` changeait la dérivation, ils resteraient **verts** pendant que tous les
   secrets déjà en base (mots de passe de bind SMPP, clés API) deviendraient invérifiables — panne
   totale d'authentification. Vérifié au moment du bump v0.53→v0.56 : le code d'`argon2` est identique
   entre les deux, donc le risque ne s'est pas matérialisé. Il n'est pas gardé pour autant.
3. **Une clé `exactroute:` de mauvais type boucle sans borne.** Un `WRONGTYPE` n'est ni `redis.Nil` ni
   une erreur de décodage : il est classé faute Redis, donc remonté, donc redélivré — sur la même clé,
   indéfiniment, et le TTL ne le borne pas puisque la clé n'expire pas d'elle-même. Toute la partition
   est figée jusqu'à un `DEL` manuel. Le traitement de la valeur illisible (guérie depuis la table)
   s'applique mot pour mot ; la distinction demande de reconnaître l'erreur `WRONGTYPE`, ce qui repose
   sur son texte — d'où le renvoi ici plutôt qu'un correctif fragile dans step-250e.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] gosec intégré ; govulncheck en gate ; audit sans secret/corps

## Hors périmètre
TLS/mTLS transport → step-300. Auth opérateur OIDC → step-310.
