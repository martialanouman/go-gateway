# step-250d — Les quatre politiques Redis documentées mais jamais prouvées

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-250 · **Bloque :** —

## Pourquoi cette fiche existe

step-250 a complété la matrice de référence des politiques de panne (`docs/guide-codage-go.md` §16)
avec quatre lignes Redis qu'elle omettait, **et n'a testé aucune des quatre**. Sa section « Hors
périmètre » le disait : « Le volet **et testée** du [MUST] §16 pour les quatre politiques neuves :
fiche à ouvrir. » La fiche n'a jamais été ouverte — trouvé en step-260, six semaines plus tard.

C'est le mode d'échec que le dépôt connaît déjà : une dette écrite dans un document daté, que plus
personne ne relit. Le `[MUST]` de §16 exige que chaque dépendance arrive avec sa politique de panne
documentée **et testée** ; ces quatre-là n'ont aujourd'hui que la moitié documentée.

## Les quatre lignes

| Sous-système | Politique déclarée | Ce qui existe |
|---|---|---|
| Registre de sessions | fail-closed (`ESME_RSYSERR`) au bind | un faux qui erre : `internal/session/server_test.go:121` |
| Anti-brute-force de bind | fail-open (un Redis mort n'interdit pas les binds) | `internal/smppserver/throttle_test.go:150` |
| Routage L0 (numéro exact) | fail-closed en rejeu | `internal/routing/l0_test.go:25` |
| Jeton d'annulation | asymétrique (ADR-0013) | `internal/cancel/cancel_test.go:251` |

**Un faux qui retourne une erreur n'est pas une coupure.** C'est précisément la distinction que
step-250 a établie : un double peut imiter la forme d'une panne sans son contrat — délais, sockets
mortes, commandes qui échouent en cours de pipeline. Le harnais existe (`redistest.Cuttable` /
`CuttableConfig` + `internal/testutil/tcpproxy`) et cinq paquets s'en servent déjà.

## Périmètre

Un test de chaos par politique, **dans le paquet qui porte la politique** (pas une suite unique) —
c'est la règle posée en step-250 et rappelée dans `docs/strategie-de-test-passerelle.md` §4.8.

Chacun suit la forme éprouvée : contrôle avec Redis UP (sans lui, on ne distingue pas un fail-closed
d'un harnais qui n'a jamais marché), `proxy.Cut()`, l'assertion de la politique, `proxy.Resume()`, et le
retour au comportement nominal — un repli qui « latche » est un défaut que seul le Resume révèle.

**Pour la ligne fail-closed du registre de sessions**, l'assertion structurante est la même qu'en
step-250 côté billing : `errs.CodeOf(err)` doit dire si l'erreur est **codée**. Un refus codé devient un
`ESME_RSYSERR` définitif ; un refus non codé est une faute transitoire. Se tromper de camp change le
sort du bind.

## Definition of Done

- [ ] `make check` vert
- [ ] les 4 politiques prouvées sur un Redis **réellement coupé**, chacune dans son paquet
- [ ] pour chacune, la mutation « la coupure ne compte pas » (neutraliser `Cut()`) vue tomber
- [ ] §16 inchangée si les politiques se confirment ; **corrigée si le code dit autre chose** — c'est
      l'issue la plus probable pour au moins une des quatre, et la plus utile

## Hors périmètre

Les politiques PostgreSQL (§16 n'en porte aucune) → **step-260b**, qui les documente et les teste.
