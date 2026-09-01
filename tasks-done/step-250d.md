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

## Design arrêté

**Deux erreurs de cette fiche, corrigées.** La colonne « Ce qui existe » attribuait au registre de
sessions « un faux qui erre : `internal/session/server_test.go:121` ». Ce test est
`TestServer_DisconnectPublishErrorIsInternal` : un faux **publisher**, sur le chemin **Disconnect**,
construit avec `session.NewServer(nil, pub)` — registre nil. `Registry.Bind` sous panne Redis n'avait
**aucune couverture, d'aucune sorte**, et ne pouvait pas en avoir : `Registry` détient un
`*redis.Client` concret, pas une interface. C'était la plus nue des quatre, pas la mieux lotie. La
section « Hors périmètre » était elle aussi périmée : elle renvoyait les politiques PostgreSQL à
step-260b « qui les documente et les teste », alors que 260b n'a livré que la ligne billing et a renvoyé
les trois autres à step-260c.

**L'écart §16 prédit existe, il est unique, et il est dans la ligne du jeton d'annulation.** §16 écrivait
« fail-open côté pool (journalise et envoie) » ; le pool a **deux** sites fail-open, et le second
(`connectorpool.go:900`, branche d'expiration max-age) journalise et **dead-letter en
`delivery_expired`**. Une annulation concurrente y est mésenregistrée comme périmée. Le code l'assumait
en commentaire ; §16 l'ignorait. Corrigé, avec la précision du camp SMPP (`ESME_RSYSERR`, pas
`ESME_RCANCELFAIL`).

**Ce qui n'était PAS un écart, vérifié avant d'écrire.** ADR-0013 nomme `ESME_RCANCELFAIL` ; sur panne
Redis le refus est `ESME_RSYSERR`. Ce n'est pas une contradiction : `cancel.go:122-136` sépare nettement
`err != nil` → `ErrInternal` de `holder != HolderNone` → `ErrCancelFailed`. L'ADR décrit le second cas.
Rien à corriger.

**Six tests pour quatre politiques.** La politique du jeton **est** l'asymétrie : n'en prouver qu'un
côté n'en prouve que la moitié, et les deux côtés vivent dans deux paquets. Le couple est ce qui le
démontre — la mutation « `Claim` concède un jeton libre sous panne » fait tomber le versant fail-closed
(`internal/cancel`) **et laisse le versant fail-open vert** (`internal/connectorpool`). Sans ce couple on
aurait testé deux fois le même camp.

Le sixième est venu de la revue, et redit la même leçon d'un cran plus bas : **compter les sites, pas les
politiques.** La §16 corrigée ci-dessus nomme **deux** sites fail-open côté pool ; n'en prouver qu'un
laissait le second — la branche d'expiration, précisément celle qui mésenregistre — appuyé sur un faux
qui erre, soit la dette que cette fiche solde, refaite en petit par la PR qui la solde.
`TestExpiredCancellationIsMisfiledWhenTheCancelTokenStoreIsCut` le couvre : le jeton d'annulation posé
avant la coupure est ce qui empêche l'assertion d'être vraie de toute façon.

**Le contrôle doit rendre la coupure observable** — la leçon de step-250, appliquée quatre fois. Pour le
throttle : franchir réellement `MaxFailures` avant de couper, avec un mot de passe **valide**, parce
qu'un bind bloqué répond `ESME_RINVPASWD`, délibérément indiscernable d'un mauvais mot de passe. Pour le
registre : **deux comptes**, l'un à quota plein pour montrer à quoi ressemble un refus sur le fond
(`ESME_RBINDFAIL`), l'autre avec un slot libre pour que le refus mesuré ne puisse être que la panne.
Pour le L0 : semer le Bloom **et** la clé, un numéro hors Bloom étant un miss définitif sans appel
réseau. Pour le pool : le magasin doit prendre le jeton avant la coupure et redevenir décisif après,
sinon « le message est parti » serait vrai de toute façon.

**Ce qui n'est délibérément pas testé.** Le *coût* du fail-open : `throttleBlocks` appelle `Check` sans
timeout, et une découverte de panne devenue lente ferait de l'anti-brute-force un vecteur de DoS.
Hors de portée de l'outil — `Cut()` produit une socket morte, jamais un Redis lent. C'est la leçon de
step-260b (couper un lien ne teste pas ce qu'une coupure ne produit pas) ; le proxy retardateur est déjà
fiché en step-260c.

**La trouvaille qui dépasse la step.** En cherchant comment semer la clé du test L0 : **rien n'écrit
`exactroute:{msisdn}`** — ni config-sync, ni l'Admin API, et `EncodeTarget` n'a aucun appelant. Le
court-circuit L0 ne résout jamais et la portabilité des numéros ne fonctionne pas en production. Trois
commentaires du code affirmaient le contraire ; ils sont corrigés ici, et **step-250e** ouvre la
réparation. Cela n'invalide pas le test L0 : la lecture Redis, elle, s'exécute à chaque faux positif du
Bloom, donc la politique de panne est vivante même si la résolution ne l'est pas.

## Definition of Done

- [ ] `make check` vert
- [ ] les 4 politiques prouvées sur un Redis **réellement coupé**, chacune dans son paquet
- [ ] pour chacune, la mutation « la coupure ne compte pas » (neutraliser `Cut()`) vue tomber
- [ ] §16 inchangée si les politiques se confirment ; **corrigée si le code dit autre chose** — c'est
      l'issue la plus probable pour au moins une des quatre, et la plus utile

## Hors périmètre

Les politiques PostgreSQL (§16 n'en porte aucune) → **step-260b**, qui les documente et les teste.
