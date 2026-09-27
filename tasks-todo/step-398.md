# step-398 — Un STOP reçu en MO n'est appliqué qu'à la prochaine mutation Admin

> **Jalon :** Défaut de conformité trouvé en ouvrant step-399 · **Statut :** À FAIRE
> **Dépend de :** step-395 · **Bloque :** step-399, step-410

## Pourquoi cette fiche existe

Un STOP reçu en MO écrit sa ligne `suppressions` (`internal/modlrrouter/stop.go:182`) et **n'annonce
rien**. Le routeur filtre l'opt-out par un Bloom en mémoire : un « absent » répond « non désabonné » sans
lire la base (`internal/pipeline/optout/optout.go:129`), et ce Bloom n'est rechargé qu'au rebuild
déclenché par une invalidation. Tant qu'aucune mutation Admin n'arrive — des jours, parfois — l'abonné
qui a répondu STOP continue de recevoir des MT.

C'est exactement ce que la spec interdit (§6.20) : « jamais de faux négatif — la propriété qui compte
ici : un faux négatif enverrait à un désabonné ». Ce n'est pas une invalidation perdue (step-399) : c'est
une invalidation qui n'existe pas, sur le chemin nominal.

START n'est pas concerné : un Bloom qui garde une suppression retirée répond « peut-être », et la lecture
exacte en base répond « non ».

## Design arrêté

Arbitrage Fable le 2026-09-27, corrigé sur un point (la course, ci-dessous). La spec ne fixe aucune borne
chiffrée (Annexe B : « near-immediate » ; §6.20 : « jamais de faux négatif ») ; aucun ADR ne tranche.

**Annonce dédiée, rebuild sélectif.** Après l'écriture d'un STOP, `StopDetector` publie sur un canal
dédié `optout:changed` — au mieux, sous un délai d'1 s (l'annonce Admin en a 5 ; celle-ci est sur le
chemin du consommateur MO), et même si la suppression existait déjà (`created=false`) : un STOP répété
répare une annonce perdue. Publier
`config:changed` aurait déclenché un rebuild **complet** par STOP, Bloom exact compris (`exact_routes`,
alimentée par la base MNP, peut compter des millions de lignes), sur chaque réplica.

`router-svc` lance un **second `config.Watcher`**, réutilisé tel quel (fenêtre de 250 ms, rejeu de
step-395), dont le rebuild n'est que `optOut.Reload` : un STOP coûte une relecture de `suppressions` et de
`inbound_numbers`, pas du reste. Il est supervisé comme le premier et pose la jauge
`bloom_capacity_bits{filter="optout"}` comme lui. Il n'alimente pas `config_rebuild_total` : cette
métrique dit la fraîcheur de la config entière.

**La course, que l'arbitrage jugeait bénigne.** Les deux watchers peuvent appeler `Enforcer.Reload` en
même temps. Chaque appel lit puis remplace : un rebuild général qui a lu `suppressions` juste avant le
commit d'un STOP peut remplacer le Bloom **après** le watcher opt-out qui, lui, l'a lu après — et le STOP
est perdu de nouveau. `Enforcer.Reload` prend donc un verrou sur la lecture et le remplacement : le
dernier à lire est le dernier à écrire.

**Échec de publication.** Journalisé, jamais propagé : un STOP n'interrompt jamais la remise du MO
(§6.20). Le rattrapage d'une annonce perdue est l'objet de step-399 (resynchronisation périodique).

**Documentation.** Le canal rejoint la liste des clés Redis (`db/schema_passerelle_sms.sql` — commentaire
seul, aucune migration — et Annexe B de la spec).

## Chaîne de preuves

1. Rouge de bout en bout dans `cmd/router-svc` : une suppression écrite en base puis annoncée sur
   `optout:changed`, **sans** aucune annonce `config:changed`, fait refuser un MT vers ce numéro. Aujourd'hui
   le MT passe.
2. `internal/modlrrouter` : un STOP publie `optout:changed`, y compris quand la suppression existait déjà ;
   un échec de publication ne fait pas échouer le MO.
3. `internal/pipeline/optout` : deux `Reload` concurrents, le plus ancien lu en premier, laissent le plus
   frais en place.
4. Mutations : publication retirée, second watcher débranché, verrou retiré — chacune fait tomber un test.
5. `make check` vert.

## Hors périmètre

La resynchronisation périodique (step-399). Le rechargement à chaud des `opt_out_keywords` de
mo-dlr-router, chargés au boot seulement (`cmd/mo-dlr-router-svc/wiring.go:287`).
