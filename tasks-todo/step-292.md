# step-292 — Priorité effective et réservation de connecteurs (ADR-0020 §2-§4)

> **Jalon :** ADR-0020 §2-§4 · **Statut :** EN COURS
> **Dépend de :** step-288, step-289 · **Bloque :** step-292b, les écrans connecteurs et simulateur de route
> du tableau de bord
> Sortie de la livraison sender ID du 05/10/2026 (step-288, 289, 291) ; unité faute de multiple de dix libre.

## Pourquoi
step-288, 289 et 291 livrent ce dont l'écran des sender IDs a besoin. Le reste d'ADR-0020 donne à la
catégorie son effet sur l'acheminement : la priorité effective et la réservation de connecteurs. ADR-0021
va à step-292b.

## Arbitrages
Tranchés le 06/10/2026, voir ci-dessous.

## Design arrêté (06/10/2026)

Arbitrages : spec d'abord, puis Fable, qui a tranché tous les points sans heurter la spec. Un écart à
l'avis de Fable, nommé ci-dessous (DLR).

**Découpage : deux fiches.** ADR-0021, action item 10, demande une fiche qui porte l'ADR et paie la dette
de la file prioritaire. Cette fiche ne garde donc qu'ADR-0020 §2-§4 ; ADR-0021 §1-§2 et §4-§6 vont à
**step-292b** (topics par catégorie, consommateurs par topic, ordonnanceur par bind), qui paie
`debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`.

**Place.** step-292 et 292b passent **avant step-409** : ADR-0021 §1 fixe les 4 partitions OTP et
transactionnelles comme « valeur de départ, que step-409 mesurera ». step-409 dépend donc de step-292b.
**step-287 passe avant step-292b**, sur l'ancienne topologie : ADR-0021 (action item 8) lui confie la mesure
de la clé `message_id` de step-282, référence qui calibre les partitions et la part minimale. Les deux PR
de cette fiche ne touchent pas la topologie : elles n'attendent pas step-287.

### PR1 — priorité effective portée jusqu'au SMSC et au CDR
- `TrafficCategory.EffectivePriority(requested)` : méthode pure du type, la table
  d'ADR-0020 §2 : `min(max(requested, défaut), plafond)`. Appelée dans `Pipeline.Process` après
  `senderid.Authorize`. `RoutedMT` gagne `TrafficCategory` et `Priority` (effective).
- `mt.routed` (`routedWire`) : `traffic_category` et `priority`. Décodage tolérant : champ absent →
  `marketing`/0. Reroute, parking, dead-letter et `mt-replay` passent tous par `EncodeRouted` et héritent
  des deux champs sans code propre.
- `buildSubmit` : `PriorityFlag` = priorité effective si non nulle, sinon `priority_flag_default` du
  connecteur (ADR-0020 §3). Le pool lit la colonne dans `connectorConfigSource.Load`, qui charge déjà la
  ligne : zéro requête neuve.
- Réponse automatique au STOP : `transactional`, priorité 1. C'est une confirmation réglementaire à l'abonné
  (§6.20), pas une campagne. La spec est muette sur ce point ; c'est une décision de Fable.
- CDR : `cdr.traffic_category LowCardinality(String) DEFAULT ''` et `cdr.priority UInt8 DEFAULT 0`, dans une
  migration ClickHouse. `cdr_events` ne change pas : la catégorie est un attribut du message, pas un jalon.
  **Point porteur.** `cdr` est `ReplacingMergeTree(version)`, et la fusion garde la *ligne* de plus haute
  version entière. Toute ligne écrite après l'autorisation doit donc porter les deux champs, faute de quoi
  la ligne finale les efface. Cela concerne :
  - `rerouted` et `failed` au pool ;
  - `OutcomeMT` sur `mt.outcome`, qui gagne les deux champs ;
  - **la ligne DLR** (`modlrrouter.buildCDRRow`, rang 40, la plus haute en régime nominal) : `dlrmap.Mapping`
    gagne les deux champs (JSON). Fable avait omis cet écrivain ; c'est le seul écart.

  La ligne `accepted`, projetée depuis `mt.inbound` avant l'autorisation, reste à `''`/0 : elle est
  supplantée par la suivante. La ligne `rejected` aussi, et c'est un écart à la première version de ce
  design, décidé à l'implémentation. La ligne est terminale, donc rien ne l'efface ; la porter obligerait
  `Process` à rendre un gabarit partiel avec son erreur. Le coût est consigné dans
  `debts/categorie-du-cdr-ni-servie-ni-sur-les-rejets.md`.
- L'Admin API ne sert pas encore ces colonnes : l'ADR ne demande aucun champ de contrat CDR (même fiche).
- `priority_flag_default` n'est modifiable par aucune opération Admin, pas plus que ses voisins `*_default` :
  `debts/defauts-smpp-du-connecteur-non-modifiables.md`.

### PR2 — `priority_tier` : un connecteur réservé est indisponible au rang inférieur
- Le routeur lit `priority_tier` dans le **`Snapshot` immuable**, sous forme d'une map `connectorID → tier`
  bâtie par `BuildSnapshot` et rebâtie au même rechargement que les routes. C'est de la configuration,
  pas de l'état volatil : il n'a rien à faire dans l'overlay du disjoncteur. À vérifier au plan :
  l'invalidation de `smsc_connectors` reconstruit-elle l'instantané du routeur ?
- Rang = une méthode `TrafficCategory.Rank()` (0/1/2), qui arrive avec cette PR. Un connecteur dont `tier > rang` est sauté :
  - **L0** : cible vérifiée avant retour, sinon chute vers L1/L2 (ADR-0004) ;
  - **script** : filtré sur sa sortie ;
  - **déclaratif** : dans chaque stratégie, et dans le repli de route ;
  - **`fallback_chain`** : filtrée.

  Il ne reste rien → `ErrNoRoute`. Aucun code d'erreur neuf, aucune métrique neuve (ADR-0020 renvoie la
  mauvaise configuration au simulateur de route).
- Spec §6.1 et §6.8 (définition de `priority_tier`) dans la même PR. PR1 porte §3.4 (CDR), §6.19
  (priorité effective) et §6.20 (catégorie de la réponse au STOP).
- Invariant b : la garde s'applique **après** la résolution et ne court-circuite aucune étape. Un test de
  l'invariant b le prouve sur un message L0 refusé par le tier puis routé en L2.
- La réponse au STOP n'est pas résolue (`ConnectorID` = connecteur du MO) et la garde ne s'y applique pas.
  C'est accepté : il s'agit d'un seul message, vers le lien qui vient de livrer le MO.

### Ordre de déploiement (PR1)
La migration ClickHouse 0007 passe d'abord : tout écrivain du CDR nomme les deux colonnes. Ensuite, un
ancien `mo-dlr-router-svc` ou un ancien projecteur `mt.outcome` ignore les champs neufs et écrit la ligne
la plus haute du segment sans catégorie. Il faut donc déployer ces deux consommateurs avant
`connector-pool-svc`. L'ordre inverse est sûr : un ancien enregistrement lu par le nouveau code donne
`marketing`/0. Seul l'environnement de test existe aujourd'hui (ADR-0021, *Conséquences*).

### Hors de cette fiche
Topics, consommateurs, ordonnanceur, lag par catégorie, `deploy/k8s` et la ligne « ordre du pipeline » de
`CLAUDE.md` vont à step-292b.

## Hors périmètre
Tout ce que step-288, 289 et 291 livrent.
