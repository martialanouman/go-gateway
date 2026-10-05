# step-292 — Priorité effective, réservation de connecteurs et un topic par catégorie

> **Jalon :** ADR-0020 §2-§4, ADR-0021 §1-§2 et §4-§6 · **Statut :** À FAIRE
> **Dépend de :** step-288, step-289 · **Bloque :** les écrans connecteurs et simulateur de route du tableau
> de bord ; paie `debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`
> Sortie de la livraison sender ID du 05/10/2026 (step-288, 289, 291) ; unité faute de multiple de dix libre.

## Pourquoi
step-288, 289 et 291 livrent ce dont l'écran des sender IDs a besoin. Le reste des deux ADR, qui donne à
la catégorie son effet sur l'acheminement, n'a pas de step (ADR-0021, action item 10).

## Arbitrages à trancher (dans la fiche, avant tout code)
- **Découpage** : cette fiche est sans doute trop grosse pour une PR. Une découpe naturelle :
  1. priorité effective sur `mt.routed`, `priority_flag` écrit par `buildSubmit`, et CDR
     `traffic_category`/`priority` ;
  2. garde `priority_tier` aux trois niveaux et sur la `fallback_chain` ;
  3. les six topics et un consommateur par topic, au routeur puis au pool ;
  4. l'ordonnanceur par bind et `CONNECTOR_MARKETING_MIN_SHARE`.
- **Place par rapport au go-live** : avant step-409, la campagne mesurerait la topologie finale. Après,
  step-409 mesure deux topics qui disparaîtront.
- **Ordre avec step-287** : la campagne du VPS mesure-t-elle l'ancienne topologie ?

## Hors périmètre
Tout ce que step-288, 289 et 291 livrent.
