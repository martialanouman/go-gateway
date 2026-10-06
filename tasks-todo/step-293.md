# step-293 — La catégorie du CDR servie à l'Admin, et portée par les rejets

> **Jalon :** ADR-0020 · **Statut :** EN COURS
> **Dépend de :** step-292 (PR1, #273), step-294 · **Bloque :** l'écran CDR Explorer (§6.4 du tableau de bord)
> Paie `debts/categorie-du-cdr-ni-servie-ni-sur-les-rejets.md`. Demande humaine du 06/10/2026 ; unité
> faute de multiple de dix libre.

## Design arrêté (06/10/2026)
L'arbitrage a été rendu par Fable, sans contradiction avec la spec.
- **Surfaces : `search-messages` et l'export Admin, rien d'autre.**
  - `messageSummaryDTO` gagne `traffic_category`, en enum nullable : `null` désigne un rejet antérieur à
    l'autorisation. Il gagne aussi `priority`. L'export écrit ce même DTO.
  - Un filtre `traffic_category` est ajouté sur `search-messages` et sur les filtres d'export
    (`CDRSearchFilter`, `WHERE`).
  - Ni l'API publique ni le flux temps réel ne sont concernés : aucun écran ne les lit, et le flux reste
    porté par ADR-0020, action item 8.
  - Contrat MINEUR. La spec du tableau de bord, §6.4, gagne le filtre « catégorie ».
- **Rejets** : `Pipeline.Process` renvoie `out` au lieu de `RoutedMT{}` sur les chemins d'erreur
  postérieurs à l'étape 2 (opt-out, anti-spam, route, crédit). La doc de `Process` le dit : en erreur, le
  gabarit ne porte que ce qui est connu jusqu'à l'étape fautive. `rejectedRow` reçoit la catégorie et la
  priorité. Pas d'erreur typée.

## Definition of Done
- [x] un rejet `category_mismatch` en `block` se retrouve par le filtre de catégorie : prouvé par maillons
  (rejet anti-spam au pipeline, ligne `rejected` au routeur, filtre ClickHouse sur placeholder + rejet), sans
  test de bout en bout
- [x] fiche de dette passée en PAYÉE
