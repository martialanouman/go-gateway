# step-294 — `priority_flag_default` modifiable à l'Admin API

> **Jalon :** ADR-0020 §3 · **Statut :** LIVRÉE
> **Dépend de :** step-292 (PR1, #273) · **Bloque :** —
> Paie `debts/defauts-smpp-du-connecteur-non-modifiables.md`. Demande humaine du 06/10/2026 ; unité faute
> de multiple de dix libre.

## Pourquoi
Depuis step-292, le pool envoie `priority_flag_default` pour un message de priorité effective 0. Mais aucune
opération Admin ne l'écrit : seul le 0 du schéma est atteignable, sauf à passer par SQL.

## Design arrêté (06/10/2026)
L'arbitrage a été rendu par Fable, sans contradiction avec la spec.
- **Seul `priority_flag_default` devient modifiable**, dans `ConnectorCreate` et `ConnectorUpdate`
  (optionnel, `minimum: 0`, `maximum: 3`), puis dans `cp.ConnectorPatch`, `cp.NewConnector` et le store.
  Contrat MINEUR.
- **Les autres `*_default` restent en lecture seule.** Aucun code d'exécution ne les lit : ouvrir leur
  écriture promettrait un effet qui n'existe pas. Leur description dans `Connector` le dit. Ce n'est pas une
  dette, puisque rien n'est différé : les faire envoyer par le pool serait une fonctionnalité, avec son ADR.
- **`CHECK (priority_flag_default BETWEEN 0 AND 3)`**, dans le schéma et dans la migration 0033. Il ferme la
  voie SQL, la seule ouverte jusqu'ici.
- **Pas de `SignalReconfigure` à la mise à jour.** `update()` n'en émet pour aucun champ, pas même pour
  l'hôte ou le mot de passe. La valeur est relue au prochain (re)dial, et `rebind-connector` l'applique tout
  de suite. La description du champ le dit.
- `priority_tier` n'a ni bornes ni `CHECK` non plus. Ce point va à la PR2 de step-292, qui définit sa
  sémantique.

## Definition of Done
- [x] contrat déclaré avant le handler, bump MINEUR
- [x] création et mise à jour écrivent la valeur ; 4 refusé en 422 ; le `CHECK` refuse 4 en SQL
- [x] fiche de dette passée en PAYÉE
