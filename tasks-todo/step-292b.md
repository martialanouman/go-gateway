# step-292b — Un topic par catégorie, un consommateur par topic, priorité bornée par bind (ADR-0021)

> **Jalon :** ADR-0021 §1-§2, §4-§6 · **Statut :** À FAIRE
> **Dépend de :** step-292, step-287 · **Bloque :** step-409 ; paie
> `debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`
> Née du découpage de step-292 (06/10/2026, ADR-0021 action item 10) ; unité faute de multiple de dix libre.

## Pourquoi
step-292 porte la catégorie et la priorité effective sur `mt.routed`, mais une file unique laisse l'OTP
derrière le backlog marketing, au routeur comme au pool. ADR-0021 sépare les files par catégorie et borne la
priorité par bind.

## Découpage arrêté (step-292, design du 06/10/2026)
- **PR3a** : les topics `mt.routed.<catégorie>` (routeur → pool). Un consommateur par topic et par connecteur
  au pool. Le drainer de parking et `mt-replay` republient selon la catégorie du record. Dans la même PR :
  les groupes du pool dans `deploy/k8s` et le lag par catégorie côté pool.
- **PR3b** : les topics `mt.inbound.<catégorie>`. L'ingestion (REST, SMPP) lit la catégorie dans un
  instantané de `sender_ids` (une lecture, pas une autorisation : ADR-0021 §2). Un consommateur par topic au
  routeur. `mt.inbound` et `mt.routed` sont retirés sans drain (ADR-0021, *Conséquences*). Dans la même PR :
  HPA et groupes du routeur, alerte de lag OTP, spec §5.1, ligne « ordre du pipeline » de `CLAUDE.md`.
- **PR4** : l'ordonnanceur par bind (OTP → transactionnel → marketing) et `CONNECTOR_MARKETING_MIN_SHARE`
  (défaut 0,4). Dans la même PR : spec §6.4 et guide §4.1. Paie la dette.

`routed` passe avant `inbound`, parce que la dette est au pool : c'est là que l'OTP attend au débit plafonné
du SMSC.

## Arbitrages à trancher (dans la fiche, avant tout code)
- Largeurs de départ : 4 pour l'OTP et le transactionnel, 12 pour le marketing (ADR-0021 §1) ; à confronter
  à la mesure de step-287.

## Hors périmètre
Tout ce que step-292 livre (priorité effective, `priority_flag`, CDR, `priority_tier`).
