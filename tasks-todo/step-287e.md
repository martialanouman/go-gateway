# step-287e — Chronomètres du chemin chaud : facturation durable, pipeline du routeur, règlement

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-287d · **Bloque :** step-287 (reprise de la campagne)
> Demande humaine du 07/10/2026 : « instrumente RecordDurable et tout composant critique le temps du
> développement » ; unité faute de multiple de dix libre.

## Pourquoi
Run 2 de step-287 (VPS de test, `1df24e2`, 07/10/2026) : le pool envoie ~275 `submit_sm`/s et n'est plus le
goulot (lag de 245). Le routeur passe **226 ms par message**, dont 218 ms de réservation, dont **185 ms
d'écriture durable** dans billing-svc. Le consommateur `billing-svc-settle` règle environ 85 messages/s,
pour 275 publiés. Le `BillingBatcher` n'a qu'un écrivain, et chaque lot fait quatre allers-retours Postgres
en série. Aucune métrique ne dit où partent ces 185 ms.

## Design arrêté (07/10/2026)
On suit le patron de step-287c : des histogrammes par étape à vocabulaire fermé, actifs en permanence, sans
span neuf. Le label `stage` est déjà autorisé par la garde.

- **`billing_durable_stage_seconds{stage}`**, dans billing-svc, alimenté par le `BillingBatcher` :
  - par entrée : `handoff` (attente pour être pris par l'écrivain, donc le lot précédent qui s'écrit) et
    `reply` (de la prise en charge à la réponse : l'écriture de son lot) ;
  - par lot : `begin`, `claim`, `copy`, `commit`.
- **`pipeline_stage_seconds{stage}`**, dans le catalogue, exposé par le routeur : chaque étape de
  `Pipeline.Process`, mesurée dans `Pipeline.stage`, le point unique par lequel elles passent toutes. Le nom
  d'étape est celui du span, sans le préfixe `pipeline.` : `e164`, `sender_id`, `opt_out`, `anti_spam`,
  `route`, `encoding`, `segment`, `credit`.
- **`billing_settle_seconds{action}`**, dans billing-svc : la durée d'un règlement du consommateur
  `billing-svc-settle` (`capture` ou `release`). Elle couvre le verrou terminal, les lectures et l'écriture
  durable.
- Un observateur nil n'observe rien : les tests et les binaires qui ne le câblent pas ne changent pas.

## Definition of Done
- [ ] chaque étape est observée (tests, mutation) et exposée par son service (tests d'exposition)
- [ ] un run de 10 min sur le VPS, avec la répartition relevée dans le journal de step-287 et le goulot de
      la facturation durable nommé
