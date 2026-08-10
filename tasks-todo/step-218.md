# step-218 — Politiques de stockage de contenu (§6.23) : la plateforme n'a pas de défaut configurable

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-213 (triage) · **Bloque :** —

## But

Servir les 4 opérations de politique de contenu. Deux sont une surface de lecture/écriture sur une
colonne existante ; deux exigent une **décision de conception** que step-162 avait explicitement
différée.

| Opération | Méthode et chemin |
|---|---|
| `get-customer-content-policy` | `GET /admin/customers/{id}/content-policy` |
| `update-customer-content-policy` | `PATCH /admin/customers/{id}/content-policy` |
| `get-platform-content-policy` | `GET /admin/platform/content-policy` |
| `update-platform-content-policy` | `PATCH /admin/platform/content-policy` |

## Le constat

Côté client, tout existe : `customers.content_storage` (`off` / `stored_plaintext` /
`stored_encrypted` / `inherit`), le scellement à la ligne CDR `accepted` (step-162), la lecture gardée
et auditée (step-163), le crypto-shred (step-164). Seul l'endpoint dédié manque.

Côté plateforme, **rien n'existe** : `inherit` est aujourd'hui résolu vers une **constante** `off`
(défaut conservateur choisi en step-162, faute de politique de plateforme). Il n'y a ni table, ni ligne
de configuration. Les deux opérations `platform` ne peuvent donc pas être servies sans créer d'abord le
support de cette valeur.

## Points d'implémentation clés

- **Le défaut conservateur ne change pas par accident.** Aujourd'hui `inherit → off` : aucun corps de
  message n'est stocké sans opt-in explicite. Rendre la valeur configurable crée la possibilité qu'un
  seul PATCH bascule *tous* les clients en `inherit` vers du stockage. Écrire ce que vaut la politique à
  la migration (`off`, identique au comportement actuel) et interdire que la migration elle-même change
  la sémantique observable.
- **Où vit la valeur plateforme.** Une table de configuration à une ligne, ou une clé de configuration
  générale — trancher en phase d'arbitrage, avec la spec d'abord : §6.23 ne prescrit pas de support. Le
  choix a une conséquence concrète : le plan de données lit la politique par un **instantané de boot**
  (`content.PolicySnapshot`), donc la valeur plateforme doit arriver par le même chemin, sous peine
  d'être lue à des instants différents selon les pods.
- **Le rechargement à chaud est déjà câblé pour la politique CLIENT — n'en construis pas un second.**
  `content.PolicyHolder` tient l'instantané derrière un pointeur atomique précisément « so the data
  plane can HOT-RELOAD the content-storage policy on a config change without a restart », le routeur le
  recharge à chaque invalidation, et le middleware admin publie l'événement sur **toute** mutation
  réussie. Une mise à jour de politique client devient donc effective sans redémarrage, et le test doit
  le vérifier plutôt que documenter une fenêtre. Seule la **valeur plateforme** est neuve : c'est elle
  qui doit rejoindre ce chemin, faute de quoi elle serait lue à des instants différents selon les pods.
- **Ne jamais dégrader vers le clair.** La règle de step-162 tient : en cas d'indisponibilité du service
  de clés, on écrit la ligne CDR **sans contenu** et on incrémente `content_dropped` — jamais un repli
  en clair. Une surface d'administration ne doit pas ouvrir un chemin qui contourne cette règle.

## Tests

- Les trois valeurs de politique client sont servies et relues ; un changement est **effectif** sur la
  ligne CDR suivante (assertion sur le contenu scellé/absent, pas sur la colonne de configuration).
- Le défaut plateforme vaut `off` après migration : test de non-régression sur un client `inherit`, dont
  le corps ne doit toujours pas être stocké.
- Invariant (a) rejoué sous les trois modes, comme en step-162 : le corps ne fuit dans aucune
  sérialisation, quelle que soit la politique.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les 4 opérations servies ; le défaut plateforme = `off`, sans changement de comportement observable
- [ ] fenêtre de propagation câblée ou documentée ; invariant (a) vert sous les trois modes
- [ ] `db/schema_passerelle_sms.sql` **et** la migration `golang-migrate` si une table est ajoutée
- [ ] `api/collections/admin-api.yaml` synchronisée ; lignes retirées de `deferred` (step-213)

## Hors périmètre

Le chiffrement lui-même, la lecture gardée, le crypto-shred et la rétention (step-162 à 165, livrés).
