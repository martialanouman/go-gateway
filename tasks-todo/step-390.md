# step-390 — Réglages de compte créables mais non modifiables, et trois opérations orphelines

> **Jalon :** Surfaces Admin déclarées au contrat, jamais construites (§6.19, §6.22 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** step-320 (triage) · **Bloque :** —

## But

Fermer les 5 dernières opérations de l'écart contrat ↔ implémentation. Deux d'entre elles corrigent un
défaut fonctionnel réel : **un compte créé avec la mauvaise politique ne peut plus être corrigé**.

| Opération | Méthode et chemin |
|---|---|
| `set-account-sender-id-policy` | `PATCH /admin/smpp-accounts/{id}/sender-id-policy` |
| `set-account-smpp-ops` | `PATCH /admin/smpp-accounts/{id}/smpp-ops` |
| `suspend-smpp-account` | `POST /admin/smpp-accounts/{id}/suspend` |
| `reorder-routes` | `POST /admin/routes/reorder` |
| `list-customer-accounts` | `GET /admin/customers/{id}/smpp-accounts` |

## Le constat, opération par opération

- **`sender_id_policy`, `query_sm_enabled`, `cancel_sm_enabled` sont réglables à la création et jamais
  après.** `accountCreateBody` (`internal/adminapi/accounts.go`) les accepte ; `accountUpdateBody` ne
  porte que `name` et `status`. Il n'existe donc **aucun chemin** pour durcir la politique de sender ID
  d'un compte existant (§6.19) ou pour couper `query_sm`/`cancel_sm` (§6.22) — il faut recréer le
  compte, c'est-à-dire changer son identité de bind. C'est le défaut le plus concret des 30.
- **`suspend-smpp-account`** est déclaré au contrat sans handler ; la suspension passe aujourd'hui par
  le PATCH `update-smpp-account` avec `status`, qui porte aussi le déclenchement de la déconnexion
  forcée (step-032). Le point d'entrée dédié existe pour les clients (`suspend-customer`, servi) : les
  deux surfaces doivent se ressembler.
- **`reorder-routes`** : `control_plane.routes.priority` existe (plus bas = évalué d'abord) et se règle
  aujourd'hui une route à la fois, sans opération de réordonnancement atomique.
- **`list-customer-accounts` est un doublon.** `list-smpp-accounts` accepte déjà `?customerId=` **et**
  `?groupId=`. Cette opération n'apporte rien qu'un paramètre de requête ne donne — sauf une seconde
  forme à maintenir.

## Points d'implémentation clés

- **Un changement de politique doit atteindre les sessions vivantes, et les deux réglages n'empruntent
  pas le même chemin.** Vérifié :
  - `query_sm_enabled` / `cancel_sm_enabled` sont lus **au bind** depuis PostgreSQL
    (`internal/storage/postgres/bind_authn.go`) puis **figés dans l'état de session** : seule une
    déconnexion forcée (le mécanisme de step-032) les propage à une session déjà ouverte ;
  - `sender_id_policy` est lu par l'instantané du routeur (`senderid.LoadSnapshot`), rechargé par le
    watcher de config : aucune reconnexion nécessaire.

  Ne pas chercher un « compteur de génération de config » pour ces projections : le seul du dépôt
  (`connector:cfggen:{id}`, step-128) est **par connecteur** et ne les concerne pas. Sans ce câblage, le
  PATCH est vrai en base et faux en production.
- **`suspend-smpp-account` ne doit pas devenir un second chemin de suspension.** L'aligner sur le
  handler existant : même transition de statut, même appel de déconnexion forcée avec motif
  `account_suspended`. Deux chemins qui divergent d'un jour à l'autre valent moins qu'un seul.
- **`reorder-routes` est atomique ou n'est pas.** Un réordonnancement appliqué route par route laisse,
  entre deux écritures, un état où deux routes partagent la même priorité — et le routeur lit un
  instantané pendant ce temps. Une seule transaction, et l'instantané reconstruit après.
- **Le sort de `list-customer-accounts` est une décision explicite**, à prendre en phase d'arbitrage et
  à écrire dans la fiche : soit on la sert (quelques lignes, une forme de plus), soit on la **retire du
  contrat** — auquel cas `oasdiff` classera la rupture `ERR`, ce qui impose un **bump majeur** de
  `api/package.json` et une note au dépôt du tableau de bord, qui consomme le package npm. Ne pas
  trancher en silence.

## Tests

- Une politique de sender ID durcie **refuse** un `submit_sm` que l'ancienne acceptait, sur une session
  déjà ouverte. C'est la seule assertion qui prouve que le changement a traversé ; un test qui relit la
  colonne ne prouve que l'écriture.
- `cancel_sm` coupé → la commande est refusée avec le code du contrat (§6.22).
- `reorder-routes` : l'ordre résultant est celui demandé, et aucune lecture concurrente ne voit deux
  routes à la même priorité.
- `suspend-smpp-account` : les binds du compte tombent, avec le motif attendu.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] les changements de politique atteignent les sessions vivantes, prouvé par un test de bout en bout
- [ ] le sort de `list-customer-accounts` tranché et écrit ; bump majeur si retrait du contrat
- [ ] `api/collections/admin-api.yaml` synchronisée ; les 5 lignes retirées de `deferred` (step-320)

## Hors périmètre

L'autorisation de sender ID elle-même (§6.19, livrée en step-060) et les bascules `query_sm`/`cancel_sm`
côté protocole (livrées en step-025/030). Cette fiche ne fait que rendre leurs réglages modifiables.
