# step-288 — La catégorie de trafic se déclare par sender ID ; `sender_id_policy` disparaît

> **Jalon :** ADR-0020 §1 · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-289, step-291, step-292 ; l'écran sender IDs du tableau de bord
> (go-gateway-bo step-067)
> Décision humaine du 05/10/2026 ; unité faute de multiple de dix libre.

## Pourquoi
ADR-0020 §1 fait de la catégorie (`otp | transactional | marketing`) une déclaration par expéditeur, et exige
que tout expéditeur soit enregistré, numérique compris. Aujourd'hui, aucune `traffic_category` n'existe, et
`smpp_accounts.sender_id_policy` laisse passer un expéditeur inconnu (`allow_unregistered_numeric`,
`disabled` : `internal/pipeline/senderid/senderid.go:74-98`).

## Design arrêté
- **Schéma** : la migration `0029` ajoute `sender_ids.traffic_category text NOT NULL DEFAULT 'marketing'`
  avec un `CHECK` nommé, et supprime `smpp_accounts.sender_id_policy`. Le `down` remet la colonne à
  `'strict'`. Les deux changements vont aussi dans `db/schema_passerelle_sms.sql`, en même temps.
- **Pas de phase d'expansion** avant la suppression, faute de production (ADR-0021, « Migration »). Sur
  l'environnement de test, la PR inventorie les comptes en `allow_unregistered_numeric` ou `disabled` et
  enregistre leurs expéditeurs **avant** de déployer (ADR-0020, Conséquences).
- **Routeur** : `Authorize` n'a plus de politique. Une ligne `active` du client est exigée, sinon
  `ErrSenderIDNotAuthorized`. L'ordre du pipeline ne change pas. *Amendé à l'implémentation* : l'étape
  ne rend pas encore la catégorie, rien ne la lirait avant step-291, qui l'ajoute avec son consommateur.
- **Contrat Admin** :
  - `SenderId.traffic_category` est toujours présent ;
  - il est optionnel à `create-sender-id` (défaut `marketing`) et modifiable par `update-sender-id` ;
  - le paramètre de requête `traffic_category` filtre `list-sender-ids`. La liste reste un tableau nu,
    puisqu'elle est déjà bornée à un client ;
  - `set-account-sender-id-policy` et `SmppAccount.sender_id_policy` sont retirés.
- **Contrat public** : `sender_id_policy` est retiré du compte (`api/openapi-public.yaml:553,572`, où il est
  requis).
- **Bump MAJEUR 7.0.0** : un seul paquet npm porte les deux contrats. Le tableau de bord n'appelle plus
  l'opération depuis son step-064, mais il lit encore `SmppAccount`, et c'est à confirmer dans la PR.
- Contrats déclarés **avant** l'implémentation (`.claude/rules/contracts-api.md`).

## Tests rouges attendus
- Un expéditeur numérique non enregistré est rejeté (`ErrSenderIDNotAuthorized`), sur un compte qui était
  autrefois en `allow_unregistered_numeric`.
- Une ligne créée sans catégorie vaut `marketing`, et un PATCH du seul statut ne touche pas la catégorie.
  (Que `Authorize` rende la catégorie se teste en step-291.)
- Le filtre `traffic_category` de `list-sender-ids` exclut les autres catégories. La garde de contrat
  ignore les paramètres de requête (`debts/la-garde-de-contrat-ignore-les-parametres-de-requete.md`) : le
  paramètre se prouve donc par un test de handler, pas par la garde.
- `create-sender-id` refuse une catégorie inconnue (422).
- La garde DDL↔enum couvre `TrafficCategory`, comme `enums_test.go` le fait pour les autres enums.

## Hors périmètre
- La priorité effective, `priority_flag_default`, `priority_tier`, les topics par catégorie et le CDR
  `traffic_category` (ADR-0020 §2-§4, ADR-0021) : **step-292**.
- La spec passerelle §6.19 et le glossaire passent par la PR qui livre (ADR-0020, action item 6), pour la
  partie sender ID seulement.
