# step-291 — Le trafic d'un expéditeur est contrôlé contre sa catégorie déclarée ; les signalements se comptent

> **Jalon :** ADR-0020 §5 · **Statut :** À FAIRE
> **Dépend de :** step-288 · **Bloque :** le compteur de signalements de l'écran sender IDs du tableau de bord
> (go-gateway-bo step-067)
> Décision humaine du 05/10/2026 (métrique + compteur Redis) ; unité faute de multiple de dix libre.

## Pourquoi
ADR-0020 §5 : une déclaration qui ne se vérifie pas n'engage personne. Aujourd'hui, un `flag` anti-spam ne
laisse **aucune trace** au-delà d'un attribut de span (`internal/pipeline/pipeline.go:203`). Il n'y a ni
compteur, ni métrique, ni stockage, et le tableau de bord n'a rien à afficher.

## Design arrêté
- **Règle `category_mismatch`** : un nouveau `rule_type` (le `CHECK` est étendu par migration, en même temps
  que le schéma). Les portées sont `global`, `customer` et `smpp_account`. Elle s'évalue en mémoire, comme
  les règles de contenu, avant la sortie anticipée de `Evaluate`. La catégorie vient de l'étape sender ID
  (step-288 la stocke ; c'est step-291 qui la fait rendre par `Authorize` et la passe à l'anti-spam). Les
  paramètres, validés par `ValidateRuleConfig` :
  - `otp` : un code de `min_digits` à `max_digits` chiffres (4 et 8 par défaut), aucune URL, au plus
    `max_length` caractères ;
  - `transactional` : aucun des `promo_markers`, une liste configurable ;
  - `marketing` : rien à vérifier.
- **Action** : `flag` par défaut, `block` configurable. *Amendé le 06/10/2026* : un schéma OpenAPI simple
  ne porte pas de défaut propre à un seul `rule_type`, donc `action` sort de `required` avec
  `default: flag` **pour tous les types**. C'est un relâchement, pas une rupture : une action omise était
  un 422, elle devient `flag`, l'action la moins sévère.
- **Rien du corps ne sort du moteur** (invariant a) : ni log, ni métrique, ni span ne porte un extrait ou
  un marqueur trouvé. *Amendé le 06/10/2026* : le verdict ne porte pas l'identifiant de règle. L'ADR
  borne ce qui peut sortir (« seuls l'identifiant de règle et le verdict »), il n'exige pas de l'émettre ;
  le porter refondrait `Evaluate` pour tous les types sans aucun consommateur.
- **Métrique** `anti_spam_category_mismatch_total{action}` (`flag` ou `block`), sans label d'expéditeur ni
  de client (garde des labels), nommée comme sa voisine `anti_spam_fail_open_total`. *Amendé le
  06/10/2026* : le design prévoyait `antispam_verdicts_total{rule_type, action}` sur toutes les règles ;
  compter par règle exigerait de suivre quelle règle décide dans chaque évaluateur, et la spec du tableau
  de bord ne parle que des signalements `category_mismatch`. Alertmanager s'en sert, et l'alerte persiste
  côté tableau de bord dans `notifications` : c'est là qu'une alerte ratée par un opérateur reste non
  lue.
- **Compteur par sender ID** : chaque correspondance `category_mismatch` (en `flag` comme en `block`) fait
  un `INCR` sur une tranche horaire Redis, clé `(customer_id, address)` + heure, TTL 25 h. L'écriture est
  en fail-open : une panne Redis ne retient pas le message. *Amendé le 06/10/2026* : le design comptait
  tout `flag`, quelle que soit la règle ; la spec du tableau de bord v2.1 (§6.19, go-gateway-bo) demande
  un « compteur de signalements `category_mismatch` récents par expéditeur ». La spec l'emporte.
- **Contrat Admin, bump MINEUR** : `SenderId.recent_category_mismatches_24h` est la somme des 24 dernières
  tranches,
  servie par `list-sender-ids` en **un seul `MGET`** pour toute la liste. La fenêtre de 24 h glissantes est
  déclarée dans le nom et la description. `null` si Redis ne répond pas : le compte est inconnu, pas nul.
- **Rechargement à chaud des règles** : le moteur anti-spam est chargé une seule fois au démarrage du
  routeur (`cmd/router-svc/wiring.go:375-379`). Il rejoint le watcher de snapshot du routeur, sans quoi
  une règle `category_mismatch` créée au tableau de bord ne s'appliquerait qu'au prochain redémarrage.

## Vérifié avant le contrat (06/10/2026)
La spec v2.1 du tableau de bord (go-gateway-bo, `1c29fa6`) ne nomme aucune métrique anti-spam dans
`alert_rules` : le nom `antispam_verdicts_total` est libre.

## Tests rouges attendus
- Un OTP déclaré qui porte une URL est signalé. Le même corps envoyé sous un expéditeur `marketing` ne
  l'est pas.
- Un transactionnel qui porte un marqueur promotionnel est signalé. Avec `block`, il est rejeté
  (`ErrContentBlocked`).
- **Invariant a** : les logs capturés du moteur, les attributs de span et les labels de métrique ne
  contiennent aucun extrait du corps ni aucun marqueur trouvé. `AssertNoBody` est ajouté à
  `TestPipelineSpamFlagDoesNotBlock`, qui ne l'appelle pas aujourd'hui.
- `recent_category_mismatches_24h` compte une correspondance de l'heure courante et oublie une tranche
  de plus de 24 h ; un `flag` d'une autre règle n'y entre pas.
- Une règle créée par l'API Admin s'applique sans redémarrage du routeur.

## Dettes ouvertes par ce design
- `debts/file-de-revue-antispam-inexistante.md` : le compteur n'a pas de file de revue vers laquelle
  pointer.
- `debts/signalements-n-alimentent-pas-la-reputation.md` : ADR-0020 §5 dit que le flag alimente la
  réputation, et personne n'écrit `antispam:rep:`.
