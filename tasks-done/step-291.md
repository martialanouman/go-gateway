# step-291 — Le trafic d'un expéditeur est contrôlé contre sa catégorie déclarée ; les signalements se comptent

> **Jalon :** ADR-0020 §5 · **Statut :** LIVRÉE
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

## Livré
- Contrat 7.3.0 (mineure) : `category_mismatch` dans `rule_type`, `action` optionnelle (`default: flag`),
  `config_json` documenté pour ce type, `SenderId.recent_category_mismatches_24h` (entier ≥ 0 ou `null`).
- Migration 0032 (0031 était prise par #267) : `category_mismatch` dans le `CHECK` ; le défaut d'`action` est posé par l'API seule.
- `Authorize` rend la catégorie, le pipeline la passe à `Evaluate` ; la règle tourne en mémoire, à côté des
  règles de contenu, et compte chaque correspondance (Redis, tranches horaires, TTL 25 h, fail-open) et la
  métrique `anti_spam_category_mismatch_total{action}`.
- Le moteur anti-spam se recharge sur l'invalidation de configuration (`antispam.Holder`).
- `list-sender-ids` lit les compteurs de toute la liste en un seul `MGET` ; une lecture en échec sert `null`
  sans faire échouer la liste.
- Mutations tuées : marketing vérifié, URL ignorée, longueur ignorée, marqueurs sensibles à la casse,
  correspondance non comptée, métrique absente, extrait du corps journalisé, catégorie perdue à
  l'autorisation puis au pipeline, rechargement du moteur absent, fenêtre de 25 tranches, tranche sans
  TTL, compteur ignoré par l'Admin, zéro servi au lieu de `null`, défaut d'action à `block`.
- Revue (06/10/2026) :
  - un `otp_code_max_digits` au-delà de 1000 faisait paniquer la regex, donc l'Admin (500) et le routeur au
    démarrage : plafond à 32, une ligne hors borne est écartée par le moteur ;
  - le compteur et la métrique comptent chaque message **une fois** (marqueur par `message_id` dans le même
    script Lua), comme les règles duplicate et velocity : un rejeu ne gonfle plus l'alerte ;
  - un lien sans schéma (`bit.ly/x9`) est détecté ; un code écrit `123 456` ou `123-456` n'est plus
    signalé ; une clé de config inconnue est refusée ;
  - l'horloge du compteur est injectable : le test ne chevauche plus un changement d'heure ;
  - la fenêtre est décrite telle qu'elle est (l'heure en cours et les 23 précédentes) ;
  - le `DEFAULT 'flag'` en base, jamais atteint, est retiré : l'API pose le défaut ; `AntispamRuleType.Valid`,
    sans appelant, est supprimée ;
  - le moteur se recharge en dernier, pour qu'une table de règles illisible ne retienne pas la politique de
    contenu.
- Laissés de côté, sans effet aujourd'hui : `throttle` accepté pour ce type (aucune conséquence au-delà d'un
  label borné), la garde d'un `Holder` vide (toujours rempli au câblage), la factorisation des quatre
  résolutions de portée (code existant).
