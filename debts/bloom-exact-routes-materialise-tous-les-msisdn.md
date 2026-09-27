# Le rebuild du Bloom `exact_routes` matérialise tous les MSISDN avant de dimensionner le filtre

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-399 · **Portée par :** —

**Ce qu'on a fait à la place.** `buildFilter` (`internal/routing/exact/bloom.go:77`) pagine la table par
1 000, accumule chaque MSISDN dans un `[]string`, puis dimensionne et remplit le filtre d'un coup. Depuis
step-399, ce rebuild n'est plus seulement événementiel : chaque pod routeur le refait toutes les
`CONFIG_RESYNC_INTERVAL` (5 min).

**Pourquoi.** Le filtre se dimensionne sur le nombre d'entrées ; les lire d'abord est la manière la plus
simple de le connaître. Compter puis streamer dans le filtre demande une requête de plus et un second
parcours, sans bénéfice tant que la table reste petite. step-399 ne touchait pas au Bloom (arbitrage Fable,
2026-09-27).

**Ce qu'il en coûte.** Pour 5 M de numéros portés, de l'ordre de 150 Mo transitoires par rebuild et par pod,
soit une rafale d'allocation et de GC toutes les 5 min sur un pod qui route. La gigue de la resync décorrèle
les pods entre eux, elle ne réduit pas la rafale.

**À quoi on reconnaîtra qu'il faut la payer.** Une pause GC ou un pic de latence du routeur qui revient à la
période de resync, ou un pic de RSS du pod qui approche sa limite mémoire au moment des rebuilds.

Source : `internal/routing/exact/bloom.go:77` (`buildFilter`)
