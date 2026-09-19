# Le retry différé des webhooks n'honore pas le back-off de la politique

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-192, relevée par step-340 · **Portée par :** — (step-340 la nomme sans la porter)

Sur le chemin différé, seul `max_attempts` traverse : le runner « ne pace que sur ses propres
constantes », et la politique n'est consultée que pour décider de l'épuisement.

**Ce qu'il en coûte.** Écrit : « une surface qui laisse configurer `initial_backoff_ms` sans que le
retry différé s'en serve **promet un réglage inerte** ». Le jour où step-340 livrera l'administration
des webhooks, elle exposera un champ que le chemin principal ignore.

**À quoi on reconnaîtra qu'il faut la payer.** À l'implémentation de step-340 : soit le runner honore
la politique, soit la surface documente que ce réglage ne vaut pas sur le chemin différé. L'alternative
est écrite ; le choix ne l'est pas.

Source : `internal/webhook/retry.go:79`
