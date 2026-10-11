# La politique de contenu du CDR MO n'est relue qu'au démarrage

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-402 (arbitrage Fable) · **Portée par :** —

**Ce qu'on a fait à la place.** mo-dlr-router-svc scelle le corps de chaque ligne CDR MO selon une
politique de contenu chargée une seule fois au démarrage (`cmd/mo-dlr-router-svc/wiring.go:582`).
router-svc, lui, la recharge à chaque invalidation de configuration (step-102).

**Pourquoi.** Le service n'a aucun watcher de configuration. Sa table de routage et ses mots-clés STOP
sont eux aussi chargés à froid. Importer le watcher dépassait la PR qui écrivait la première ligne MO.

**Ce qu'il en coûte si on ne la paie jamais.** Un client qui passe de `stored_*` à `off` continue de
voir le corps de ses MO stocké jusqu'au prochain redémarrage du pod. C'est un engagement de
confidentialité tenu en retard. L'inverse, un corps attendu mais absent, est moins grave.

**À quoi on reconnaîtra qu'il faut la payer.** Quand mo-dlr-router-svc recevra un watcher pour sa
table de routage, ou à la première plainte sur un MO conservé après un passage à `off`.
