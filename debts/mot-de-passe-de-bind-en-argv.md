# Le mot de passe de bind transite par `argv`, dans deux binaires

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** revue de step-200 · **Portée par :** step-410

Même raison que le harnais de charge : « ne coûtent rien tant que la patte sortante est un
simulateur ». La seconde occurrence a été **ajoutée après** le constat, en step-201 — la dette a donc
grandi pendant qu'elle attendait.

**Ce qu'il en coûte.** Écrit : « visible dans `ps` pour tout utilisateur de la machine pendant tout le
run, et dans l'historique du shell ».

**À quoi on reconnaîtra qu'il faut la payer.** Le premier usage contre un SMSC réel, avec un vrai
secret. Le correctif est écrit : « le lire dans l'environnement, flag conservé en repli documenté
comme non sûr — **les deux binaires** ».

Sources : `cmd/smpp-bindgen/main.go:69` · `cmd/smsc-ceiling/main.go:103`
