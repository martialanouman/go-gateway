# Le harnais de charge peut envoyer ~480 000 SMS réels sans verrou

> **Statut :** OUVERTE · **Nature :** technique et produit
> **Née de :** revue de step-200 · **Portée par :** step-410 (« à solder ici au plus tard »)

Laissée ouverte pour une raison explicite : ces points « ne coûtent rien tant que la patte sortante
est un simulateur ». La justification expire donc **mécaniquement** au premier run contre un SMSC
réel.

**Ce qu'il en coûte.** Écrit : `make load BASE_URL=<passerelle réelle>` avec une clé valide envoie
« ~500 messages en profil `smoke`, ~480 000 en `sustained`, vers des numéros `+22507000xxxx` — **un
préfixe Orange CI actif ; il n'existe aucune plage réservée aux tests en `+225`** ». Et : « restreindre
le tirage ne suffit pas : c'est l'**envoi** qui doit devenir délibéré ».

**À quoi on reconnaîtra qu'il faut la payer.** Le premier run contre autre chose qu'un simulateur —
c'est-à-dire step-410 elle-même. Une dette dont l'échéance et le porteur sont le même événement est
une dette qui se paie sous pression.

Source : `test/load/README.md:43`
