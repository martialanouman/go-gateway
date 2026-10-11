# Le corps révélé par l'Admin n'est pas décodé selon son codage

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** revue de step-402 · **Portée par :** —

**Ce qu'on a fait à la place.** La lecture auditée du contenu rend les octets stockés tels quels, en
`string(plain)` (`internal/adminapi/messages.go`, `get-message-content`). Un corps stocké dans son codage
SMPP s'affiche donc faux : UTF-16BE pour `ucs2`, septets non empaquetés pour `gsm7`. Depuis step-402,
c'est le cas de chaque MO, car `projectMORouted` scelle les octets du `deliver_sm`. C'était déjà le cas
d'un MT soumis en SMPP UCS2.

**Pourquoi.** step-402 payait l'absence de ligne CDR MO. Décoder à la lecture touche une autre surface,
celle du contenu audité, avec ses propres tests. Et le défaut existait avant pour le MT.

**Ce qu'il en coûte si on ne la paie jamais.** Un opérateur qui révèle un MO en arabe, ou portant un
emoji, lit des caractères de remplacement. En GSM7, `@` et les lettres accentuées sont faux.

**À quoi on reconnaîtra qu'il faut la payer.** À la première révélation d'un MO non ASCII. Le DTO porte
déjà `encoding` : c'est lui qui doit choisir le décodeur.
