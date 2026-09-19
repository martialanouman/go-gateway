# L'intégration réelle du fournisseur de facturation externe est « optionnelle »

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** `docs/plan-execution-passerelle.md:462` · **Portée par :** —

« Hors périmètre : pas de tarification temps réel via un fournisseur externe synchrone en prod
(**l'adaptateur existe, l'intégration réelle est optionnelle**). » Aucune raison n'est donnée au-delà
du mot « optionnelle ».

**Ce qu'il en coûte.** Non écrit ici — mais écrit ailleurs : c'est le pendant de la sonde HTTP réelle
qui doit sortir `test-billing-provider` des suffixes de lecture. Les deux sont la même dette vue de
deux côtés, et **ni l'une ni l'autre n'a de fiche**.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier client tarifé par un fournisseur externe.
Lire alors, ensemble, ce fichier et celui de la sonde.

Source : `internal/adminapi/billing_admin.go:393` (le stub)
