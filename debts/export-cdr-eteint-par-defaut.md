# L'export CDR répond 503 par défaut : la capacité est au contrat, éteinte en pratique

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-187 · **Portée par :** —

Le choix est bon et écrit : `EXPORT_DIR` vide « means the deployment has no export storage, and
create-message-export answers 503 rather than queueing a job nothing can fulfil » — mieux vaut un
refus net que remplir en silence le disque d'un pod. Le problème est ailleurs : **aucun manifeste de
`deploy/k8s` ne pose `EXPORT_DIR`**, donc le défaut est la configuration livrée.

**Ce qu'il en coûte.** Un opérateur détenant le scope `cdr:export_bulk` — un scope conçu pour un rôle
d'investigation — voit l'opération au contrat et reçoit un 503 sans explication actionnable.

**À quoi on reconnaîtra qu'il faut la payer.** La première investigation qui a besoin d'un export.
step-187 est livrée ; la mise en service de son stockage n'a aucune fiche.

Sources : `internal/config/config.go:416` · `deploy/k8s/` (absence)
