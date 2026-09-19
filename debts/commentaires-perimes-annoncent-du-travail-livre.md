# Trois paquets annoncent comme « à venir » du travail déjà livré

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** dérive naturelle · **Portée par :** —

`internal/content/aead.go` dit « that wiring lands in later M10 steps » alors que `ingest/content.go`
appelle déjà `content.SealBody`. `internal/pipeline/optout/optout.go` renvoie les écritures STOP et le
rechargement à chaud à « step-063, M7 » — step-063 est livrée, et les deux existent.
`internal/pipeline/ratelimit/ratelimit.go` renvoie à step-085, livrée elle aussi.

**Ce qu'il en coûte.** Dans un dépôt dont les commentaires **sont** la documentation d'architecture —
il n'y a aucun `TODO`, aucun `FIXME` — un commentaire périmé est pire qu'absent : il fait douter un
lecteur de l'existence d'une fonctionnalité livrée, et un agent qui grep « arrive later » la
réimplémente.

**À quoi on reconnaîtra qu'il faut la payer.** Elle se paie à la lecture, une ligne à la fois, par
qui passe à côté. Ce fichier existe pour que le passage soit délibéré.

Sources : `internal/content/aead.go:6` · `internal/pipeline/optout/optout.go:6` · `internal/pipeline/ratelimit/ratelimit.go:7`
