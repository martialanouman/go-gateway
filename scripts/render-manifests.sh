#!/usr/bin/env bash
#
# Rend les manifests de deploy/k8s au tag publié, sur la sortie standard.
#
# deploy/k8s porte le gabarit :v0.0.0 partout (step-270). Ni kustomize ni Helm : la substitution est
# un sed, et ce script existe pour qu'elle ne soit pas un sed tapé de mémoire dans un terminal. Il
# VÉRIFIE SA PROPRE SORTIE avant de la rendre — un rendu partiel, où un service resterait sur le
# gabarit pendant que les onze autres avancent, ne peut pas atteindre kubectl.
#
# Usage : scripts/render-manifests.sh v1.4.2 | kubectl apply -f -

set -euo pipefail

VERSION="${1:-}"
PREFIX="ghcr.io/martialanouman/go-gateway"
PLACEHOLDER="v0.0.0"
DIR="${MANIFEST_DIR:-deploy/k8s}"

if [[ -z "$VERSION" ]]; then
  echo "usage: $0 <version>   (ex. $0 v1.4.2)" >&2
  exit 2
fi

# Le tag publié par GoReleaser est {{ .Tag }} : vX.Y.Z, avec le v. Refuser tout le reste ici plutôt
# que de laisser kubectl tirer une image qui n'existe pas.
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "$0: version %q invalide : ${VERSION} — attendu vX.Y.Z (le v compris)" >&2
  exit 2
fi

rendered=$(
  find "$DIR" -type f \( -name '*.yaml' -o -name '*.yml' \) -print0 \
    | sort -z \
    | while IFS= read -r -d '' f; do
        echo "---"
        sed -E "s|(${PREFIX}/[a-z0-9-]+):${PLACEHOLDER}|\1:${VERSION}|g" "$f"
      done
)

# La vérification qui fait de ce script autre chose qu'un alias. Si un manifeste porte une version
# figée à la main, ou si le motif a cessé de correspondre, le gabarit survit au rendu et le service
# concerné se déploierait sur une version périmée, en silence.
if grep -q "${PREFIX}/[a-z0-9-]*:${PLACEHOLDER}" <<<"$rendered"; then
  echo "$0: le gabarit ${PLACEHOLDER} a survécu au rendu :" >&2
  grep -n "${PREFIX}/[a-z0-9-]*:${PLACEHOLDER}" <<<"$rendered" >&2
  exit 1
fi

if ! grep -q "${PREFIX}/[a-z0-9-]*:${VERSION}" <<<"$rendered"; then
  echo "$0: aucune image ne porte ${VERSION} — la substitution n'a rien touché" >&2
  exit 1
fi

printf '%s\n' "$rendered"
