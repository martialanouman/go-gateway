#!/usr/bin/env bash
#
# Garde de cohérence entre les contrats OpenAPI et la version du package publié
# (@martialanouman/gateway-api-contracts, voir api/README.md).
#
# Le tableau de bord Admin vit dans un dépôt séparé et consomme ce package. Deux façons de le casser
# en silence : modifier un contrat sans bumper la version (le consommateur ne voit jamais passer le
# changement), ou introduire une rupture sous un bump mineur (le consommateur l'installe sans le
# savoir). Ce script rend les deux impossibles. Il n'interdit pas les ruptures — il exige qu'elles
# soient déclarées par un bump MAJEUR.
#
# Usage : scripts/check-contracts.sh          (compare à origin/main)
#         BASE_REF=origin/dev scripts/... (compare à une autre base)

set -euo pipefail

BASE_REF="${BASE_REF:-origin/main}"
OASDIFF="${OASDIFF:-oasdiff}"
PKG="api/package.json"
CONTRACTS=(api/openapi-admin.yaml api/openapi-public.yaml)

fail() {
	echo "::error::$*" >&2
	exit 1
}

command -v jq >/dev/null || fail "jq est requis (brew install jq · apt-get install -y jq)."
command -v "$OASDIFF" >/dev/null || fail "oasdiff est requis — lance 'make tools'."
git rev-parse --verify --quiet "$BASE_REF" >/dev/null ||
	fail "Base '$BASE_REF' introuvable. Lance 'git fetch origin main', ou passe BASE_REF=<ref>."

# 1. Quels contrats ont bougé ? Si aucun, il n'y a rien à garder.
changed=()
for contract in "${CONTRACTS[@]}"; do
	git diff --quiet "$BASE_REF" -- "$contract" || changed+=("$contract")
done

if [[ ${#changed[@]} -eq 0 ]]; then
	echo "Aucun contrat modifié depuis $BASE_REF — rien à vérifier."
	exit 0
fi
echo "Contrats modifiés depuis $BASE_REF : ${changed[*]}"

# 2. La PR qui introduit le package n'a pas de version antérieure à comparer.
if ! base_pkg=$(git show "$BASE_REF:$PKG" 2>/dev/null); then
	echo "$PKG absent de $BASE_REF — PR d'introduction du package, garde de version inapplicable."
	exit 0
fi

base_version=$(jq -er .version <<<"$base_pkg")
head_version=$(jq -er .version "$PKG")

[[ "$base_version" != "$head_version" ]] ||
	fail "Contrat modifié sans bump de version dans $PKG (toujours $base_version) — voir api/README.md."

newest=$(printf '%s\n%s\n' "$base_version" "$head_version" | sort -V | tail -1)
[[ "$newest" == "$head_version" ]] ||
	fail "Version en régression dans $PKG : $base_version → $head_version."

# 3. Niveau de bump. Un package déjà publié ne se dépublie pas : le niveau doit être juste du
#    premier coup, d'où la vérification ci-dessous plutôt qu'une correction après merge.
IFS=. read -r base_major base_minor _ <<<"$base_version"
IFS=. read -r head_major head_minor _ <<<"$head_version"
if ((head_major > base_major)); then
	bump=majeur
elif ((head_minor > base_minor)); then
	bump=mineur
else
	bump=correctif
fi
echo "Bump de version : $base_version → $head_version ($bump)"

# 4. Ruptures. oasdiff sort non-zéro dès qu'il classe un changement ERR ; il sort non-zéro aussi s'il
#    n'arrive pas à lire un document, et on traite les deux pareil — bloquer à tort coûte une lecture
#    de log, laisser passer une rupture coûte un dépôt consommateur cassé.
breaking=0
for contract in "${changed[@]}"; do
	echo "── oasdiff breaking : $contract"
	"$OASDIFF" breaking --fail-on ERR "$BASE_REF:$contract" "$contract" || breaking=1
done

if ((breaking)) && [[ "$bump" != majeur ]]; then
	fail "Rupture détectée sous un bump $bump ($base_version → $head_version) — une rupture exige un bump MAJEUR."
fi

if ((breaking)); then
	echo "Ruptures détectées, déclarées par un bump majeur — OK."
else
	echo "Aucune rupture — OK."
fi
