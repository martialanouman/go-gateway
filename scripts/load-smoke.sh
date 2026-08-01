#!/usr/bin/env bash
#
# Fumigation du harnais de charge : prouve que les seuils k6 sont câblés (step-200 D3).
#
# Un run de charge contre un stub local passe TRIVIALEMENT — le stub répond en sub-milliseconde,
# n'importe quel budget de latence est respecté. Un tel run vert ne prouve donc rien. La preuve est
# le couple : le même script doit ÉCHOUER contre un stub délibérément ralenti au-dessus du budget.
# Si les deux runs passent, les seuils ne sont pas branchés et le harnais est décoratif.
#
# Ce script ne mesure pas les NFR — il ne dit rien de la tenue à 8 000 req/s, qui se vérifie sur
# matériel réel (step-201). Il vérifie l'instrument, pas le système.
#
# Usage : scripts/load-smoke.sh

set -euo pipefail

cd "$(dirname "$0")/.."

K6="${K6:-k6}"
SCRIPT="test/load/k6/messages.js"
ADDR="${LOAD_STUB_ADDR:-127.0.0.1:8099}"
# Doit dépasser le budget encodé dans le script (p99 < 250 ms), avec de la marge.
SLOW_DELAY="${LOAD_SLOW_DELAY:-300ms}"

# D7 : jamais de skip. Un test qui se dérobe quand son outil manque est vert sans avoir rien vérifié.
if ! command -v "$K6" >/dev/null 2>&1; then
  cat >&2 <<EOF
load-smoke: k6 est introuvable.

k6 est un binaire hors go.mod (plan §1.3, requis à M12) : il s'installe à part.
  macOS   : brew install k6
  Linux   : voir https://grafana.com/docs/k6/latest/set-up/install-k6/

Ce script échoue au lieu de se skipper : un harnais de charge qu'on croit vert
parce qu'il ne s'est pas exécuté est pire que pas de harnais du tout.
EOF
  exit 2
fi

# `nc` décide trois fois si le port est pris. Non vérifié, son absence se lit « port libre » (le
# `|| return 0` de cleanup) et fait accuser le stub de ne pas écouter alors qu'il écoute très bien.
if ! command -v nc >/dev/null 2>&1; then
  echo "load-smoke: nc est introuvable — il sonde le port entre les deux runs." >&2
  echo "            macOS : livré avec le système · Debian/Ubuntu : apt install netcat-openbsd" >&2
  exit 2
fi

HOST="${ADDR%:*}"
PORT="${ADDR##*:}"
# Une adresse sans hôte (":8099", le défaut du stub) donnerait un HOST vide : nc échouerait toujours
# et BASE_URL serait invalide. On la ramène sur la boucle locale plutôt que d'échouer 10 s plus tard.
if [[ -z "$HOST" ]]; then
  HOST="127.0.0.1"
  ADDR="$HOST:$PORT"
fi

# Le stub est COMPILÉ puis lancé directement, jamais via `go run` : `go run` exécute le binaire dans
# un processus enfant, si bien que tuer son PID laisse le serveur vivant et le port occupé. Le run
# suivant taperait alors l'ancien stub — celui qui n'est pas ralenti — et passerait le seuil sans
# que rien ne le signale.
STUB_BIN="$(mktemp -t load-stub.XXXXXX)"
NEG_LOG=""
# Le trap est posé AVANT le build : un `go build` qui échoue sous `set -e` sortirait sinon en laissant
# le fichier temporaire derrière lui à chaque tentative.
trap 'cleanup || true; rm -f "$STUB_BIN" "$NEG_LOG"' EXIT
go build -o "$STUB_BIN" ./cmd/load-stub

STUB_PID=""
cleanup() {
  if [[ -n "$STUB_PID" ]]; then
    kill "$STUB_PID" 2>/dev/null || true
    wait "$STUB_PID" 2>/dev/null || true
    STUB_PID=""
  fi
  # Le port doit être réellement rendu avant le run suivant, sinon on mesure le stub précédent.
  for _ in {1..50}; do
    nc -z "$HOST" "$PORT" 2>/dev/null || return 0
    sleep 0.2
  done
  echo "load-smoke: $ADDR est toujours occupé après l'arrêt du stub" >&2
  return 1
}

# start_stub <délai> — démarre le stub et attend qu'il accepte réellement une connexion.
start_stub() {
  if nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "load-smoke: $ADDR est déjà occupé — un run mesurerait le mauvais serveur" >&2
    return 1
  fi
  "$STUB_BIN" -addr "$ADDR" -delay "$1" &
  STUB_PID=$!
  for _ in {1..50}; do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "load-smoke: le stub n'écoute pas sur $ADDR après 10 s" >&2
  return 1
}

# Les deux runs capturent le code de sortie de k6 lui-même. Surtout pas de pipe vers tail ici :
# le code de sortie serait celui de tail, et un seuil violé passerait pour un succès.
echo "==> run POSITIF — stub sans délai, le budget doit être tenu"
start_stub 0
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT"
positive=$?
set -e
cleanup

if [[ $positive -ne 0 ]]; then
  echo "load-smoke: ÉCHEC — le run positif aurait dû passer (exit $positive)" >&2
  exit 1
fi

echo
echo "==> run NÉGATIF — stub ralenti à $SLOW_DELAY, le seuil de latence doit tomber"
start_stub "$SLOW_DELAY"
NEG_LOG="$(mktemp -t load-smoke-negative.XXXXXX)"
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT" 2>&1 | tee "$NEG_LOG"
negative=${PIPESTATUS[0]}   # surtout pas $? : ce serait le code de tee, jamais celui de k6
set -e
cleanup

# Un exit non nul ne suffit PAS comme preuve. k6 sort 99 dès qu'un seuil QUELCONQUE tombe : contre un
# serveur absent, http_req_failed tombe à 100 % et p(99) reste vert — le run échoue sans avoir jamais
# mesuré la latence. Conclure « OK » là-dessus, c'est le faux vert que ce script existe pour empêcher.
# On exige donc que ce soit BIEN le budget de latence qui casse, et que le trafic ait été servi.
if [[ $negative -ne 99 ]]; then
  echo "load-smoke: ÉCHEC — k6 a rendu $negative, or seul 99 signale un seuil franchi." >&2
  echo "            (105 interruption · 107 exception du script · 108/109 erreur interne)" >&2
  exit 1
fi
if ! grep -qE "✗ .*p\(99\)" "$NEG_LOG"; then
  echo "load-smoke: ÉCHEC — le run négatif a échoué SANS que le budget de latence tombe." >&2
  echo "            Le seuil d'ingestion n'a donc pas été mis à l'épreuve. Extrait :" >&2
  grep -E "✓|✗" "$NEG_LOG" >&2 || true
  exit 1
fi
if grep -qE "✗ .*http_req_failed" "$NEG_LOG"; then
  echo "load-smoke: ÉCHEC — des requêtes ont échoué : le stub ne servait pas le trafic." >&2
  echo "            Un run où rien n'aboutit ne prouve rien du budget de latence." >&2
  exit 1
fi
rm -f "$NEG_LOG"

echo
echo "load-smoke: OK — le harnais passe à vide (exit 0) et son budget de latence tombe sous contrainte"
echo "            (exit 99 sur p(99), trafic servi)."
