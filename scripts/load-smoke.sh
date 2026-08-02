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
# Doit rester identique au sélecteur de seuil du script k6 : c'est ce couplage que la garde vérifie.
CHECK_METRIC="checks{check:status is 202}"
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
POS_LOG=""
IDEM_LOG=""
# Le trap est posé AVANT le build : un `go build` qui échoue sous `set -e` sortirait sinon en laissant
# le fichier temporaire derrière lui à chaque tentative.
trap 'cleanup || true; rm -f "$STUB_BIN" "$NEG_LOG" "$POS_LOG" "$IDEM_LOG"' EXIT
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

# start_stub <délai> [mode d'idempotence] — démarre le stub et attend qu'il accepte réellement une
# connexion. Le mode par défaut, `ignore`, laisse le stub exactement dans son comportement step-200.
start_stub() {
  if nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "load-smoke: $ADDR est déjà occupé — un run mesurerait le mauvais serveur" >&2
    return 1
  fi
  "$STUB_BIN" -addr "$ADDR" -delay "$1" -idempotency "${2:-ignore}" &
  STUB_PID=$!
  for _ in {1..50}; do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "load-smoke: le stub n'écoute pas sur $ADDR après 10 s" >&2
  return 1
}

# k6 rend son bloc THRESHOLDS par métrique — le nom sur une ligne, puis UN marqueur PAR SEUIL :
#     http_req_failed
#     ✓ 'rate<0.01' rate=0.00%
#     ✗ 'rate>0.5'  rate=0.00%
# D'où deux exigences. Lire tout le bloc jusqu'à la ligne vide, et non la seule ligne suivante : sinon
# l'ajout d'un second seuil rendrait la garde aveugle sans rien signaler. Et pouvoir exiger QUELLE
# statistique est tombée : « http_req_duration a franchi un seuil » ne dit pas que c'est p(99), or
# c'est p(99) que ce harnais existe pour certifier.
#
# La comparaison est littérale (awk index/==, pas de regex) : un nom de métrique comme
# `checks{check:status is 202}` ferait sortir grep en erreur, donc répondre « non » — un échec ouvert.
#
#   crossed <journal> <métrique> [statistique attendue]
crossed() {
  local log="${1:?crossed appelée sans journal}"
  awk -v m="$2" -v want="${3:-}" '
    { line = $0; gsub(/^[ \t]+|[ \t]+$/, "", line) }
    line == m           { inblock = 1; next }
    inblock && line == ""   { inblock = 0 }
    inblock && index($0, "✗") {
      if (want == "" || index($0, want) > 0) { found = 1 }
    }
    END { exit found ? 0 : 1 }
  ' "$log"
}

# sampled <journal> <métrique> — vrai si le seuil de cette métrique a mesuré quelque chose.
#
# k6 affiche un seuil SANS AUCUN échantillon comme respecté : « ✓ 'rate>0.99' rate=0.00% ». Un seuil
# devenu inopérant — sélecteur qui ne correspond plus à rien — est donc rendu VERT, des deux côtés du
# couple positif/négatif. Sans cette garde, le harnais qui existe pour détecter les seuils morts en
# laisserait passer un. On exige un taux non nul, c'est-à-dire des données.
sampled() {
  local log="${1:?sampled appelée sans journal}"
  awk -v m="$2" '
    { line = $0; gsub(/^[ \t]+|[ \t]+$/, "", line) }
    line == m         { inblock = 1; next }
    inblock && line == "" { inblock = 0 }
    inblock && match($0, /rate=[0-9]+\.[0-9]+%/) {
      rate = substr($0, RSTART + 5, RLENGTH - 6) + 0
      if (rate > 0) { found = 1 }
    }
    END { exit found ? 0 : 1 }
  ' "$log"
}

# Les deux runs capturent le code de sortie de k6 lui-même. Surtout pas de pipe vers tail ici :
# le code de sortie serait celui de tail, et un seuil violé passerait pour un succès.
echo "==> run POSITIF — stub sans délai, le budget doit être tenu"
start_stub 0
POS_LOG="$(mktemp -t load-smoke-positive.XXXXXX)"
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT" 2>&1 | tee "$POS_LOG"
positive=${PIPESTATUS[0]}
set -e

# Le diagnostic AVANT le nettoyage : si le port ne se libère pas, cleanup sort sous `set -e` et
# l'opérateur lit « port occupé » à la place de la vraie cause.
if [[ $positive -ne 0 ]]; then
  echo "load-smoke: ÉCHEC — le run positif aurait dû passer (exit $positive)" >&2
  exit 1
fi

# Un seuil SANS ÉCHANTILLON est rendu vert par k6 (« ✓ 'rate>0.99' rate=0.00% »). Un sélecteur de
# check dévié — renommage, montée de version de k6, `check()` qui cesse d'être appelé — passe donc
# vert des deux côtés, et le harnais qui existe pour détecter les seuils morts en laisse passer un.
# On exige que le seuil sur les 202 ait réellement observé du trafic.
if ! sampled "$POS_LOG" "$CHECK_METRIC"; then
  echo "load-smoke: ÉCHEC — le seuil « $CHECK_METRIC » n'a mesuré aucun échantillon." >&2
  echo "            k6 rend un seuil sans données comme VERT : celui-ci ne prouve donc rien." >&2
  echo "            Le sélecteur de check du script k6 ne correspond probablement plus." >&2
  grep -B1 -E "✓|✗" "$POS_LOG" >&2 || true
  exit 1
fi
cleanup

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

if ! crossed "$NEG_LOG" "http_req_duration" "p(99)"; then
  echo "load-smoke: ÉCHEC — le run négatif a échoué SANS que le budget de latence tombe." >&2
  echo "            Le seuil d'ingestion n'a donc pas été mis à l'épreuve. Seuils du run :" >&2
  grep -B1 -E "✓|✗" "$NEG_LOG" >&2 || true
  exit 1
fi
if crossed "$NEG_LOG" "http_req_failed" "rate"; then
  echo "load-smoke: ÉCHEC — des requêtes ont échoué : le stub ne servait pas le trafic." >&2
  echo "            Un run où rien n'aboutit ne prouve rien du budget de latence. Seuils du run :" >&2
  grep -B1 -E "✓|✗" "$NEG_LOG" >&2 || true
  exit 1
fi
rm -f "$NEG_LOG"

# ---------------------------------------------------------------------------- option Idempotency-Key
#
# Le dépôt n'a ni node_modules ni jest, et la CI n'installe que Go et le binaire k6 : on ne teste donc
# pas le JavaScript de l'option `IDEMPOTENCY`, on OBSERVE ses effets sur le stub, qui reçoit chaque
# requête (step-201 D11). Trois runs, dont un négatif — sans lui, les deux positifs passeraient
# trivialement contre un stub qui ignore tout, et débrancher l'observateur ne ferait aucun bruit.
IDEM_LOG="$(mktemp -t load-smoke-idempotency.XXXXXX)"

echo
echo "==> run IDEMPOTENCY=on contre un stub require-unique — chaque clé doit être unique et non vide"
start_stub 0 require-unique
set +e
IDEMPOTENCY=on BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT" 2>&1 | tee "$IDEM_LOG"
idem_unique=${PIPESTATUS[0]}
set -e

if [[ $idem_unique -ne 0 ]]; then
  echo "load-smoke: ÉCHEC — IDEMPOTENCY=on a été rejeté par le stub require-unique (exit $idem_unique)." >&2
  echo "            Le stub refuse en 422 une clé absente, vide, de plus de 128 caractères ou déjà vue." >&2
  echo "            Une clé constante mesurerait le cache d'idempotence au lieu du chemin idempotent." >&2
  grep -B1 -E "✓|✗" "$IDEM_LOG" >&2 || true
  exit 1
fi
# Exit 0 sans trafic ne prouve rien : on exige que les ~500 acceptations aient bien été observées.
if ! sampled "$IDEM_LOG" "$CHECK_METRIC"; then
  echo "load-smoke: ÉCHEC — le run IDEMPOTENCY=on n'a mesuré aucune acceptation." >&2
  echo "            Un run sans échantillon sort 0 sans avoir soumis une seule clé." >&2
  exit 1
fi
cleanup

# Les deux runs suivants partagent UN SEUL stub `forbid`, délibérément : le premier prouve que ce
# processus sert le trafic (501 × 202), si bien que le rejet du second ne peut venir que de l'en-tête.
# Deux stubs distincts laisseraient « le second stub n'a jamais démarré » comme explication concurrente
# d'un exit 99, et ce script existe pour supprimer exactement ce genre de faux verdict.
echo
echo "==> run IDEMPOTENCY absent contre un stub forbid — aucun en-tête ne doit être émis"
start_stub 0 forbid
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT" 2>&1 | tee "$IDEM_LOG"
idem_off=${PIPESTATUS[0]}
set -e

if [[ $idem_off -ne 0 ]]; then
  echo "load-smoke: ÉCHEC — le script émet l'en-tête alors qu'IDEMPOTENCY n'est pas demandé (exit $idem_off)." >&2
  echo "            Le stub forbid refuse la PRÉSENCE de l'en-tête, y compris présent et vide — cas" >&2
  echo "            que la passerelle réelle traiterait silencieusement comme « pas d'idempotence »." >&2
  grep -B1 -E "✓|✗" "$IDEM_LOG" >&2 || true
  exit 1
fi
if ! sampled "$IDEM_LOG" "$CHECK_METRIC"; then
  echo "load-smoke: ÉCHEC — le run IDEMPOTENCY absent n'a mesuré aucune acceptation." >&2
  exit 1
fi

echo
echo "==> run NÉGATIF IDEMPOTENCY=on contre le MÊME stub forbid — l'en-tête doit être détecté"
set +e
IDEMPOTENCY=on BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT" 2>&1 | tee "$IDEM_LOG"
idem_on=${PIPESTATUS[0]}
set -e
cleanup

if [[ $idem_on -ne 99 ]]; then
  echo "load-smoke: ÉCHEC — k6 a rendu $idem_on, or seul 99 signale un seuil franchi." >&2
  echo "            IDEMPOTENCY=on n'a donc pas émis d'en-tête que le stub ait pu refuser :" >&2
  echo "            l'observateur est débranché et les deux runs positifs ne prouvent rien." >&2
  grep -B1 -E "✓|✗" "$IDEM_LOG" >&2 || true
  exit 1
fi
# Le seuil qui doit tomber est celui des 202 — c'est le rejet de l'en-tête. Le run précédent, sur ce
# même processus, vient d'obtenir 100 % de 202 : un exit 99 accompagné d'un taux de 202 effondré ne
# peut donc pas s'expliquer par un stub absent ou muet.
if ! crossed "$IDEM_LOG" "$CHECK_METRIC" "rate"; then
  echo "load-smoke: ÉCHEC — le run a échoué SANS que le taux de 202 tombe." >&2
  echo "            L'échec ne vient donc pas du refus de l'en-tête. Seuils du run :" >&2
  grep -B1 -E "✓|✗" "$IDEM_LOG" >&2 || true
  exit 1
fi
rm -f "$IDEM_LOG"
IDEM_LOG=""

echo
echo "load-smoke: OK — le harnais passe à vide (exit 0) et son budget de latence tombe sous contrainte"
echo "            (exit 99 sur p(99), trafic servi) ; l'option IDEMPOTENCY émet une clé unique par"
echo "            itération quand elle est demandée, et rien du tout sinon."
