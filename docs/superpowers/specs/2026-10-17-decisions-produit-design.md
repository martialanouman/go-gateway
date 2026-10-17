# Décisions produit — exigence ou non-objectif

2026-10-17 · Décision 8 du monorepo (`docs/audit-architecture-2026-10-14.md` §3 T17, T15, §6).
Statut : validé.

## 1. Problème

Des décisions produit ont été renvoyées en dette (T17), et des réglages ont été acceptés sans effet
(T1). Chaque sujet devient ici une **exigence** ou un **non-objectif net**, jamais un report.

## 2. Décisions

| # | Sujet | Verdict | Contenu |
|---|---|---|---|
| P1 | Mono- ou multi-pays | **Exigence : mono-pays** | Un pays de référence par plateforme : indicatif par défaut de la normalisation E.164, une liste de mots-clés STOP, un fuseau horaire, un régulateur. L'opt-out porte sur le MSISDN E.164. L'envoi vers l'étranger reste possible par le routage ; appliquer les règles d'un autre pays de destination est un **non-objectif** |
| P2 | Quiet hours | **Exigence, opt-in** | Désactivées par défaut : tout part normalement. Activées, l'exploitant définit la fenêtre (début, fin, jours), à l'heure du pays de référence, au niveau de la plateforme (`platform_policy`, §3). Elles ne visent que la catégorie `marketing`. Un message `marketing` soumis dans la fenêtre est **refusé à l'admission**, avant l'ACK, là où la catégorie est déjà lue pour choisir le topic (`quiet_hours`), jamais différé : différer serait de l'envoi programmé, hors périmètre (spec §1) |
| P3 | Formule de réputation anti-spam | **Non-objectif** | La réputation est supprimée. L'anti-spam se compose de règles et de compteurs de débit |
| P4 | Plafond journalier `max_per_day` | **Non-objectif** | Champ supprimé. Le crédit prépayé borne la dépense, l'admission borne le débit |
| P5 | Fournisseur de facturation externe (§6.10) | **Non-objectif** | Seul le grand livre interne existe (décision 1). `external_billing_providers` et le stub sont supprimés |
| P6 | API de collecte des MO et des DLR | **Non-objectif** | Réception par webhook ou par bind SMPP (décision 4) |
| P7 | Session du dashboard (T15) | **Exigence** | L'activité est un geste de l'opérateur : 15 min d'inactivité ferment la session ; une interrogation automatique ne la prolonge pas. Un écran mural de NOC passe par un rôle **`noc`** en lecture seule (`stream:read` et lectures), session de 12 h sans délai d'inactivité, aucune écriture |

## 3. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| Spec §1 | Mono-pays et quiet hours opt-in deviennent des exigences ; P3 à P6 vont aux non-objectifs |
| Spec §6.5 (anti-spam) | Réputation retirée |
| Spec §6.10 | Facturation externe retirée |
| Schéma | `platform_content_policy`, ligne unique, devient `platform_policy` : elle porte déjà la politique de contenu par défaut et gagne `quiet_hours` (nullable = désactivé) ; `max_per_day`, la réputation et `external_billing_providers` sont supprimés |
| Contrat Admin | Champs retirés et `quiet_hours` ajouté : inclus dans le bump majeur de la décision 5. Code d'erreur `quiet_hours` (les trois endroits de `.claude/rules/errors.md`) |
| Contrat BFF | Rôle `noc` ; politique d'inactivité |
| ADR-0010 | `external_billing_providers` retiré du modèle client |
| ADR-0016 | `external_billing_providers.auth_config_sealed` disparaît avec sa table |
