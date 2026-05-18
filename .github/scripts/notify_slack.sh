#!/usr/bin/env bash
# Post a Slack notification summarizing a daily Jepsen workflow run.
#
# Usage:
#   notify_slack.sh <title> [<job_name_prefix>]
#
# Args:
#   title             - human-readable test family, e.g. "ScalarDB Cluster"
#   job_name_prefix   - if given, matrix-job names with this prefix are used to
#                       detect tests that produced no result artifact (likely
#                       job-level timeouts). Examples: "ScalarDB Cluster / ",
#                       "DB-".
#
# Required env:
#   GITHUB_REPOSITORY, GITHUB_RUN_ID  (provided by GitHub Actions)
#   GH_TOKEN                          (token with read access to the run)
#   SLACK_WEBHOOK_URL                 (incoming webhook to post to)

set -o pipefail

TITLE="${1:?title is required (e.g., \"ScalarDB Cluster\")}"
JOB_PREFIX="${2:-}"

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is not set}"
: "${GH_TOKEN:?GH_TOKEN is not set}"
: "${SLACK_WEBHOOK_URL:?SLACK_WEBHOOK_URL is not set}"

mkdir -p results triages

# result-* artifacts each contain a result-<name>.txt; flatten into results/.
gh api --paginate \
  "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
  --jq '.artifacts[] | select(.name | startswith("result-")) | .name' \
  | while read -r name; do
      echo "Downloading $name"
      gh run download "${GITHUB_RUN_ID}" -n "$name" -D results \
        -R "${GITHUB_REPOSITORY}" || true
    done

# triage-<name> artifacts each contain triage.md; keep them per-test.
gh api --paginate \
  "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/artifacts" \
  --jq '.artifacts[] | select(.name | startswith("triage-")) | .name' \
  | while read -r name; do
      echo "Downloading $name"
      gh run download "${GITHUB_RUN_ID}" -n "$name" \
        -D "triages/${name#triage-}" -R "${GITHUB_REPOSITORY}" || true
    done

# Extract the triage label for a given test, if any.
get_label() {
  local triage="triages/$1/triage.md"
  [ -f "$triage" ] || return
  grep -E '^- label:' "$triage" | head -n1 \
    | sed -E 's/.*\*\*([^*]+)\*\*.*/\1/'
}

successes=()
failures=()
timeouts=()
declare -A found_tests

for file in results/result-*.txt; do
  [ -f "$file" ] || continue
  while read -r line; do
    name="${line%%:*}"
    status="${line#*:}"
    found_tests["$name"]=1

    if [ "$status" = "success" ]; then
      successes+=("- $name")
    else
      label=$(get_label "$name")
      suffix=""
      [ -n "$label" ] && suffix=" ($label)"
      if [ "$status" = "timeout" ]; then
        timeouts+=("- $name$suffix")
      else
        failures+=("- $name$suffix")
      fi
    fi
  done < "$file"
done

# When a matrix-job prefix is given, detect tests whose result file is missing
# (job-level timeouts produce no result artifact).
if [ -n "$JOB_PREFIX" ]; then
  while IFS= read -r expected; do
    [ -n "$expected" ] || continue
    if [ -z "${found_tests[$expected]:-}" ]; then
      timeouts+=("- $expected (no result)")
    fi
  done < <(
    gh api --paginate \
      "repos/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}/jobs" \
      --jq ".jobs[] | select(.name | startswith(\"${JOB_PREFIX}\")) | .name | ltrimstr(\"${JOB_PREFIX}\")"
  )
fi

if [ "${#failures[@]}" -gt 0 ] || [ "${#timeouts[@]}" -gt 0 ]; then
  icon=":fire:"
else
  icon=":white_check_mark:"
fi

# Compose the message with real newlines; jq -Rs will JSON-encode them.
{
  echo "${icon} *${TITLE} tests completed*"
  echo
  echo "*Success:*"
  for s in "${successes[@]+"${successes[@]}"}"; do echo "$s"; done
  echo
  echo "*Failed:*"
  for s in "${failures[@]+"${failures[@]}"}"; do echo "$s"; done
  echo
  echo "*Timed out:*"
  for s in "${timeouts[@]+"${timeouts[@]}"}"; do echo "$s"; done
  echo "<https://github.com/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}|View logs>"
} > /tmp/slack_message.txt

payload=$(jq -Rs '{text: .}' < /tmp/slack_message.txt)

curl -s -X POST -H 'Content-type: application/json' \
  --data "$payload" \
  "$SLACK_WEBHOOK_URL"
