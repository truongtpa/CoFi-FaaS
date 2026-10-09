#!/bin/bash

NAMESPACE="benchmark"
POD="tpch-benchmark-driver"
INTERVAL=5
YAMLS=("spark-tpch.yaml" "spark-tpch-aqe.yaml")

TG_URL="https://api.telegram.org/bot8624263213:AAGCd521mgbcjS60vJCQoLy94fHeT1Dpmbc/sendMessage"
TG_CHAT="6670592858"

tg() {
    local text="$1"
    curl -s -X POST "$TG_URL" \
        -H "Content-Type: application/json" \
        --data @<(jq -n --arg chat "$TG_CHAT" --arg text "$text" \
            '{chat_id: $chat, text: $text, parse_mode: "Markdown"}') \
        > /dev/null
}

fmt_duration() {
    local sec=$1
    printf '%02d:%02d:%02d' $((sec/3600)) $((sec%3600/60)) $((sec%60))
}

BATCH_START=$(date +%s)
TOTAL=${#YAMLS[@]}
IDX=0

tg "🚀 *Spark benchmark batch started*
├ Jobs: \`$TOTAL\`
├ Namespace: \`$NAMESPACE\`
└ Started: \`$(date '+%Y-%m-%d %H:%M:%S')\`"

for YAML in "${YAMLS[@]}"; do
    IDX=$((IDX+1))
    JOB_START=$(date +%s)

    echo "[$(date)] Applying $YAML ..."
    kubectl apply -f "$YAML"

    tg "▶️ *Job [$IDX/$TOTAL] Started*
├ File: \`$YAML\`
└ Time: \`$(date '+%H:%M:%S')\`"

    echo "[$(date)] Waiting for pod/$POD to start..."
    sleep 10

    while true; do
        STATUS=$(kubectl get pod "$POD" -n "$NAMESPACE" \
            --no-headers -o custom-columns=":status.phase" 2>/dev/null)

        if [[ -z "$STATUS" ]]; then
            sleep "$INTERVAL"
            continue
        fi

        echo "[$(date)] [$YAML] Pod status: $STATUS"

        if [[ "$STATUS" == "Succeeded" || "$STATUS" == "Failed" ]]; then
            JOB_END=$(date +%s)
            ELAPSED=$((JOB_END - JOB_START))
            DURATION=$(fmt_duration $ELAPSED)

            if [[ "$STATUS" == "Succeeded" ]]; then
                ICON="✅"
            else
                ICON="❌"
            fi

            echo "[$(date)] [$YAML] Finished: $STATUS (${DURATION})"
            tg "$ICON *Job [$IDX/$TOTAL] Finished*
├ File: \`$YAML\`
├ Status: \`$STATUS\`
├ Duration: \`$DURATION\`
└ Time: \`$(date '+%H:%M:%S')\`"
            break
        fi

        sleep "$INTERVAL"
    done

    echo "---"
done

BATCH_END=$(date +%s)
TOTAL_ELAPSED=$((BATCH_END - BATCH_START))
TOTAL_DURATION=$(fmt_duration $TOTAL_ELAPSED)

tg "🏁 *All Jobs Completed*
├ Total jobs: \`$TOTAL\`
├ Total time: \`$TOTAL_DURATION\`
└ Finished: \`$(date '+%Y-%m-%d %H:%M:%S')\`"

echo "[$(date)] All jobs completed. Total: $TOTAL_DURATION"