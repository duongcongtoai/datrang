#!/usr/bin/env bash
set -euo pipefail

# Optional: fail early if these aren’t provided
: "${PGHOST:?PGHOST is required}"
: "${PGPORT:?PGPORT is required}"
: "${PGUSER:?PGUSER is required}"
: "${PGDATABASE:?PGDATABASE is required}"
: "${SLOT_NAME:?SLOT_NAME is required}"
: "${SLOT_PLUGIN:?SLOT_PLUGIN is required}"

# Default publication name to "${SLOT_NAME}_pub" unless overridden
PUBLICATION_NAME="${PUBLICATION_NAME:-${SLOT_NAME}_pub}"

echo "Waiting for Postgres at $PGHOST:$PGPORT (db=$PGDATABASE, user=$PGUSER)…"
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
  sleep 1
done

psql_cmd() {
  psql -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" "$@"
}

echo "Ensuring table public.event exists…"
psql_cmd <<'SQL'
CREATE TABLE IF NOT EXISTS public.event (
  id TEXT PRIMARY KEY,
  "timestamp" TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL
);
SQL



# Do some migration from superbase/etl
# cargo install --git https://github.com/supabase/etl.git etl-api --rev https://github.com/supabase/etl/commit/762c9c0a479ae0c29083ae3993deaf10d80a9ddc
# APP_ENVIRONMENT=dev && etl-api migrate
psql_cmd <<'SQL'
CREATE SCHEMA etl;
SQL
echo "Done."

echo "Ensuring publication '${PUBLICATION_NAME}' exists and includes public.event…"
PUB_EXISTS="$(psql_cmd -Atc "SELECT 1 FROM pg_publication WHERE pubname='${PUBLICATION_NAME}'" || true)"
if [[ "$PUB_EXISTS" != "1" ]]; then
  psql_cmd -c "CREATE PUBLICATION \"${PUBLICATION_NAME}\" FOR TABLE public.event"
else
  IN_PUB="$(psql_cmd -Atc "SELECT 1
                           FROM pg_publication_tables
                           WHERE pubname='${PUBLICATION_NAME}'
                             AND schemaname='public'
                             AND tablename='event'" || true)"
  if [[ "$IN_PUB" != "1" ]]; then
    psql_cmd -c "ALTER PUBLICATION \"${PUBLICATION_NAME}\" ADD TABLE public.event;"
  else
    echo "Table public.event already in publication '${PUBLICATION_NAME}'."
  fi
fi


