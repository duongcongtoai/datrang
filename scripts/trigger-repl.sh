psql_cmd() {
  psql -v ON_ERROR_STOP=1 -h "$POSTGRES__HOST" -p "$POSTGRES__PORT" -U "$POSTGRES__USERNAME" -d "$POSTGRES__NAME" "$@"
}
psql_cmd -c "ALTER SUBSCRIPTION mysub REFRESH PUBLICATION"
 
SELECT subname, subslotname, subpublications
FROM pg_subscription
WHERE subpublications @> ARRAY['your_publication'];