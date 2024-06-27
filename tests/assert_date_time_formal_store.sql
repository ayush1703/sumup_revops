
WITH import_csv AS (
        SELECT * FROM read_csv_auto('../store.csv', normalize_names=True)
    )
SELECT
    *
FROM import_csv
WHERE customer_id is not null and try_strptime(created_at, '%m/%d/%Y %I:%M:%S') is null --to exclude other bad format timestamps