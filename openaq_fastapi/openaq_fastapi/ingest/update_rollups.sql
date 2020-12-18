SET SEARCH_PATH to public, rollups;
SELECT
    rollups.update_rollups(
        '{mindate}'::timestamptz,
        '{maxdate}'::timestamptz,
        null,
        TRUE,
        FALSE,
        FALSE
    );