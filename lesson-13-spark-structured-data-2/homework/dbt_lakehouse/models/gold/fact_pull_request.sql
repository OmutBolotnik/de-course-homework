-- Крок 9: gold.fact_pull_request. Специфікація: ../../SPEC.md → «Крок 9».
-- Джерело: {{ ref('pull_requests') }}. Грануляція: PR.
-- pr_id = md5(concat_ws('|', repo_name, cast(pr_number as string)));
-- merged_date_id — NULL, якщо не змерджено; label_count через CASE (size(NULL) = -1).
-- Колонки: pr_id, repo_id, author_id, opened_date_id, merged_date_id, state, is_merged, is_draft,
--          additions, deletions, churn, changed_files, commits_count, comments, review_comments,
--          hours_open, label_count.

SELECT
    md5(concat_ws('|', repo_name, cast(pr_number AS string))) AS pr_id,
    md5(repo_name)                                            AS repo_id,
    md5(author_login)                                         AS author_id,
    cast(date_format(opened_at, 'yyyyMMdd') AS int)           AS opened_date_id,
    CASE
        WHEN merged_at IS NOT NULL
        THEN cast(date_format(merged_at, 'yyyyMMdd') AS int)
    END                                                       AS merged_date_id,
    state,
    is_merged,
    is_draft,
    additions,
    deletions,
    churn,
    changed_files,
    commits_count,
    comments,
    review_comments,
    hours_open,
    CASE WHEN label_names IS NULL THEN 0 ELSE size(label_names) END AS label_count
FROM {{ ref('pull_requests') }}
