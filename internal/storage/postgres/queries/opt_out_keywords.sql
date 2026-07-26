-- name: ListActiveOptOutKeywords :many
-- Every active opt-out keyword, for the MO STOP-detection snapshot (step-063) and the pipeline
-- opt-out stage. A disabled keyword must not trigger a suppression or auto-reply.
SELECT * FROM control_plane.opt_out_keywords WHERE status = 'active' ORDER BY country_code, keyword;
