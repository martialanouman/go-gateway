-- A router that reads this table fails its boot without it: roll the router back first.
DROP TABLE control_plane.platform_content_policy;
