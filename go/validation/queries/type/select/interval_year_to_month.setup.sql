CREATE TABLE test_interval_year_to_month (
    idx INTEGER,
    res INTERVAL YEAR TO MONTH
);

INSERT INTO test_interval_year_to_month (idx, res) VALUES (1, INTERVAL '3' MONTH);
INSERT INTO test_interval_year_to_month (idx, res) VALUES (2, INTERVAL '2' YEAR);
INSERT INTO test_interval_year_to_month (idx, res) VALUES (3, INTERVAL '2-6' YEAR TO MONTH);
INSERT INTO test_interval_year_to_month (idx, res) VALUES (4, INTERVAL '0-0' YEAR TO MONTH);
INSERT INTO test_interval_year_to_month (idx, res) VALUES (5, NULL);
