CREATE TABLE test_interval_day_to_second (
    idx INTEGER,
    res INTERVAL DAY TO SECOND
);

INSERT INTO test_interval_day_to_second (idx, res) VALUES (1, INTERVAL '2' DAY);
INSERT INTO test_interval_day_to_second (idx, res) VALUES (2, INTERVAL '1' HOUR);
INSERT INTO test_interval_day_to_second (idx, res) VALUES (3, INTERVAL '30' MINUTE);
INSERT INTO test_interval_day_to_second (idx, res) VALUES (4, INTERVAL '45.123' SECOND);
INSERT INTO test_interval_day_to_second (idx, res) VALUES (5, INTERVAL '1 23:59:59.999' DAY TO SECOND);
INSERT INTO test_interval_day_to_second (idx, res) VALUES (6, NULL);
