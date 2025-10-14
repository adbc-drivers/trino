CREATE TABLE test_timetz (
    idx INTEGER,
    res TIME(6) WITH TIME ZONE
);

INSERT INTO test_timetz (idx, res) VALUES (1, TIME '01:02:03.456 -08:00');
INSERT INTO test_timetz (idx, res) VALUES (2, TIME '13:45:30.123 +05:30');
INSERT INTO test_timetz (idx, res) VALUES (3, TIME '23:59:59.999 +00:00');
INSERT INTO test_timetz (idx, res) VALUES (4, TIME '00:00:00.000 -05:00');
INSERT INTO test_timetz (idx, res) VALUES (5, NULL);
