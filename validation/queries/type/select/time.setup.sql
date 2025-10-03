CREATE TABLE test_time (
    idx INTEGER,
    res TIME(6)
);

INSERT INTO test_time (idx, res) VALUES (1, TIME '13:45:31.123456');
INSERT INTO test_time (idx, res) VALUES (2, TIME '00:00:00');
INSERT INTO test_time (idx, res) VALUES (3, TIME '23:59:59.999999');
INSERT INTO test_time (idx, res) VALUES (4, TIME '12:30:45.500');
INSERT INTO test_time (idx, res) VALUES (5, NULL);
