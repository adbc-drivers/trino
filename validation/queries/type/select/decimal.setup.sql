CREATE TABLE test_decimal (
    idx INTEGER,
    res DECIMAL(10,2)
);

INSERT INTO test_decimal (idx, res) VALUES (1, 123.45);
INSERT INTO test_decimal (idx, res) VALUES (2, 0.00);
INSERT INTO test_decimal (idx, res) VALUES (3, -999.99);
INSERT INTO test_decimal (idx, res) VALUES (4, 9999999.99);
INSERT INTO test_decimal (idx, res) VALUES (5, NULL);
