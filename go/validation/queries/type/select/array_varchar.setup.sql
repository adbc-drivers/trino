CREATE TABLE test_array_varchar (
    idx INTEGER,
    res ARRAY(VARCHAR)
);

INSERT INTO test_array_varchar (idx, res) VALUES (1, ARRAY['a', 'b']);
INSERT INTO test_array_varchar (idx, res) VALUES (2, NULL);
INSERT INTO test_array_varchar (idx, res) VALUES (3, ARRAY[]);
