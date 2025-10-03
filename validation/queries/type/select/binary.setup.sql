CREATE TABLE test_binary (
    idx INTEGER,
    res VARBINARY
);

INSERT INTO test_binary (idx, res) VALUES (1, X'e38193e38293e381abe381a1e381afe38081e4b896e7958cefbc81');  -- 'こんにちは、世界！' in UTF-8
INSERT INTO test_binary (idx, res) VALUES (2, X'00');
INSERT INTO test_binary (idx, res) VALUES (3, X'deadbeef');
INSERT INTO test_binary (idx, res) VALUES (4, X'');
INSERT INTO test_binary (idx, res) VALUES (5, NULL);
