CREATE TABLE test_uuid (
    idx INTEGER,
    res UUID
);

INSERT INTO test_uuid (idx, res) VALUES (1, UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59');
INSERT INTO test_uuid (idx, res) VALUES (2, UUID '00000000-0000-0000-0000-000000000000');
INSERT INTO test_uuid (idx, res) VALUES (3, UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479');
INSERT INTO test_uuid (idx, res) VALUES (4, UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff');
INSERT INTO test_uuid (idx, res) VALUES (5, UUID '550e8400-e29b-41d4-a716-446655440000');
INSERT INTO test_uuid (idx, res) VALUES (6, NULL);
