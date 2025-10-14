CREATE TABLE test_ipaddress (
    idx INTEGER,
    res IPADDRESS
);

INSERT INTO test_ipaddress (idx, res) VALUES (1, IPADDRESS '192.168.1.1');
INSERT INTO test_ipaddress (idx, res) VALUES (2, IPADDRESS '10.0.0.1');
INSERT INTO test_ipaddress (idx, res) VALUES (3, IPADDRESS '::1');
INSERT INTO test_ipaddress (idx, res) VALUES (4, IPADDRESS '2001:db8::1');
INSERT INTO test_ipaddress (idx, res) VALUES (5, IPADDRESS '0.0.0.0');
INSERT INTO test_ipaddress (idx, res) VALUES (6, NULL);
