/* KC SPCS release test (SNOW-4202412): one-time account provisioning.
   Run ONCE by an account admin. NOT run by CI.
   DEDICATED empty test account only. The account owner must configure a
   budget/resource monitor, approved driver egress CIDRs and emergency access.
   Do not run against the shared Snowprober account or a customer account.

   Placeholders:
     <<DRIVER_USER>>      CI service user (key-pair, runs OUTSIDE SPCS)
     <<DRIVER_PUBKEY>>    its RSA public key (PEM body, one line)
     <<DRIVER_CIDR>>      approved CI egress CIDR; never a universal allow-list
     <<INSTANCE_FAMILY>>  smallest CPU family >= 2 vCPU / 8 GiB on this
                          deployment (SHOW COMPUTE POOL INSTANCE FAMILIES)
*/

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS KC_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

CREATE ROLE IF NOT EXISTS KC_SPCS_TEST;
GRANT ROLE KC_SPCS_TEST TO ROLE SYSADMIN;
-- The test role owns objects only in the dedicated test schema.
GRANT USAGE, OPERATE ON WAREHOUSE KC_WH TO ROLE KC_SPCS_TEST;

CREATE DATABASE IF NOT EXISTS KC_TEST;
CREATE SCHEMA IF NOT EXISTS KC_TEST.KC;
GRANT OWNERSHIP ON DATABASE KC_TEST TO ROLE KC_SPCS_TEST COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA KC_TEST.KC TO ROLE KC_SPCS_TEST COPY CURRENT GRANTS;

CREATE COMPUTE POOL IF NOT EXISTS KC_POOL
  MIN_NODES = 1 MAX_NODES = 1
  INSTANCE_FAMILY = <<INSTANCE_FAMILY>>
  AUTO_SUSPEND_SECS = 300 AUTO_RESUME = TRUE
  COMMENT = 'KC SPCS release test (SNOW-4202412)';
GRANT USAGE, MONITOR, OPERATE ON COMPUTE POOL KC_POOL TO ROLE KC_SPCS_TEST;

USE ROLE KC_SPCS_TEST;
CREATE IMAGE REPOSITORY IF NOT EXISTS KC_TEST.KC.KC_REPO;
CREATE STAGE IF NOT EXISTS KC_TEST.KC.HARNESS_STAGE
  DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Operator-controlled sentinel, readable but not editable by the job role.
USE ROLE ACCOUNTADMIN;
CREATE TABLE KC_TEST.KC.FIXTURE_IDENTITY (ACCOUNT_LOCATOR VARCHAR, PURPOSE VARCHAR);
INSERT INTO KC_TEST.KC.FIXTURE_IDENTITY
  SELECT CURRENT_ACCOUNT(), 'SNOW-4202412_DEDICATED';
GRANT SELECT ON TABLE KC_TEST.KC.FIXTURE_IDENTITY TO ROLE KC_SPCS_TEST;

-- Approved driver addresses only. SPCS evaluation uses the pool rule.
CREATE NETWORK RULE IF NOT EXISTS KC_TEST.KC.KC_SPCS_IPV4
  TYPE = IPV4 MODE = INGRESS VALUE_LIST = ('<<DRIVER_CIDR>>');
CREATE NETWORK RULE IF NOT EXISTS KC_TEST.KC.KC_SPCS_POOL
  TYPE = COMPUTE_POOL MODE = INGRESS VALUE_LIST = ('KC_POOL');
-- Pinned to KC_POOL on purpose: cell B tests a named compute-pool rule.
-- The owner must qualify both policies before enabling the CI environment.

USE ROLE ACCOUNTADMIN;
-- Cell B: IPv4 + compute-pool rule. Cell C: IPv4 only (expected 390422).
CREATE NETWORK POLICY IF NOT EXISTS KC_NP_WITH_POOL
  ALLOWED_NETWORK_RULE_LIST = ('KC_TEST.KC.KC_SPCS_IPV4', 'KC_TEST.KC.KC_SPCS_POOL');
CREATE NETWORK POLICY IF NOT EXISTS KC_NP_WITHOUT_POOL
  ALLOWED_NETWORK_RULE_LIST = ('KC_TEST.KC.KC_SPCS_IPV4');
-- Driver's own policy. User-level policy overrides the account policy the
-- driver sets per cell, so the CI runner can never lock itself out.
CREATE NETWORK POLICY IF NOT EXISTS KC_NP_DRIVER
  ALLOWED_NETWORK_RULE_LIST = ('KC_TEST.KC.KC_SPCS_IPV4');

-- The driver applies cell policies at ACCOUNT level (dedicated account only).
-- ACCOUNTADMIN is granted because ALTER ACCOUNT SET NETWORK_POLICY needs it;
-- this account must hold nothing but test data.
CREATE USER IF NOT EXISTS <<DRIVER_USER>>
  TYPE = SERVICE DEFAULT_ROLE = KC_SPCS_TEST DEFAULT_WAREHOUSE = KC_WH
  RSA_PUBLIC_KEY = '<<DRIVER_PUBKEY>>'
  NETWORK_POLICY = KC_NP_DRIVER;
GRANT ROLE KC_SPCS_TEST TO USER <<DRIVER_USER>>;
GRANT ROLE ACCOUNTADMIN TO USER <<DRIVER_USER>>;

/* Push the base image once (from a machine with docker and snow CLI):
     snow spcs image-registry login -c <conn>
     docker build --platform linux/amd64 --build-arg BASE_IMAGE=<reviewed-jre17-digest> \
       -t <repo_url>/kc-spcs-release:1 test/spcs
     docker push <repo_url>/kc-spcs-release:1
   <repo_url> comes from SHOW IMAGE REPOSITORIES IN SCHEMA KC_TEST.KC.       */

SHOW NETWORK POLICIES;
SHOW COMPUTE POOLS LIKE 'KC_POOL';
