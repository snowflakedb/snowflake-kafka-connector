package com.snowflake.kafka.connector.internal;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.LinkedList;

public class PLConnectionServiceIT {

    private SnowflakeConnectionService conn = TestUtils.getConnectionService();

    private Credentials credentials = null;
    
    private Credentials getCredentials() {
        if (credentials != null) {
            return credentials;
        }

        AWSSecurityTokenService sts_client = AWSSecurityTokenServiceClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                "sts-endpoint.amazonaws.com", "signing-region"))
                .build();

        GetSessionTokenRequest session_token_request = new GetSessionTokenRequest();
        session_token_request.setDurationSeconds(7200);

        GetSessionTokenResult session_token_result =
                sts_client.getSessionToken(session_token_request);
        credentials = session_token_result.getCredentials();
        return credentials;
    }


    public String getId() {
        return getCredentials().getAccessKeyId();
    }

    public String getToken() {
        return getCredentials().getSessionToken();
    }
    
    @Before
    public void before() throws Exception {

//        TestUtils.executeQuery("create organization AWSPL1 salesforce_id='testorg' " +
//                "organization_type=internal region_groups='PUBLIC';");
        TestUtils.executeQuery("create or replace account awsaccount_pl2 " +
                "with server_type=medium, license_accepted=true,internal_volume=sfc_fdndata, " +
                "stage_volume=LOCALFS_STAGE, show_internal_params =true, change_internal_params = true, " +
                "advanced_ui_view=true;");
        TestUtils.executeQuery("alter account awsaccount_pl2 set qa_mode=true;");
        TestUtils.executeQuery("alter account awsaccount_pl2 set SERVICE_LEVEL='BUSINESS_CRITICAL' " +
                "PARAMETER_COMMENT='For PL enable';");
        TestUtils.executeQuery("alter account awsaccount_pl2 set enforce_tls = false " +
                "PARAMETER_COMMENT='Disable TLS check when service level is BUSINESS_CRITICAL';");

        // setting parameter to enable the feature
        TestUtils.executeQuery("alter system set ENABLE_PRIVATELINK_MANAGEMENT_FUNCTIONS=true");
        TestUtils.executeQuery("alter account awsaccount_pl2 set ENABLE_PRIVATELINK_SELF_SERVICE=true");
        // setting the service endpoint
        TestUtils.executeQuery("alter system set privatelink_endpoint_id ='{" +
                TestUtils.getSnowflakeAWSPrivateLink() + "}'");
        TestUtils.executeQuery("ALTER SESSION SET qa_cloud_region_api_override = 'AWS_US_WEST_2';");
        
        TestUtils.executeQueryForPrivateLink("ALTER SESSION SET qa_cloud_region_api_override = 'AWS_US_WEST_2';");
    }

    private void testSupport(String id) {
        TestUtils.executeQuery("SELECT SYSTEM$support_AUTHORIZE_PRIVATELINK_CUSTOMER('{" + id + "}')");
        TestUtils.executeQuery("select SYSTEM$support_REVOKE_PRIVATELINK_CUSTOMER('{" + id + "}')");
    }

    private void authorizePLAccess(String id, String token) throws Exception {
        LinkedList<String> contentResult = new LinkedList<>();
        ResultSet authorizeRes = TestUtils.executeQueryForPrivateLink("" +
                "SELECT SYSTEM$AUTHORIZE_PRIVATELINK('{" + id + "}','{" + token + "}')");

        while (authorizeRes.next()) {
            contentResult.add(authorizeRes.getString("RECORD_CONTENT"));
        }
        authorizeRes.close();

        contentResult.get(0).contains("Private link access authorized");

        ResultSet getResult = TestUtils.executeQueryForPrivateLink(
                "SELECT SYSTEM$GET_PRIVATELINK('{" + id + "}','{" + token + "}')");

        assert InternalUtils.resultSize(getResult) != 0;

        while (getResult.next()) {
            contentResult.add(getResult.getString("RECORD_CONTENT"));
        }
        getResult.close();

        contentResult.get(1).contains("Private link access authorized");
    }

    private void revokePLAccess(String id, String token) throws Exception {
        LinkedList<String> contentResult = new LinkedList<>();

        ResultSet revokeRes = TestUtils.executeQueryForPrivateLink(
                "SELECT SYSTEM$REVOKE_PRIVATELINK('{" + id + "}','{" + token + "}')");

        assert InternalUtils.resultSize(revokeRes) != 0;

        while (revokeRes.next()) {
            contentResult.add(revokeRes.getString("RECORD_CONTENT"));
        }
        revokeRes.close();

        contentResult.get(0).contains("Private link access revoked");

        ResultSet getResult = TestUtils.executeQueryForPrivateLink(
                "SELECT SYSTEM$GET_PRIVATELINK('{" + id + "}','{" + token + "}')");

        assert InternalUtils.resultSize(getResult) != 0;

        while (getResult.next()) {
            contentResult.add(getResult.getString("RECORD_CONTENT"));
        }
        getResult.close();

        contentResult.get(1).contains("Private link access revoked");
    }

    @Test
    public void createConnectionService() {
        String id = getId();
        String token = getToken();
        try {
            testSupport(id);
            authorizePLAccess(id, token);
            revokePLAccess(id, token);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void afterEach() {
        TestUtils.executeQuery("alter session unset qa_cloud_region_api_override");
        TestUtils.executeQuery("alter system unset ENABLE_PRIVATELINK_MANAGEMENT_FUNCTIONS");
        TestUtils.executeQuery("drop account awsaccount_pl2");
        TestUtils.executeQuery("drop organization AWSPL1");
        TestUtils.executeQuery("alter session unset qa_mode");
    }
}