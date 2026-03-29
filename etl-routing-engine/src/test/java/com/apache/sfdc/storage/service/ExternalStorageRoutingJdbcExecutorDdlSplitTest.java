package com.apache.sfdc.storage.service;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.model.repository.ExternalDatabaseStorageRoutingRepository;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.etlplatform.common.storage.database.DatabaseVendor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class ExternalStorageRoutingJdbcExecutorDdlSplitTest {

    @Mock
    private StreamingRepository streamingRepository;
    @Mock
    private PubSubRepository pubSubRepository;
    @Mock
    private ExternalDatabaseStorageRoutingRepository externalStorageRepository;
    @Mock
    private DatabaseCredentialEncryptor credentialEncryptor;

    @Test
    void oracleAnonymousBlockStaysIntactWhileTrailingDdlStillSplits() throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = new ExternalStorageRoutingJdbcExecutor(
                streamingRepository,
                pubSubRepository,
                externalStorageRepository,
                credentialEncryptor
        );

        List<String> statements = invokeSplit(
                executor,
                """
                BEGIN
                  EXECUTE IMMEDIATE 'DROP TABLE STUDY_ORG_ACCOUNT';
                EXCEPTION
                  WHEN OTHERS THEN NULL;
                END;
                /
                CREATE TABLE STUDY_ORG_ACCOUNT (sfid VARCHAR2(18));
                """,
                DatabaseVendor.ORACLE
        );

        assertEquals(2, statements.size());
        assertEquals(
                "BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE STUDY_ORG_ACCOUNT';\nEXCEPTION\n  WHEN OTHERS THEN NULL;\nEND;",
                statements.get(0)
        );
        assertEquals("CREATE TABLE STUDY_ORG_ACCOUNT (sfid VARCHAR2(18))", statements.get(1));
    }

    @Test
    void nonOracleSplitIgnoresSemicolonsInsideStrings() throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = new ExternalStorageRoutingJdbcExecutor(
                streamingRepository,
                pubSubRepository,
                externalStorageRepository,
                credentialEncryptor
        );

        List<String> statements = invokeSplit(
                executor,
                "INSERT INTO audit_log(message) VALUES ('alpha;beta'); CREATE TABLE routed_account (sfid VARCHAR(32));",
                DatabaseVendor.POSTGRESQL
        );

        assertEquals(List.of(
                "INSERT INTO audit_log(message) VALUES ('alpha;beta')",
                "CREATE TABLE routed_account (sfid VARCHAR(32))"
        ), statements);
    }

    @SuppressWarnings("unchecked")
    private List<String> invokeSplit(ExternalStorageRoutingJdbcExecutor executor,
                                     String ddl,
                                     DatabaseVendor vendor) throws Exception {
        Method method = ExternalStorageRoutingJdbcExecutor.class.getDeclaredMethod(
                "splitDdlStatements",
                String.class,
                DatabaseVendor.class
        );
        method.setAccessible(true);
        return (List<String>) method.invoke(executor, ddl, vendor);
    }
}
