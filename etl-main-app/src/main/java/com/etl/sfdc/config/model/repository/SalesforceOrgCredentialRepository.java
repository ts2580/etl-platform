package com.etl.sfdc.config.model.repository;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.Delete;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SalesforceOrgCredentialRepository {

    @Insert("INSERT INTO config.salesforce_org_credentials (org_key, org_name, my_domain, schema_name, client_id, client_secret, access_token, access_token_issued_at, is_active, is_default) " +
            "VALUES (#{orgKey}, #{orgName}, #{myDomain}, #{schemaName}, #{clientId}, #{clientSecret}, #{accessToken}, NOW(), COALESCE(#{isActive}, true), COALESCE(#{isDefault}, false)) " +
            "ON DUPLICATE KEY UPDATE org_name = VALUES(org_name), my_domain = VALUES(my_domain), schema_name = VALUES(schema_name), client_id = VALUES(client_id), " +
            "client_secret = VALUES(client_secret), access_token = VALUES(access_token), " +
            "access_token_issued_at = VALUES(access_token_issued_at), is_active = VALUES(is_active), is_default = VALUES(is_default), updated_at = CURRENT_TIMESTAMP")
    int upsertSalesforceOrg(SalesforceOrgCredential credential);

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, schema_name AS schemaName, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt " +
            "FROM config.salesforce_org_credentials WHERE is_active = true ORDER BY is_default DESC, id ASC")
    List<SalesforceOrgCredential> findAllActiveOrDefault();

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, schema_name AS schemaName, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, credential_version AS credentialVersion, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt " +
            "FROM config.salesforce_org_credentials WHERE org_key = #{orgKey}")
    SalesforceOrgCredential findByOrgKey(@Param("orgKey") String orgKey);

    @Update("UPDATE config.salesforce_org_credentials SET is_default = false WHERE is_default = true")
    int unsetDefaultOrgs();

    @Update("UPDATE config.salesforce_org_credentials SET is_default = true, is_active = true, updated_at = CURRENT_TIMESTAMP WHERE org_key = #{orgKey}")
    int setDefaultOrg(@Param("orgKey") String orgKey);

    @Delete("DELETE FROM config.salesforce_org_credentials WHERE org_key = #{orgKey}")
    int deleteByOrgKey(@Param("orgKey") String orgKey);

    @Update("UPDATE config.salesforce_org_credentials SET access_token = #{accessToken}, access_token_issued_at = NOW(), credential_version = credential_version + 1, updated_at = CURRENT_TIMESTAMP WHERE org_key = #{orgKey}")
    int updateAccessToken(@Param("orgKey") String orgKey, @Param("accessToken") String accessToken);
}
