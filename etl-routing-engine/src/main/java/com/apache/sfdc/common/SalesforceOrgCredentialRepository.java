package com.apache.sfdc.common;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface SalesforceOrgCredentialRepository {

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, schema_name AS schemaName, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt FROM config.salesforce_org_credentials WHERE org_key = #{orgKey} AND is_active = true")
    SalesforceOrgCredential findActiveByOrgKey(@Param("orgKey") String orgKey);

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, schema_name AS schemaName, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt FROM config.salesforce_org_credentials WHERE is_active = true AND (org_key = #{orgKey} OR my_domain = #{myDomain} OR my_domain = #{normalizedMyDomain} OR REPLACE(REPLACE(REPLACE(my_domain, 'https://', ''), 'http://', ''), '/', '') = #{normalizedMyDomain}) LIMIT 1")
    SalesforceOrgCredential findActiveByOrgIdentifier(@Param("orgKey") String orgKey,
                                                    @Param("myDomain") String myDomain,
                                                    @Param("normalizedMyDomain") String normalizedMyDomain);

    @Update("UPDATE config.salesforce_org_credentials SET access_token = #{accessToken}, access_token_issued_at = NOW(), credential_version = credential_version + 1, updated_at = CURRENT_TIMESTAMP WHERE org_key = #{orgKey}")
    int updateAccessToken(@Param("orgKey") String orgKey,
                          @Param("accessToken") String accessToken);
}
