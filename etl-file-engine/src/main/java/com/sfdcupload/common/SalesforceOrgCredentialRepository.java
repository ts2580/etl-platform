package com.sfdcupload.common;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.util.List;

@Mapper
public interface SalesforceOrgCredentialRepository {

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt FROM config.salesforce_org_credentials WHERE org_key = #{orgKey} AND is_active = true")
    SalesforceOrgCredential findByOrgKey(String orgKey);

    @Select("SELECT id, org_key AS orgKey, org_name AS orgName, my_domain AS myDomain, client_id AS clientId, client_secret AS clientSecret, access_token AS accessToken, access_token_issued_at AS accessTokenIssuedAt, is_active AS isActive, is_default AS isDefault, created_at AS createdAt, updated_at AS updatedAt FROM config.salesforce_org_credentials WHERE is_active = true ORDER BY is_default DESC, id ASC")
    List<SalesforceOrgCredential> findAllActive();

    @Update("UPDATE config.salesforce_org_credentials SET access_token = #{accessToken}, updated_at = CURRENT_TIMESTAMP WHERE org_key = #{orgKey}")
    void updateAccessToken(String orgKey, String accessToken);
}
