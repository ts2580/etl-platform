package com.apache.sfdc.common;

import java.util.List;

public interface SalesforceMutationRepositoryPort {
    int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery);

    int updateObject(StringBuilder strUpdate);

    int deleteObject(StringBuilder strDelete);
}
