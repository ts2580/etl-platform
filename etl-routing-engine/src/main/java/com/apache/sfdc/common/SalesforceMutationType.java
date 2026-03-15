package com.apache.sfdc.common;

public enum SalesforceMutationType {
    CREATE,
    UPDATE,
    DELETE,
    UNDELETE;

    public boolean isCreateLike() {
        return this == CREATE || this == UNDELETE;
    }

    public boolean isDelete() {
        return this == DELETE;
    }
}
