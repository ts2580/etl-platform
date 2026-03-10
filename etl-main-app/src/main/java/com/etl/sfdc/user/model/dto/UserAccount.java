package com.etl.sfdc.user.model.dto;

import lombok.Getter;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;


@Getter
public class UserAccount extends User implements UserDetails {
    private Member member;

    public UserAccount(Member member) {
        super(member.getEmail(), member.getPassword(), member.getAuthority());
        this.member = member;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Member that = ((UserAccount) object).member;
        return member.getUsername().equals(that.getUsername());
    }

    @Override
    public int hashCode() {
        return member.getUsername().hashCode();
    }
}