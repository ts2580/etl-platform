package com.etl.sfdc.user.model.repository;

import com.etl.sfdc.user.model.dto.Member;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.Optional;

@Mapper
public interface UserRepository {
    @Select("select * from config.member where username = #{userName}")
    Member getUserDes(String userName);
    void create(Member member);

    @Select("select * from config.member where username = #{username}")
    Optional<Member> findByUsername(String username);
}
