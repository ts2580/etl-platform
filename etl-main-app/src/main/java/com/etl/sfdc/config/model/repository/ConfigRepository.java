package com.etl.sfdc.config.model.repository;

import com.etl.sfdc.config.model.dto.User;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface ConfigRepository {

    @Select("select * from config.user where username = #{userName}")
    User getUserDes(String userName);
}
