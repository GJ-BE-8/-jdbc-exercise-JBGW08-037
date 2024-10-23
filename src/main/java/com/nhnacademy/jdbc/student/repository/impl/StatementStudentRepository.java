package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import java.util.logging.Logger;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

@Slf4j
public class StatementStudentRepository implements StudentRepository {



    @Override
    public int save(Student student){
        //todo#1 insert student

        String sql = String.format("insert into jdbc_students (student_id,student_name,gender,age) values(%s,%s,%s,%d)",
            student.getId(),
            student.getName(),
            student.getGender(),
            student.getAge());

        log.debug("save:{}",sql);

        try(Connection connection = DbUtils.getConnection();
            Statement statement = connection.createStatement()
        ) {
            int result = statement.executeUpdate(sql);
            log.debug("result:{}",result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 student 조회
        String sql = String.format("select * from jdbc_students where student_id = %s",id);

        log.debug("findById:{}",sql);
        try(Connection connection = DbUtils.getConnection();
            Statement ps = connection.prepareStatement(sql)
        ) {

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

      return Optional.empty();
    }

    @Override
    public int update(Student student){
        //todo#3 student 수정, name <- 수정합니다.

        return 0;
    }

    @Override
    public int deleteById(String id){
       //todo#4 student 삭제

        return 0;
    }

}
