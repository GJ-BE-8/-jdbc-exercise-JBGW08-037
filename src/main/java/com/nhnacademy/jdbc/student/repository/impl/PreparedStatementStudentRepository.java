package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.domain.Student.GENDER;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

@Slf4j
public class PreparedStatementStudentRepository implements StudentRepository {

    @Override
    public int save(Student student){
        //todo#1 학생 등록
        String sql =  String.format("insert jdbc_students(id, name, gender, age) valuse(%s, %s, %s, %d)", student.getId(), student.getName(), student.getGender(), student.getAge());

        log.debug("sql:{}",sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
//            ResultSet rs = ps.executeQuery();
        ) {
            int result = ps.executeUpdate();
            log.debug("result:{}",result);

            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 학생 조회
        String sql = String.format("select * from jdbc_students where id = %s", id);

        log.debug("sql:{}",sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
        ) {
            Student student = new Student(rs.getString("id"),rs.getString("name"),
                (GENDER) rs.getObject("gender"), rs.getInt("age"));

            log.debug("student:{}",student);

            return Optional.of(student);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public int update(Student student){
        //todo#3 학생 수정 , name 수정
        String sql = String.format("update jdbc_students set name = %s where id = %s", student.getName(), student.getId());
        log.debug("sql:{}",sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
        ) {
            int result = ps.executeUpdate();
            log.debug("result:{}",result);

            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public int deleteById(String id){
        //todo#4 학생 삭제
        String sql = String.format("delete from jdbc_students where id = %s", id);
        log.debug("sql:{}",sql);

        try(Connection connection = DbUtils.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql);
        ) {
            int result = ps.executeUpdate();
            log.debug("result:{}",result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

}
