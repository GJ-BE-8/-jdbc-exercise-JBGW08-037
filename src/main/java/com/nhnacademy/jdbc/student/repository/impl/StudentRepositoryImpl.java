package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import lombok.extern.slf4j.Slf4j;
import java.sql.*;
import java.util.Optional;

@Slf4j
public class StudentRepositoryImpl implements StudentRepository {

    @Override
    public int save(Connection connection, Student student){
        //todo#2 학생등록
        String sql = "insert into jdbc_students(id,name,gender,age) values(?,?,?,?)";

        try(
            PreparedStatement statement = connection.prepareStatement(sql);
            ) {
            statement.setString(1,student.getId());
            statement.setString(2,student.getName());
            statement.setString(3,student.getGender().name());
            statement.setInt(4,student.getAge());
            int result =  statement.executeUpdate();

            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Student> findById(Connection connection,String id){
        //todo#3 학생조회
        String sql = "select * from jdbc_students where id = ?";
        log.debug("sql:{}", sql);

        ResultSet rs = null;
        try(
            PreparedStatement statement = connection.prepareStatement(sql);

            ) {
            statement.setString(1,id);
            rs =statement.executeQuery();

            if(rs.next()){
                Student student = new Student(rs.getString("id"),
                    rs.getString("name"),
                    Student.GENDER.valueOf(rs.getString("gender")),
                    rs.getInt("age"));
                return Optional.of(student);
            }



        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      return Optional.empty();
    }

    @Override
    public int update(Connection connection,Student student){
        //todo#4 학생수정
        String sql = "update jdbc_students set name=?,gender=?,age=? where id =?";

        log.debug("sql:{}", sql);

        try(PreparedStatement statement = connection.prepareStatement(sql);) {
            int index = 0;
            statement.setString(++index,student.getName());
            statement.setString(++index,student.getGender().toString());
            statement.setInt(++index,student.getAge());
            statement.setString(++index,student.getId());
            int result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;


        } catch (SQLException e) {
          throw new RuntimeException(e);
        }


    }

    @Override
    public int deleteById(Connection connection,String id){
        //todo#5 학생삭제
        String sql = "delete from jdbc_students where id = ?";
        log.debug("sql:{}", sql);

        try(
            PreparedStatement statement = connection.prepareStatement(sql);
            ) {
            statement.setString(1,id);
            int result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

}