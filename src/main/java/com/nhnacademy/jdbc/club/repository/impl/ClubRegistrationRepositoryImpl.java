package com.nhnacademy.jdbc.club.repository.impl;

import com.nhnacademy.jdbc.club.domain.ClubStudent;
import com.nhnacademy.jdbc.club.repository.ClubRegistrationRepository;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Slf4j
public class ClubRegistrationRepositoryImpl implements ClubRegistrationRepository {

    @Override
    public int save(Connection connection, String studentId, String clubId) {
        //todo#11 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환
        int result = 0;
        String sql = "insert into jdbc_club_registrations (student_id, club_id) values (?, ?)";

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, studentId);
            statement.setString(2, clubId);
            result = statement.executeUpdate();
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByStudentIdAndClubId(Connection connection, String studentId, String clubId) {
        //todo#12 - 핵생 -> 클럽 탈퇴, executeUpdate() 결과를 반환
        int result = 0;
        String sql = "delete from jdbc_club_registrations where student_id = ? and club_id = ?";

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, studentId);
            statement.setString(2, clubId);
            result = statement.executeUpdate();
            return result;

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public List<ClubStudent> findClubStudentsByStudentId(Connection connection, String studentId) {
        //todo#13 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환
//        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc stduents a inner join jdbc_club_registrations b on a.club_id =b.student_id inner join jdbc_club c on b.club_id = c.club_id where a.id = ? ";
        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a inner join jdbc_club_registrations b on a.id=b.student_id inner join jdbc_club c on b.club_id=c.club_id where a.id=?";
        ResultSet rs = null;

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, studentId);
            rs = statement.executeQuery();
            List<ClubStudent> clubStudentList = new ArrayList<>();

            while (rs.next()){
                clubStudentList.add(new ClubStudent(
                    rs.getString("student_id"),
                    rs.getString("student_name"),
                    rs.getString("club_id"),
                    rs.getString("club_name")
                    )
                );

            }
            return clubStudentList;

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

}
    private List<ClubStudent> getClubStudentList(Connection connection, String sql) {
        ResultSet rs = null;
        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            rs = statement.executeQuery();
            List<ClubStudent> clubStudentList = new ArrayList<>();
            while (rs.next()){
                clubStudentList.add(new ClubStudent(
                    rs.getString("student_id"),
                    rs.getString("student_name"),
                    rs.getString("club_id"),
                    rs.getString("club_name")
                ));
            }
            return clubStudentList;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
    }

    @Override
    public List<ClubStudent> findClubStudents(Connection connection) {
        //todo#21 - join
        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a inner join jdbc_club_registrations b on a.id = b.student_id inner join jdbc_club c on b.club_id = c.club_id";

      return getClubStudentList(connection,sql);

    }

    @Override
    public List<ClubStudent> findClubStudents_left_join(Connection connection) {
        //todo#22 - left join
       String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a left join jdbc_club_registrations b on a.id = b.student_id left join jdbc_club c on b.club_id = c.club_id";
       return getClubStudentList(connection,sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_right_join(Connection connection) {
        //todo#23 - right join
        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a right join jdbc_club_registrations b on a.id = b.student_id right join jdbc_club c on b.club_id = c.club_id";
        return getClubStudentList(connection,sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_full_join(Connection connection) {
        //todo#24 - full join = left join union right join
        StringBuilder sql = new StringBuilder();
        sql.append("select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a left join jdbc_club_registrations b on a.id = b.student_id left join jdbc_club c on b.club_id = c.club_id");
        sql.append(System.lineSeparator());
        sql.append("union");
        sql.append(System.lineSeparator());
        sql.append("select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a right join jdbc_club_registrations b on a.id = b.student_id right join jdbc_club c on b.club_id = c.club_id");
        return getClubStudentList(connection,sql.toString());
    }

    @Override
    public List<ClubStudent> findClubStudents_left_excluding_join(Connection connection) {
        //todo#25 - left excluding join
//        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a left join jdbc_club_registrations b on a.id = b.student_id left join jdbc_club c on b.club c on b.club_id = c.club_id where c.club_id is null order by a.id asc";
        String sql = "select a.id as student_id,  a.name as student_name,  c.club_id,  c.club_name from jdbc_students a  left join jdbc_club_registrations b on a.id=b.student_id left join jdbc_club c on b.club_id=c.club_id where c.club_id is null order by a.id asc";
        return getClubStudentList(connection,sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_right_excluding_join(Connection connection) {
        //todo#26 - right excluding join
//        String sql = "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a right join jdbc_club_registrations b on a.id = b.student_id right join jdbc_club c on b.club_id = c.club_id where c.club_id is null order by a.id asc";
        String sql = "select   a.id as student_id,  a.name as student_name,  c.club_id,  c.club_name from jdbc_students a  right join jdbc_club_registrations b on a.id=b.student_id right join jdbc_club c on b.club_id=c.club_id where a.id is null order by b.club_id asc ";

        return getClubStudentList(connection,sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_outher_excluding_join(Connection connection) {
        //todo#27 - outher_excluding_join = left excluding join union right excluding join
        StringBuilder sql = new StringBuilder();

//        sql.append(
//            "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a left join jdbc_club_registrations b on a.id=b.student_id left join jdbc_club c on b.club_id = c.club_id where c.club_id is null order by a.id asc");
        sql.append("select   a.id as student_id,  a.name as student_name,  c.club_id,  c.club_name from jdbc_students a  left join jdbc_club_registrations b on a.id=b.student_id left join jdbc_club c on b.club_id=c.club_id where c.club_id is null");

        sql.append(System.lineSeparator());
        sql.append("union");
        sql.append(System.lineSeparator());
//        sql.append(
//            "select a.id as student_id, a.name as student_name, c.club_id, c.club_name from jdbc_students a right join jdbc_club_registrations b on a.id = b.student_id right join jdbc_club c on b.club_id = c.club_id where c.club_id is null");
        sql.append("select   a.id as student_id,  a.name as student_name,  c.club_id,  c.club_name from jdbc_students a  right join jdbc_club_registrations b on a.id=b.student_id right join jdbc_club c on b.club_id=c.club_id where a.id is null");
        return getClubStudentList(connection, sql.toString());
    }
}