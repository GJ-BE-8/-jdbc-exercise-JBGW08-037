package com.nhnacademy.jdbc.user.repository.impl;

import com.nhnacademy.jdbc.user.domain.User;
import com.nhnacademy.jdbc.user.repository.UserRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

@Slf4j
public class PreparedStatementUserRepository implements UserRepository {
    @Override
    public Optional<User> findByUserIdAndUserPassword(String userId, String userPassword) {
        //todo#11 -PreparedStatement- 아이디 , 비밀번호가 일치하는 회원조회
        String query = "SELECT user_id, user_name, user_password FROM jdbc_users WHERE user_id = ? AND user_password = ?";
        try (Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, userId);
            preparedStatement.setString(2, userPassword);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    User student = new User(
                        resultSet.getString("user_id"),
                        resultSet.getString("user_name"),
                        resultSet.getString("user_password")
                    );
                    return Optional.of(student);
                }
            }
        } catch (SQLException e) {
            log.error("Error finding student", e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<User> findById(String userId) {
        //todo#12-PreparedStatement-회원조회
        String query = "SELECT user_id, user_name, user_password FROM jdbc_users WHERE user_id = ?";
        try (Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, userId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    User student = new User(
                        resultSet.getString("user_id"),
                        resultSet.getString("user_name"),
                        resultSet.getString("user_password")
                    );
                    return Optional.of(student);
                }
            }
        } catch (SQLException e) {
            log.error("Error finding student", e);
        }
        return Optional.empty();
    }

    @Override
    public int save(User user) {
        //todo#13-PreparedStatement-회원저장
        String query = "INSERT INTO jdbc_users (user_id, user_name, user_password) VALUES (?, ?, ?)";
        try (Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, user.getUserId());
            preparedStatement.setString(2, user.getUserName());
            preparedStatement.setString(3, user.getUserPassword());

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Error inserting student", e);
        }
        return 0;
    }

    @Override
    public int updateUserPasswordByUserId(String userId, String userPassword) {
        //todo#14-PreparedStatement-회원정보 수정
        String query = "UPDATE jdbc_users SET user_password = ? WHERE user_id = ?";
        try (Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, userPassword);
            preparedStatement.setString(2, userId);

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Error updating student", e);
        }
        return 0;
    }

    @Override
    public int deleteByUserId(String userId) {
        //todo#15-PreparedStatement-회원삭제
        String query = "DELETE FROM jdbc_users WHERE user_id = ?";
        try (Connection connection = DbUtils.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setString(1, userId);

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Error deleting student", e);
        }
        return 0;
    }
}