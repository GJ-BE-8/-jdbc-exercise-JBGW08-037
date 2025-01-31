package com.nhnacademy.jdbc.bank.repository.impl;

import com.nhnacademy.jdbc.bank.domain.Account;
import com.nhnacademy.jdbc.bank.repository.AccountRepository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccountRepositoryImpl implements AccountRepository {

    public Optional<Account> findByAccountNumber(Connection connection, long accountNumber){
        //todo#1 계좌-조회
        String sql = "select * from jdbc_account where account_number ="+accountNumber;
        log.debug("sql:{}",sql);
        ResultSet rs = null;
        try(PreparedStatement statement = connection.prepareStatement(sql)){
            rs = statement.executeQuery();
            if(rs.next()){
                Optional<Account> account = Optional.of(
                    new Account(rs.getLong("account_number"), rs.getString("name"),
                        rs.getLong("balance")));
                log.debug("account:{}", account);
                return account;
            }

        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      return Optional.empty();
    }

    @Override
    public int save(Connection connection, Account account) {
        //todo#2 계좌-등록, executeUpdate() 결과를 반환 합니다.
        String sql = "insert into jdbc_account (account_number,name , balance) values (?, ?, ?)";
        log.debug("sql:{}",sql);

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, account.getAccountNumber());
            statement.setString(2, account.getName());
            statement.setLong(3, account.getBalance());
            int result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public int countByAccountNumber(Connection connection, long accountNumber) {
        int count = 0;
        ResultSet rs = null;
        //todo#3 select count(*)를 이용해서 계좌의 개수를 count해서 반환
        String sql = "select count(*) from jdbc_account where account_number =" + accountNumber;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            rs = statement.executeQuery();
            if (rs.next()) {
                count = rs.getInt(1);
                log.debug("count:{}", count);
            }
            return count;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public int deposit(Connection connection, long accountNumber, long amount){
        //todo#4 입금, executeUpdate() 결과를 반환 합니다.
        int result = 0;

        String sql = "update jdbc_account set balance = balance + ? where account_number = ?";

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, amount);
            statement.setLong(2, accountNumber);
            result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public int withdraw(Connection connection, long accountNumber, long amount){
        //todo#5 출금, executeUpdate() 결과를 반환 합니다.
        int result = 0;

        String sql = "update jdbc_account set balance = balance - ? where account_number = ?";
        log.debug("sql:{}",sql);

        try(PreparedStatement statement = connection.prepareStatement(sql)){
            statement.setLong(1, amount);
            statement.setLong(2, accountNumber);
            result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }

    @Override
    public int deleteByAccountNumber(Connection connection, long accountNumber) {
        //todo#6 계좌 삭제, executeUpdate() 결과를 반환 합니다.

        int result = 0;
        String sql = "delete from jdbc_account where account_number = ?";
        log.debug("sql:{}",sql);

        try(PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, accountNumber);
            result = statement.executeUpdate();
            log.debug("result:{}", result);
            return result;
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

    }
}
