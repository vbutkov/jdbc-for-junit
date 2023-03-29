package dao;

import entity.Task;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;

public class TaskDao {
    private final DataSource dataSource;

    public TaskDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Task save(Task task) {
        // get connection
        // create statement
        // set params
        // execute
        // get id
        // set id
        String sql = "INSERT INTO task (title, finished, created_date) VALUES (?, ?, ?)";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        ) {

            statement.setString(1, task.getTitle());
            statement.setBoolean(2, task.getFinished());
            statement.setTimestamp(3, java.sql.Timestamp.valueOf(task.getCreatedDate()));
            statement.executeUpdate();

            try (ResultSet resultSet = statement.getGeneratedKeys()) {
                if (resultSet.next()) {
                    task.setId(resultSet.getInt(1));
                }
            }

        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }

        return task;
    }

    public List<Task> findAll() {
        List<Task> tasks = new ArrayList<>();
        String sql = "SELECT task_id, title, finished, created_date FROM task ORDER BY task_id";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)
        ) {

            while (resultSet.next()) {
                final Task task = new Task(
                        resultSet.getString(2),
                        resultSet.getBoolean(3),
                        resultSet.getTimestamp(4).toLocalDateTime()
                );
                task.setId(resultSet.getInt(1));
                tasks.add(task);
            }

        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }

        return tasks;
    }

    public int deleteAll() {
        final String DELETE_ALL_FROM_TASK = "DELETE FROM task";
        int rowCountDelete;

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
        ) {
            rowCountDelete = statement.executeUpdate(DELETE_ALL_FROM_TASK);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return rowCountDelete;
    }

    public Task getById(Integer id) {
        final String GET_TASK_BY_ID = "SELECT task_id, title, finished, created_date FROM task WHERE task_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_TASK_BY_ID);
        ) {
            preparedStatement.setObject(1, id);
            var resultSet = preparedStatement.executeQuery();
            Task task = null;
            if (resultSet.next()) {
                task = buildTask(resultSet);
            }
            return task;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    public List<Task> findAllNotFinished() {
        final String GET_ALL_TASK_NOT_FINISHED = "SELECT task_id, title, finished, created_date " +
                "FROM task " +
                "WHERE finished IS FALSE " +
                "ORDER BY task_id";

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(GET_ALL_TASK_NOT_FINISHED);
        ) {
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                Task task = buildTask(resultSet);
                tasks.add(task);
            }
            return tasks;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Task> findNewestTasks(Integer numberOfNewestTasks) {
        final String FIND_NEWEST_TASKS = "SELECT task_id, title, finished, created_date " +
                "FROM task " +
                "ORDER BY task_id DESC " +
                "LIMIT ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(FIND_NEWEST_TASKS);
        ) {
            preparedStatement.setObject(1, numberOfNewestTasks);
            var resultSet = preparedStatement.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                var task = buildTask(resultSet);
                tasks.add(task);
            }
            return tasks;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public Task finishTask(Task task) {
        final String GET_CORRECT_SET_FINISHED_FLAG = "SELECT task_id, title, finished, created_date " +
                "FROM task " +
                "WHERE finished = ? ";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(GET_CORRECT_SET_FINISHED_FLAG);
        ) {
            preparedStatement.setObject(1, task.getFinished());
            var resultSet = preparedStatement.executeQuery();
            Task resultTask = null;
            if (resultSet.next()) {
                resultTask = buildTask(resultSet);
            }
            return resultTask;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void deleteById(Integer id) {
        final String DELETE_TASK_BY_ID = "DELETE FROM task WHERE task_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DELETE_TASK_BY_ID);
        ) {
            preparedStatement.setObject(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Task buildTask(ResultSet resultSet) throws SQLException {
        Task task = new Task(
                resultSet.getObject("title", String.class),
                resultSet.getObject("finished", Boolean.class),
                resultSet.getObject("created_date", Timestamp.class).toLocalDateTime()
        );
        task.setId(resultSet.getObject("task_id", Integer.class));

        return task;
    }
}
