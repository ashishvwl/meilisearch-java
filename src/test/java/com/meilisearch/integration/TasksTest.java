package com.meilisearch.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import com.meilisearch.integration.classes.AbstractIT;
import com.meilisearch.integration.classes.TestData;
import com.meilisearch.sdk.Index;
import com.meilisearch.sdk.exceptions.MeilisearchException;
import com.meilisearch.sdk.model.*;
import com.meilisearch.sdk.utils.Movie;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class TasksTest extends AbstractIT {

    private TestData<Movie> testData;

    @BeforeEach
    public void initialize() {
        this.setUp();
        this.setUpJacksonClient();
        if (testData == null) testData = this.getTestData(MOVIES_INDEX, Movie.class);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @AfterAll
    static void cleanMeilisearch() {
        cleanup();
    }

    /** Test Get Task */
    @Test
    public void testClientGetTask() throws Exception {
        String indexUid = "GetClientTask";
        TaskInfo response = client.createIndex(indexUid);
        client.waitForTask(response.getTaskUid());

        Task task = client.getTask(response.getTaskUid());

        assertThat(task, is(instanceOf(Task.class)));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getStartedAt(), is(notNullValue()));
        assertThat(task.getEnqueuedAt(), is(notNullValue()));
        assertThat(task.getFinishedAt(), is(notNullValue()));
        assertThat(task.getUid(), is(greaterThanOrEqualTo(0)));
        assertThat(task.getDetails(), is(notNullValue()));
        assertThat(task.getDetails().getPrimaryKey(), is(nullValue()));
    }

    /** Test Get Tasks */
    @Test
    public void testClientGetTasks() throws Exception {
        String indexUid = "GetClientTasks";
        TaskInfo response = client.createIndex(indexUid);
        client.waitForTask(response.getTaskUid());

        TasksResults result = client.getTasks();
        Task[] tasks = result.getResults();

        Task task = tasks[0];
        client.waitForTask(task.getUid());

        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getUid(), is(greaterThanOrEqualTo(0)));
        assertThat(task.getDetails(), is(notNullValue()));
    }

    /** Test Get Tasks with limit */
    @Test
    public void testClientGetTasksLimit() throws Exception {
        int limit = 2;
        TasksQuery query = new TasksQuery().setLimit(limit);
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(equalTo(limit)));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(notNullValue()));
    }

    /** Test Get Task Error Values */
    @Test
    public void testClientGetTaskErrorValues() throws Exception {
        String indexUid = "CheckTaskErrorValues";
        Index index = client.index(indexUid);

        // Deleting all documents from an index that does not exist results in a task error.
        TaskInfo taskInfo = index.deleteAllDocuments();
        index.waitForTask(taskInfo.getTaskUid());

        Task task = client.getTask(taskInfo.getTaskUid());

        assertThat(task.getError(), is(notNullValue()));
        assertThat(task.getError().getCode(), not(blankOrNullString()));
        assertThat(task.getError().getType(), not(blankOrNullString()));
        assertThat(task.getError().getLink(), not(blankOrNullString()));
        assertThat(task.getError().getMessage(), not(blankOrNullString()));
    }

    /** Test Get Task Error Values When Adding Documents */
    @Test
    public void testClientGetTaskErrorWhenAddingDocuments() throws Exception {
        String indexUid = "CheckTaskErrorWhenAddingDocuments";
        Index index = client.index(indexUid);

        TaskInfo taskInfo = client.createIndex(indexUid);
        client.waitForTask(taskInfo.getTaskUid());

        String json = "{\"identifyer\": 1, \"name\": \"Donald Duck\"}";
        // Adding a document with a wrong identifier results in a task error.
        TaskInfo taskInfoAddDocuments = index.addDocuments(json, "identifier");
        client.waitForTask(taskInfoAddDocuments.getTaskUid());

        Task task = client.getTask(taskInfoAddDocuments.getTaskUid());

        assertThat(task.getError(), is(notNullValue()));
        assertThat(task.getError().getCode(), not(blankOrNullString()));
        assertThat(task.getError().getType(), not(blankOrNullString()));
        assertThat(task.getError().getLink(), not(blankOrNullString()));
        assertThat(task.getError().getMessage(), not(blankOrNullString()));
    }

    /** Test Get Tasks with limit and from */
    @Test
    public void testClientGetTasksLimitAndFrom() throws Exception {
        // Create several indexes to make sure we have enough tasks
        int numIndexes = 4;
        for (int i = 1; i <= numIndexes; i++) {
            String indexUid = "GetClientTasksLimitFrom" + i;
            TaskInfo response = client.createIndex(indexUid);
        }

        int limit = 2;
        int from = 2;
        TasksQuery query = new TasksQuery().setLimit(limit).setFrom(from);
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(equalTo(limit)));
        assertThat(result.getFrom(), is(equalTo(from)));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(equalTo(limit)));
    }

    /** Test Get Tasks with uid as filter */
    @Test
    public void testClientGetTasksWithUidFilter() throws Exception {
        TasksQuery query = new TasksQuery().setUids(new int[] {1});
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(notNullValue()));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(notNullValue()));
    }

    /** Test Get Tasks with beforeEnqueuedAt as filter */
    @Test
    public void testClientGetTasksWithDateFilter() throws Exception {
        Date date = new Date();
        TasksQuery query = new TasksQuery().setBeforeEnqueuedAt(date);
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(notNullValue()));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(notNullValue()));
    }

    /** Test Get Tasks with canceledBy as filter */
    @Test
    public void testClientGetTasksWithCanceledByFilter() throws Exception {
        TasksQuery query = new TasksQuery().setCanceledBy(new int[] {1});
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(notNullValue()));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(notNullValue()));
    }

    /** Test Get Tasks with all query parameters */
    @Test
    public void testClientGetTasksAllQueryParameters() throws Exception {
        int limit = 2;
        int from = 2;
        TasksQuery query =
                new TasksQuery()
                        .setLimit(limit)
                        .setFrom(from)
                        .setStatuses(new String[] {"enqueued", "succeeded"})
                        .setTypes(new String[] {"indexDeletion"});
        TasksResults result = client.getTasks(query);

        assertThat(result.getLimit(), is(equalTo(limit)));
        assertThat(result.getFrom(), is(notNullValue()));
        assertThat(result.getNext(), is(notNullValue()));
        assertThat(result.getResults().length, is(notNullValue()));
    }

    @Test
    public void testGetTasksInReverse() {
        String indexUid = "tasksOnReverseOrder";
        Date currentTime = Date.from(Instant.now());
        TestData<Movie> testData = this.getTestData(MOVIES_INDEX, Movie.class);
        TasksQuery query = new TasksQuery().setAfterEnqueuedAt(currentTime);
        TasksQuery queryWithReverseFlag =
                new TasksQuery().setAfterEnqueuedAt(currentTime).setReverse(true);

        client.index(indexUid).addDocuments(testData.getRaw());
        client.waitForTask(client.index(indexUid).addDocuments(testData.getRaw()).getTaskUid());
        List<Integer> tasks =
                Arrays.stream(client.getTasks(query).getResults())
                        .map(Task::getUid)
                        .collect(Collectors.toList());
        List<Integer> reversedTasks =
                Arrays.stream(client.getTasks(queryWithReverseFlag).getResults())
                        .map(Task::getUid)
                        .collect(Collectors.toList());

        assertFalse(tasks.isEmpty());
        assertIterableEquals(
                tasks,
                reversedTasks.stream()
                        .sorted(Collections.reverseOrder())
                        .collect(Collectors.toList()),
                "The lists are not reversed versions of each other");
    }

    /** Test Cancel Task */
    @Test
    public void testClientCancelTask() throws Exception {
        CancelTasksQuery query =
                new CancelTasksQuery().setStatuses(new String[] {"enqueued", "succeeded"});

        TaskInfo task = client.cancelTasks(query);

        assertThat(task, is(instanceOf(TaskInfo.class)));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getIndexUid(), is(nullValue()));
        assertThat(task.getType(), is(equalTo("taskCancelation")));
    }

    /** Test Cancel Task with multiple filters */
    @Test
    public void testClientCancelTaskWithMultipleFilters() throws Exception {
        Date date = new Date();
        CancelTasksQuery query =
                new CancelTasksQuery()
                        .setUids(new int[] {0, 1, 2})
                        .setStatuses(new String[] {"enqueued", "succeeded"})
                        .setTypes(new String[] {"indexDeletion"})
                        .setIndexUids(new String[] {"index"})
                        .setBeforeEnqueuedAt(date);

        TaskInfo task = client.cancelTasks(query);

        assertThat(task, is(instanceOf(TaskInfo.class)));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getIndexUid(), is(nullValue()));
        assertThat(task.getType(), is(equalTo("taskCancelation")));
    }

    /** Test Delete Task */
    @Test
    public void testClientDeleteTask() throws Exception {
        DeleteTasksQuery query =
                new DeleteTasksQuery().setStatuses(new String[] {"enqueued", "succeeded"});

        TaskInfo task = client.deleteTasks(query);

        assertThat(task, is(instanceOf(TaskInfo.class)));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getIndexUid(), is(nullValue()));
        assertThat(task.getType(), is(equalTo("taskDeletion")));
    }

    /** Test Delete Task with multiple filters */
    @Test
    public void testClientDeleteTaskWithMultipleFilters() throws Exception {
        Date date = new Date();
        DeleteTasksQuery query =
                new DeleteTasksQuery()
                        .setUids(new int[] {0, 1, 2})
                        .setStatuses(new String[] {"enqueued", "succeeded"})
                        .setTypes(new String[] {"indexDeletion"})
                        .setIndexUids(new String[] {"index"})
                        .setBeforeEnqueuedAt(date);

        TaskInfo task = client.deleteTasks(query);

        assertThat(task, is(instanceOf(TaskInfo.class)));
        assertThat(task.getStatus(), is(notNullValue()));
        assertThat(task.getIndexUid(), is(nullValue()));
        assertThat(task.getType(), is(equalTo("taskDeletion")));
    }

    /** Test waitForTask */
    @Test
    public void testWaitForTask() throws Exception {
        String indexUid = "WaitForTask";
        TaskInfo response = client.createIndex(indexUid);
        client.waitForTask(response.getTaskUid());

        Task task = client.getTask(response.getTaskUid());

        assertThat(task.getUid(), is(greaterThanOrEqualTo(0)));
        assertThat(task.getEnqueuedAt(), is(notNullValue()));
        assertThat(task.getStartedAt(), is(notNullValue()));
        assertThat(task.getFinishedAt(), is(notNullValue()));
        assertThat(task.getStatus(), is(equalTo(TaskStatus.SUCCEEDED)));
        assertThat(task.getDetails(), is(notNullValue()));
        assertThat(task.getDetails().getPrimaryKey(), is(nullValue()));

        client.deleteIndex(task.getIndexUid());
    }

    /** Test waitForTask timeoutInMs */
    @Test
    public void testWaitForTaskTimoutInMs() throws Exception {
        String indexUid = "WaitForTaskTimoutInMs";
        Index index = client.index(indexUid);

        TaskInfo task = index.addDocuments(this.testData.getRaw());
        index.waitForTask(task.getTaskUid());

        assertThrows(Exception.class, () -> index.waitForTask(task.getTaskUid(), 0, 50));
    }

    /** Test Tasks with Jackson Json Handler */
    @Test
    public void testTasksWithJacksonJsonHandler() throws Exception {
        String indexUid = "tasksWithJacksonJsonHandler";
        Index index = clientJackson.index(indexUid);

        TestData<Movie> testData = this.getTestData(MOVIES_INDEX, Movie.class);
        TaskInfo task = index.addDocuments(testData.getRaw());

        assertThat(task.getStatus(), is(equalTo(TaskStatus.ENQUEUED)));
    }

    /** Test Get Task Documents - Successful case with documents */
    @Test
    public void testGetTaskDocumentsSuccess() throws Exception {
        String indexUid = "GetTaskDocumentsSuccess";
        Index index = createEmptyIndex(indexUid, "id");

        // Add documents to the index
        String documentsJson = "[{\"id\": 1, \"title\": \"Test Document 1\"}," +
                               "{\"id\": 2, \"title\": \"Test Document 2\"}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        // Get task documents
        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
        assertThat(taskDocuments, not(blankOrNullString()));
        assertThat(taskDocuments, containsString("id"));
    }

    /** Test Get Task Documents - Returns NDJSON format */
    @Test
    public void testGetTaskDocumentsFormatNDJSON() throws Exception {
        String indexUid = "GetTaskDocumentsFormatNDJSON";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 1, \"name\": \"Alice\"}," +
                               "{\"id\": 2, \"name\": \"Bob\"}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        // NDJSON format has each object on a separate line
        String[] lines = taskDocuments.split("\n");
        assertThat(lines.length, is(greaterThanOrEqualTo(1)));
        
        // Each line should be a valid JSON object
        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                assertThat(line, containsString("{"));
                assertThat(line, containsString("}"));
            }
        }
    }

    /** Test Get Task Documents - Empty result when no documents processed */
    @Test
    public void testGetTaskDocumentsEmpty() throws Exception {
        String indexUid = "GetTaskDocumentsEmpty";
        TaskInfo taskInfo = client.createIndex(indexUid);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        // Create index task should not have documents
        assertThat(taskDocuments, is(notNullValue()));
    }

    /** Test Get Task Documents - Multiple documents */
    @Test
    public void testGetTaskDocumentsMultipleDocuments() throws Exception {
        String indexUid = "GetTaskDocumentsMultipleDocuments";
        Index index = createEmptyIndex(indexUid, "id");

        String[] documentsList = new String[3];
        for (int i = 1; i <= 3; i++) {
            documentsList[i - 1] = "{\"id\": " + i + ", \"title\": \"Document " + i + "\"}";
        }
        String documentsJson = "[" + String.join(",", documentsList) + "]";
        
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
        assertThat(taskDocuments, not(blankOrNullString()));
    }

    /** Test Get Task Documents - With different document types */
    @Test
    public void testGetTaskDocumentsWithDifferentDataTypes() throws Exception {
        String indexUid = "GetTaskDocumentsWithDifferentDataTypes";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 1, \"title\": \"Test\", \"active\": true, \"score\": 9.5}," +
                               "{\"id\": 2, \"title\": \"Test2\", \"active\": false, \"score\": 8.2}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
        assertThat(taskDocuments, not(blankOrNullString()));
    }

    /** Test Get Task Documents - After successful task completion */
    @Test
    public void testGetTaskDocumentsAfterTaskCompletion() throws Exception {
        String indexUid = "GetTaskDocumentsAfterTaskCompletion";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 100, \"data\": \"Sample Data\"}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        
        // Wait for task to complete
        client.waitForTask(taskInfo.getTaskUid());
        Task completedTask = client.getTask(taskInfo.getTaskUid());
        
        assertThat(completedTask.getStatus(), is(equalTo(TaskStatus.SUCCEEDED)));

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
    }

    /** Test Get Task Documents - Returns instance of String */
    @Test
    public void testGetTaskDocumentsReturnsString() throws Exception {
        String indexUid = "GetTaskDocumentsReturnsString";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 1, \"content\": \"Test Content\"}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(instanceOf(String.class)));
    }

    /** Test Get Task Documents - With complex nested documents */
    @Test
    public void testGetTaskDocumentsWithNestedDocuments() throws Exception {
        String indexUid = "GetTaskDocumentsWithNestedDocuments";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 1, \"author\": {\"name\": \"John\", \"email\": \"john@example.com\"}, " +
                               "\"tags\": [\"tech\", \"java\"]}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
        assertThat(taskDocuments, not(blankOrNullString()));
    }

    /** Test Get Task Documents - Throws exception for invalid task uid */
    @Test
    public void testGetTaskDocumentsInvalidTaskUid() throws Exception {
        // Try to get documents for a non-existent task uid
        assertThrows(MeilisearchException.class, () -> client.getTaskDocuments(999999));
    }

    /** Test Get Task Documents - With bulk document addition */
    @Test
    public void testGetTaskDocumentsWithBulkDocuments() throws Exception {
        String indexUid = "GetTaskDocumentsWithBulkDocuments";
        Index index = createEmptyIndex(indexUid, "id");

        // Create a bulk of documents
        StringBuilder documentsBuilder = new StringBuilder("[");
        for (int i = 1; i <= 10; i++) {
            if (i > 1) documentsBuilder.append(",");
            documentsBuilder.append("{\"id\": ").append(i).append(", \"value\": \"Item ").append(i).append("\"}");
        }
        documentsBuilder.append("]");

        TaskInfo taskInfo = index.addDocuments(documentsBuilder.toString());
        client.waitForTask(taskInfo.getTaskUid());

        String taskDocuments = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments, is(notNullValue()));
        assertThat(taskDocuments, not(blankOrNullString()));
    }

    /** Test Get Task Documents - Consistency check */
    @Test
    public void testGetTaskDocumentsConsistency() throws Exception {
        String indexUid = "GetTaskDocumentsConsistency";
        Index index = createEmptyIndex(indexUid, "id");

        String documentsJson = "[{\"id\": 1, \"title\": \"Consistent Test\"}]";
        TaskInfo taskInfo = index.addDocuments(documentsJson);
        client.waitForTask(taskInfo.getTaskUid());

        // Get documents multiple times to ensure consistency
        String taskDocuments1 = client.getTaskDocuments(taskInfo.getTaskUid());
        String taskDocuments2 = client.getTaskDocuments(taskInfo.getTaskUid());

        assertThat(taskDocuments1, is(equalTo(taskDocuments2)));
    }
}
